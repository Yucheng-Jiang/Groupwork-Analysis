import requests, json, uuid
from tqdm import tqdm
import threading
from datetime import datetime
import os
import pandas as pd
import argparse
from boxsdk import JWTAuth, Client, exception
from shutil import which

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

parser = argparse.ArgumentParser()
parser.add_argument("--download_batch", help="download chunk size", type=int)
parser.add_argument("--thread_num", help="number of thread to download", type=int)
parser.add_argument("--api_token", help="api access token")
parser.add_argument("--course_instance", help="course instance id", type=int)
parser.add_argument("--store_path", help="where you want to store data")
parser.add_argument("--zip_path", help="where you want to store zip data")
parser.add_argument("--box_config_path", help="box configuration json file path")
parser.add_argument("--box_folder_id", help="box configuration json file path")
parser.add_argument("--start_date", help="start date of log file")
parser.add_argument("--end_date", help="end date of log file")

download_batch = 0; total_log_count = 0;  thread_num = 0
store_path = ""; zip_path = ""; 
COURSE_INSTANCE = -1; API_TOKEN = ""
start_date_str = ""; end_date_str = ""
box_config_path = ""; box_folder_id = ""

"""
upload file to box.
@param store_path, the path of zip file
@param file_name, the name of zip
"""
def upload(store_path, file_name, keep_file = True):
    try:
        config = JWTAuth.from_settings_file(box_config_path)
        client = Client(config)
        new_file = client.folder(box_folder_id).upload(f"{store_path}/{file_name}")
    except Exception as e:
        return e, -1
    if not keep_file:
        os.system(f"cd {store_path} && rm {file_name} && cd ..")
    return "SUCCESS", new_file.id

"""
delete file from box.
@param file id
"""
def delete(file_id):
    try:
        config = JWTAuth.from_settings_file(box_config_path)
        client = Client(config)
        client.file(file_id=file_id).delete()
    except Exception as e:
        return e

"""
Get json data object from given url. If json file exists return loaded json, otherwise download first
@param url, api file url
@param path, local stored path
@return json data object
"""
def getJson(start_index, end_index, need_data = False):
    s = requests.Session()
    for i in range(start_index, end_index):
        url = f"https://www.prairielearn.org/pl/api/v1/course_instances/{COURSE_INSTANCE}/assessment_instances/{i}/log?private_token={API_TOKEN}"
        path = f"{store_path}/assessment_instance_{i}_log.json"
        if not os.path.isfile(path):
            try:
                with open(path, "w") as f:
                    f.write(s.get(url).text)
            except Exception as e:
                print(e)
        if need_data:
            f = open(path)
            return json.load(f)
        

"""
binary search for approximate range where assessment is created on the same date as target_time

@param end_index, ending assessment id that hope to search for
@param target_time, target time expect to find

@return approximate assessment instance id
"""
def binary_search(end_index : int, target_time : datetime, is_lower_bound = True) -> int:
    low = 1
    mid = 0
    while low <= end_index:
        mid = (end_index + low) // 2
        cur = None
        while True:
            cur = getTime(mid)
            if cur != None:
                break
            mid += 1
        if cur < target_time:
            low = mid + 1
        elif cur > target_time:
            end_index = mid - 1
        else:
            if is_lower_bound:
                left_time = getTime(mid - 1)
                if left_time == target_time:
                    end_index = mid - 1
                else:
                    return mid
            else:
                right_time = getTime(mid + 1)
                if right_time == target_time:
                    low = mid + 1
                else:
                    return mid
    return -1


"""
Get create time of assessment instance

@param assessment_instance_id, assessment instance id in string format
@return datatime object representing creation time
"""
def getTime(assessment_instance_id: str) -> datetime:
    data = getJson(assessment_instance_id, assessment_instance_id + 1,True)
    if len(data) == 0:
        return None
    return datetime.strptime(data[0]['event_date'][0:10], "%Y-%m-%d")


"""
Get number of assessment instance created from courses taught by current instructor
@return integer representing total number of assessment instances created
"""
def getInstanceRange() -> int:
    low = 1
    high = 1000
    # get enough ending index
    while True:
        data = getJson(high, high + 1, True)
        if len(data) == 0:
            break
        else:
            high *= 2
        
    # binary search to pinpoint range
    mid = 0
    while low <= high:
        mid = (high + low) // 2
        data = getJson(mid, mid + 1, True)
        if len(data) == 0:
            high = mid - 1
        else:
            data = getJson(mid + 1, mid + 2, True)
            if len(data) == 0:
                return mid
            else:
                low = mid + 1


def main():
    start_index = -1; end_index = -1
    try:
        print("\nCalculate search range... ", end = "", flush = True)
        # init temp directory to store log file and zip file
        os.system(f"mkdir {store_path}; mkdir {zip_path}")
        # get ending range
        total_log_count = getInstanceRange()
        # get start and end index of download range
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        start_index = binary_search(total_log_count, start_date, True)
        end_index = binary_search(total_log_count, end_date, False)
        # remove binary search temp files
        os.system(f"cd {store_path} && rm -rf assessment_instance_*.json && cd ..")
        print(f"from {start_index} to {end_index} \u2713", flush = True)
    except (KeyboardInterrupt, Exception) as e:
        if type(e) == KeyboardInterrupt:
            print(f"\n{bcolors.WARNING}KeyboardInterrupt{bcolors.ENDC}")
        else:
            print(f"\n{bcolors.FAIL} ERROR: {e} {bcolors.ENDC}")
        exit()
    

    tid = []
    box_upload_tid = -1
    prev = start_index
    try:
        print(f"start downloading {end_index - start_index + 1} log files. Each iteration {thread_num * download_batch} log files.\n")
        # for each batch of downloading
        for i in tqdm(range(start_index, end_index + 1, thread_num * download_batch)):
            # reset tid lists
            tid.clear()
            # create n thread, each thread download download_batch log files
            for _ in range(thread_num):
                if i > end_index:
                    break
                # create thread, update i, save thread to tid, and start thread
                thread = threading.Thread(target = getJson, args = [i, min(end_index, i + download_batch - 1)])
                i += download_batch
                tid.append(thread)
                thread.start()
            # join all thread
            for t in tid:
                t.join()
            # zip all downloaded files in current batch, remove raw files
            os.system(f"zip -qq -r {zip_path}/{prev}_{i - 1}.zip {store_path}")
            os.system(f"cd {store_path} && rm -rf assessment_instance_*.json && cd ..")
            # use another thread to upload zip to box, and remove zip
            box_upload_tid = threading.Thread(target = upload, args = [zip_path, f"{prev}_{i - 1}.zip", False])
            box_upload_tid.start()
            prev = i
        # wait for last upload to finish
        box_upload_tid.join()
    except (KeyboardInterrupt, Exception) as e:
        print("waiting for all started thread to stop...")
        for t in tid:
            t.join()
        if type(box_upload_tid) != int:
            box_upload_tid.join()
        os.system(f"cd {store_path} && rm -rf assessment_instance_*.json && cd ..")
        if type(e) == KeyboardInterrupt:
            print(f"{bcolors.WARNING}KeyboardInterrupt{bcolors.ENDC}")
        else:
            print(f"{bcolors.FAIL} ERROR: {e} {bcolors.ENDC}")
        
        print(f"{bcolors.WARNING} Attempt to download log file {start_index} - {end_index}, last saved log id: {prev} {bcolors.ENDC}")
    # delete temp directories
    os.system(f"rm -r {store_path}; rm -r {zip_path}")


def sanity_check():
    thread_flag = True; api_flag = True; date_flag = True; box_flag = True; zip_flag = True
    print("Sanity Check Initiated.")
    
    if which("zip") == None:
        print(f"{bcolors.FAIL} ZIP package not exist on current system. {bcolors.ENDC}")
        zip_flag = False
    # check download_batch
    if download_batch < 0:
        print(f"{bcolors.FAIL} download_batch MUST be positive. {bcolors.ENDC}")
        thread_flag = False
    elif download_batch >= 1000:
        print(f"{bcolors.WARNING} download_batch greater than 500 will significantly affect efficiency. Type Y to confirm override, type anything else to continue checking. {bcolors.ENDC}")
        confirm = input()
        if confirm.lower() != 'y':
            thread_flag = False
    # check thread_num
    if thread_num < 0:
        print(f"{bcolors.FAIL} thread_num MUST be positive. {bcolors.ENDC}")
        thread_flag = False
    elif thread_num > 100:
        print(f"{bcolors.WARNING} thread_num greater than 100 will significantly affect efficiency. Type Y to confirm override, type anything else to continue checking. {bcolors.ENDC}")
        confirm = input()
        if confirm.lower() != 'y':
            thread_flag = False

    # check api
    test_url = f"https://www.prairielearn.org/pl/api/v1/course_instances/{COURSE_INSTANCE}/assessment_instances/{1}/log?private_token={API_TOKEN}"
    response = requests.get(test_url)
    test_text = response.text
    if response.status_code != 200:
        api_flag = False
        if "The provided authentication token was invalid" in test_text:
            print(f"{bcolors.FAIL} INVALID api_token. {bcolors.ENDC}")
        elif "Forbidden" in test_text:
            print(f"{bcolors.FAIL} INVALID course_instance. {bcolors.ENDC}")
        else:
            print(f"{bcolors.FAIL} PrairieLearn API config unknown error. Check your config again. {bcolors.ENDC}")


    # check dates
    test_start_date = None; test_end_date = None
    ## check start_date_str format
    try:
        test_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    except:
        print(f"{bcolors.FAIL} INVALID start_date format. Need to be YYYY-MM-DD. {bcolors.ENDC}")
        date_flag = False
    ## check end_date_str format
    try:
        test_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except:
        print(f"{bcolors.FAIL} INVALID end_date format. Need to be YYYY-MM-DD. {bcolors.ENDC}")
        date_flag = False
    ## check range
    if date_flag and test_end_date < test_start_date:
        print(f"{bcolors.FAIL} end_date is expected to be no earlier than start_date. {bcolors.ENDC}")
        date_flag = False

    # check box config
    if not os.path.exists(box_config_path):
        print(f"{bcolors.FAIL} PATH INVALID. box_config_path: {box_config_path} DOES NOT HAVE FILE. {bcolors.ENDC}")
        box_flag = False
    else:
        temp_testfile_name = f"{str(uuid.uuid4())}.temp"
        os.system(f"echo test_upload > {temp_testfile_name}")
        return_message, file_id = upload(store_path = ".", file_name = temp_testfile_name, keep_file = False)
        if return_message != "SUCCESS":
            box_flag = False
            if type(return_message) == json.decoder.JSONDecodeError:
                print(f"{bcolors.FAIL} JSON DECODE ERROR: {return_message}. {bcolors.ENDC}")
            elif type(return_message) == exception.BoxAPIException:
                print(f"{bcolors.FAIL} boxsdk ERROR status {return_message.status}: {return_message.message}. Please check your box_folder_id. {bcolors.ENDC}")
            elif type(return_message) == exception.BoxOAuthException:
                print(f"{bcolors.FAIL} boxsdk ERROR status {return_message.status}: {return_message.message}. Please check your box config file. {bcolors.ENDC}")
            else:
                print(f"{bcolors.FAIL} {return_message} {bcolors.ENDC}") 
        else:
            delete(file_id)


    # whole check
    print("\t zip package: \t", end = "")
    if (zip_flag):
        print(f"{bcolors.OKGREEN}\t\t\u2713 {bcolors.ENDC}")
    else:
        print(f"{bcolors.FAIL}\t\t\u2717 {bcolors.ENDC}")
    print("\t multi-thread download config: \t", end = "")
    if (thread_flag):
        print(f"{bcolors.OKGREEN}\u2713 {bcolors.ENDC}")
    else:
        print(f"{bcolors.FAIL}\u2717 {bcolors.ENDC}")
    print("\t PrairieLearn API config: \t", end = "")
    if (api_flag):
        print(f"{bcolors.OKGREEN}\u2713 {bcolors.ENDC}")
    else:
        print(f"{bcolors.FAIL}\u2717 {bcolors.ENDC}")
    print("\t start & end date: \t", end = "")
    if (date_flag):
        print(f"{bcolors.OKGREEN} \t\u2713 {bcolors.ENDC}")
    else:
        print(f"{bcolors.FAIL} \t\u2717 {bcolors.ENDC}")
    print("\t Box API config: \t", end = "")
    if (box_flag):
        print(f"{bcolors.OKGREEN} \t\u2713 {bcolors.ENDC}") 
    else:
        print(f"{bcolors.FAIL} \t\u2717 {bcolors.ENDC}")
    
    return thread_flag and api_flag and date_flag and box_flag and zip_flag
            

if __name__ == "__main__":
    args = parser.parse_args()
    # parse thread config
    download_batch = args.download_batch
    thread_num = args.thread_num
    # parse api info config
    COURSE_INSTANCE = args.course_instance
    API_TOKEN = args.api_token
    # parse download range
    start_date_str = args.start_date
    end_date_str = args.end_date
    # parse box config
    box_config_path = args.box_config_path
    box_folder_id = args.box_folder_id
    # sanity check for input and system packages
    if not sanity_check():
        exit()
    # generate random temp path to store file
    store_path = str(uuid.uuid4())
    zip_path = str(uuid.uuid4())
    # start main program
    main()
