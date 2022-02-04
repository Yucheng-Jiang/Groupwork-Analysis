from boxsdk import JWTAuth, Client, exception
import os
import json, uuid
from tqdm import tqdm
import threading
import pandas as pd
import argparse


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
parser.add_argument("--box_config_path", help="box configuration json file path", type=str)
parser.add_argument("--box_folder_id", help="box configuration json file path", type = int)
parser.add_argument("--data_dir", help="path to store downloaded zip file", type = str, default = "data")
parser.add_argument("--all_submission_dir", help="path containing all_submission_csvs", type = str)
parser.add_argument("--result_dir", help="path to store matching result json", type = str, default = "result.json")
parser.add_argument("--save_instance_match_dir", help="path to store instance-submission matching result json", type = str, default = "instance_submission_match.json")
parser.add_argument("--load_instance_match_dir", help="path to load instance-submission matching result json", type = str, default = "instance_submission_match.json")
parser.add_argument("--starting_instance_id", help="starting instance id", type = int, default = 1)


box_config_path = ""; config = None; client = None
data_dir = ""; all_submission_dir = ""; result_dir = ""
save_instance_match_dir = ""; load_instance_match_dir = ""
starting_instance_id = 1; box_folder_id = ""

"""
Get items in box folder.
@param folder_id, box folder id
@return dictionary key = item.id.astype(int), value = item.name.astype(str)
"""
def getFolderItems(folder_id):
    try:
        items =  client.folder(folder_id=folder_id).get_items(limit=5000)
        dic = {}
        for item in items:
            dic[int(item.id)] = item.name
        return dic
    except Exception as e:
        print(e)
        return None

"""
Download all zip file
@param box_files, dictionary key = item.id.astype(int), value = item.name.astype(str)
@param save_dir, directory to save zip file
"""
def downloadZip(box_files:list, save_dir:str):
    # if save directory not exist, create one
    if not os.path.exists(save_dir):
        os.system(f"mkdir {save_dir}")
    
    # get all file ids
    keys = [*box_files]
    for i in tqdm(range(len(keys))):
        # extract file_id and file_name
        file_id = int(keys[i])
        file_name = box_files[file_id]
        # if file not exists, download from box
        if not os.path.exists(f"{save_dir}/{file_name}"):
            output_file = open(f"{save_dir}/{file_name}", 'wb')
            client.file(file_id).download_to(output_file)


"""
extract single submission_id from each group in all files in directory
@param all_submission_dir, path to directory of all_submissions csv files
@return dictionary, key = submission_id.astype(str) value = (f"csv_file_name@Usernames", -1)
"""
def extractSubmissions(all_submission_dir:str)->dict:
    submission_dic = {}
    # for all files in directory
    for root, dirs, files in os.walk(all_submission_dir, topdown=False):
        for i in tqdm(range(len(files))):
            name = files[i]
            # read in dataframe
            df = pd.read_csv(os.path.join(root, name))
            # extract groups
            groups = df['Usernames'].unique()
            # for each group, extract groupdf, add one submission to dict
            for group in groups:
                group_df = df[df['Usernames'] == group]
                submission_dic[str(group_df['submission_id'].values[0])] = (f'{name}@{group}', -1)
    return submission_dic

"""
Extract all submissions from all assessment_instance_log files
@param data_dir, path to where zip file stored
@param filtered_items, filter items from filterBoxItem function
@return result_list, each element is a json with key = assessment_instance_id, value = submissions list
"""
def extractInstanceSubmissions(data_dir:str, filtered_items:dict)->list:
    result_list = []; existed_id = set()
    # if there's saved file, load in first
    try:
        if load_instance_match_dir != "" and os.path.exists(load_instance_match_dir):
            result_list = json.load(open(load_instance_match_dir))
            for d in result_list:
                existed_id.add(str(d['assessment_instance_id']))
    except Exception as e:
        print(f"Load {load_instance_match_dir} failed. {e}")
    
    unzip_dir = "" # temp unzip dorectory
    filtered_names = set(filtered_items.values())
    try:
        # load data
        for root, dirs, files in os.walk(data_dir, topdown=False):
            for i in tqdm(range(len(files))):
                file_name = files[i]
                if file_name not in filtered_names:
                    continue
                # create temporary directory
                unzip_dir = str(uuid.uuid4())
                os.system(f"mkdir {data_dir}/{unzip_dir}")
                # unzip file to temp dir
                os.system(f"unzip -qq {data_dir}/{file_name} -d {data_dir}/{unzip_dir}")
                # iterate through every json file
                for unzip_root, unzip_dirs, unzip_files in os.walk(f'{data_dir}/{unzip_dir}'):
                    for filename in unzip_files:
                        if ".json" in filename:
                            # load json, extract instance_id
                            instance_id = str(filename.replace("assessment_instance_","").split("_")[0])
                            # if already written, skip
                            if instance_id in existed_id:
                                continue
                            data = json.load(open(f'{data_dir}/{unzip_dir}/{filename}'))
                            # extract all submission_id in current json file
                            submissions = []
                            for d in data:
                                if d['submission_id'] == None:
                                    continue
                                submissions.append(d['submission_id'])
                            # create json key = assessment_instance_id, value = submissions list
                            cur_json = {'assessment_instance_id': instance_id, 'submissions' : submissions}
                            # append to result_json
                            result_list.append(cur_json)
                # delete temp dir
                os.system(f"rm -rf {data_dir}/{unzip_dir}")
        # save result
        with open(save_instance_match_dir, "w") as f:
            json.dump(result_list, f)
        return result_list
    except (KeyboardInterrupt, Exception) as e:
        if os.path.exists(f"{data_dir}/{unzip_dir}"):
            os.system(f"rm -rf {data_dir}/{unzip_dir}")
        print(e)
        return None

"""
Find assessment_instance that contains submission_id (key of submission_dic), and update submission_dic
@param submission_dic, key = submission_id.astype(str) value = (f"csv_file_name@Usernames", -1)
@param instance_submission_list, list containing json, key = assessment_instance_id, value = submissions list
@return updated submission_dic
"""
def matchInstanceId(submission_dic:dict, instance_submission_list:list)->dict:
    # for each json, key = assessment_instance_id, value = submissions_id
    for i in tqdm(range(len(instance_submission_list))):
        cur_data = instance_submission_list[i]
        instance_id = cur_data['assessment_instance_id']
        submissions = cur_data['submissions']
        # for all submissions
        for submission in submissions:
            submission = str(submission)
            # if find a match in submission_dic, update assessment_instance_id to submission_dic
            if submission in submission_dic:
                submission_dic[submission] = (submission_dic[submission][0], instance_id)
                break
    # return updated submission_dic
    return submission_dic

"""
filter zip files with starting instance_id < threshold
@param threshold, starting threshold
@param box_items, dictionary key = item.id.astype(int), value = item.name.astype(str)
@return updated_box_items, dictionary key = item.id.astype(int), value = item.name.astype(str)
"""
def filterBoxItem(threshold:int, box_items:dict)->dict:
    to_remove = []
    # find all zip that has starting instance < threshold
    for key in box_items:
        cur_start_index = int(box_items[key].split("_")[0])
        if cur_start_index < threshold:
            to_remove.append(key)
    # delete 
    for key in to_remove:
        del box_items[key]
    return box_items


def main():
    print("Step 1 / 7: Retrieving box_items...",end = "",flush=True)
    # get all items in box folder
    box_items = getFolderItems(box_folder_id)
    print(f"{bcolors.OKGREEN}\u2713 {bcolors.ENDC}")
    # filter items
    print("Step 2 / 7: Filtering required zips...", end = "",flush=True)
    filtered_items = filterBoxItem(threshold = starting_instance_id,box_items =  box_items)
    print(f"{bcolors.OKGREEN}\u2713 {bcolors.ENDC}")
    # if local does not have zip, load them
    print("Step 3 / 7: Downloading missing zip files...",flush=True)
    downloadZip(box_files = filtered_items, save_dir = data_dir)
    print("Step 4 / 7: Extracting submissions from csvs...",flush=True)
    # extract submissions from all_submission_csv
    submission_dic = extractSubmissions(all_submission_dir = all_submission_dir)
    # extract instance-submissions pair match
    print("Step 5 / 7: Creating instance - submission dictionary...",flush=True)
    match_list = extractInstanceSubmissions(data_dir = data_dir, filtered_items = filtered_items)
    # match assessment_instance_id to each submissions
    print("Step 6 / 7: Matching submission - instance...",flush=True)
    updated_submission_dic =  matchInstanceId(submission_dic = submission_dic,
                                              instance_submission_list = match_list)
    print("Step 7 / 7: Saving results...")
    # save results
    with open(result_dir, "w") as f:
        result = []
        for submission in updated_submission_dic:
            cur_data = updated_submission_dic[submission]
            pos = cur_data[0].find("@")
            cur_dic = {"file_name": cur_data[0][:pos], 
                       "Username": cur_data[0][pos + 1:],
                       "assessment_instance_id": cur_data[1]}
            result.append(cur_dic)
        json.dump(result, f)

    print(f"{bcolors.OKGREEN}successful \u2713 {bcolors.ENDC}")

if __name__ == "__main__":
    #global config, client
    
    args = parser.parse_args()
    # extract args
    box_config_path = args.box_config_path
    box_folder_id = args.box_folder_id
    data_dir = args.data_dir
    all_submission_dir = args.all_submission_dir
    result_dir = args.result_dir
    save_instance_match_dir = args.save_instance_match_dir
    load_instance_match_dir = args.load_instance_match_dir
    starting_instance_id = args.starting_instance_id
    # sanity check
    if os.path.exists(result_dir):
        print("\n\033[93mWARNING. Output directory already has file. Override [y] / N ?\033[0m")
        confirm = input()
        if confirm.lower() != 'y':
            print("program exit....")
            exit()
    # configure box
    config = JWTAuth.from_settings_file(box_config_path)
    client = Client(config)
    # start main
    main()


