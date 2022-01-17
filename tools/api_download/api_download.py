import wget
import json
import threading
import os
import sys
import argparse

course_instance = ""
access_token = ""
root_path = ""
folder = ""

parser = argparse.ArgumentParser()
parser.add_argument("--course_instance", help="course instance number")
parser.add_argument("--access_token", help="api access token")
parser.add_argument("--root_path", help="root path where you hope to store the data")
parser.add_argument("--folder", help="newly created folder name for downloaded data")

def downloadFile(url, path):
    if os.path.isfile(path):
        return
    try:
        base_url = f"https://prairielearn.engr.illinois.edu/pl/api/v1/course_instances/{course_instance}"
        permission = "?private_token=" + access_token
        wget.download(base_url + url + permission, path)
    except:
        return

def get_assessment_list():
    assessment_url = "/assessments"
    downloadFile(assessment_url, f"{root_path}/{folder}/assessments.json")

def get_gradebook():
    gradebook_url = "/pl/api/v1/course_instances/"  +course_instance + "/gradebook"
    downloadFile(gradebook_url, f"{root_path}/{folder}/gradebook.json")

def get_assessment_instance():
    #read assessments list from json file
    assessment_file = open(f'{root_path}/{folder}/assessments.json')
    assessment_list = json.loads(assessment_file.read())
    tid = []
    #setup progress bar 
    for i in range(len(assessment_list)):
        asessment_instance = assessment_list[i]   
        id = str(asessment_instance["assessment_id"])
        asessment_instance_url = "/assessments/" + id + "/assessment_instances"
        thread = threading.Thread(target=downloadFile, args=[asessment_instance_url, f"{root_path}/{folder}/Assessment_instances/asessment_" + id + "_instances.json"])
        tid.append(thread)
        thread.start()
    for i in tid:
        i.join()

def get_assessment_instance_list():
    #read assessments list from json file
    assessment_file = open(f"{root_path}/{folder}/assessments.json")
    assessment_list = json.loads(assessment_file.read())

    assessment_instance_list = []
    #setup progress bar 
    for i in range(len(assessment_list)):
        
        # get current assessment id
        asessment_instance = assessment_list[i] 
        id = str(asessment_instance["assessment_id"])
        
        # open aseessment file
        assessment_instance_file = open(f"{root_path}/{folder}/Assessment_instances/asessment_" + id + "_instances.json")
        temp_list = json.loads(assessment_instance_file.read())

        # add all asessment instance id to list
        for j in temp_list:
            instance_id = j['assessment_instance_id']
            if instance_id not in assessment_instance_list:
                assessment_instance_list.append(instance_id)
    
    return assessment_instance_list

def get_instance_questions(assessment_instance_list):
    #get quesitons
    tid = []
    for i in range(len(assessment_instance_list)):
        id = str(assessment_instance_list[i])
        instance_questions_url = "/assessment_instances/" + id + "/instance_questions"
        thread= threading.Thread(target=downloadFile, args=[instance_questions_url, f"{root_path}/{folder}/Instance_questions/assessment_instance_" + id + "_instance_questions.json"])
        tid.append(thread)
        thread.start()
    for i in tid:
        i.join()

def get_instance_submission(assessment_instance_list):
    #get submissions
    tid = []
    for i in range(len(assessment_instance_list)):
        id = str(assessment_instance_list[i])
        submissions_url = "/assessment_instances/" + id + "/submissions"
        tread = threading.Thread(target=downloadFile, args=[submissions_url, f"{root_path}/{folder}/Submissions/assessment_instance_" + id + "_submissions.json"])
        tid.append(thread)
        tread.start()
    for i in tid:
        i.join()
    

def get_instance_log(assessment_instance_list):
    #get logs
    tid = []
    for i in range(len(assessment_instance_list)):
        id = str(assessment_instance_list[i])
        log_url = "/assessment_instances/" + id + "/log"
        tread = threading.Thread(target=downloadFile, args=[log_url, f"{root_path}/{folder}/Log/assessment_instance_" + id + "_log.json"])
        tid.append(thread)
        tread.start()
    for i in tid:
        i.join()

def init():
    if not os.path.exists(f"{root_path}/{folder}"):
        os.makedirs(f"{root_path}/{folder}")
    
    if not os.path.exists(f"{root_path}/{folder}/Submissions"):
        os.makedirs(f"{root_path}/{folder}/Submissions")

    if not os.path.exists(f"{root_path}/{folder}/Log"):
        os.makedirs(f"{root_path}/{folder}/Log")
    
    if not os.path.exists(f"{root_path}/{folder}/Instance_questions"):
        os.makedirs(f"{root_path}/{folder}/Instance_questions")

    if not os.path.exists(f"{root_path}/{folder}/Assessment_instances"):
        os.makedirs(f"{root_path}/{folder}/Assessment_instances")
  

if __name__ == "__main__":
    args = parser.parse_args()
    course_instance = args.course_instance
    access_token = args.access_token
    root_path = args.root_path
    folder = args.folder
    init()
    get_assessment_list()
    get_gradebook()
    get_assessment_instance()
    assessment_instance_list = get_assessment_instance_list()
    get_instance_questions(assessment_instance_list)
    get_instance_submission(assessment_instance_list)
    get_instance_log(assessment_instance_list)


