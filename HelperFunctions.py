#!/usr/bin/env python3

# Madison Gara
# Code referenced: listBuckets.py and createBucket.py written and distributed by professor Yan Yan, W24

import os

# copy local file to s3
def copy_local(s3_res, opt):
    opts = opt.split(" ")
    pwd = os.popen("pwd").read()
    pwd = pwd.strip()

    try:
        path_rel = opts[1]
        # deal with possible relative paths in setting local path
        path = pwd + "/" + path_rel
        path = path.replace("//","/")

        #no repeat names allowed
        sections = path.split("/")
        path_str = ""
        for section in sections:
            if section in path_str:
                pass
            else:
                path_str = path_str + "/" + section
        path = os.path.normpath(path_str)
        
        # handle complete bucket path
        bucket_obj = opts[2].split("/", 2)
        bucket = bucket_obj[1]
        obj_path = bucket_obj[2]

        try:
            s3_res.Object(bucket, obj_path).upload_file(Filename=path)
            return 0
        except:
            print("Error: Could not copy local file from:", path, "To S3 location:", opts[2], "as bucket or file path may be incorrect or inaccessible.")
            return 1  
    except:
        print("Error: Please ensure command is in the form pathOfLocalFile /bucket/furtherDirectoriesAndFileName")
        return 1


# create a bucket in the current directory
def create_bucket(s3, opt, pwd):
    name = opt.split(" ")
    try:
        name = name[1]
        if name[0] != "/":
            print("Error: Cannot create bucket, bucket name must be in format /bucket")
            return 1
        name = name.replace("/","")
        try: 
            if pwd[0] != "/":
                print("Error: S3 does not allow the creation of buckets inside other buckets.")
                return 1
            else:
                s3.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': 'ca-central-1'})
                return 0
        except:
            print("Error: Cannot create bucket", name, "as bucket name must be unique.")
            return 1    
    except:
        print("Error: Cannot create bucket, bucket name must be in format /bucketName")
        return 1


# change current S3 directory and track pwd
# format: chlocn /bucket/dirpath
def s3cd(s3, opt, pwd):
    try:
        paths = opt.split(" ")
        # [0] is the command itself
        path = paths[1] 
        # save original path to revert to in case of error
        orig_pwd = pwd[0]

        # for root no directory switching needed
        if path == "/":
            pwd[0] = "/"

        # whole path specified with command
        elif ".." not in path:
            bucket = (path.split("/", 2))[1]

            # handle going between buckets off the rip, check if pwd is a different bucket
            try:
                possible_bucket = pwd[0].split("/")
                possible_bucket = possible_bucket[1]

                test = s3.head_bucket(
                        Bucket=possible_bucket
                    )  
                test = test['ResponseMetadata']['HTTPStatusCode']
                if test == 200:
                    pwd[0] = str(path)
                # existing directories added to if relative path and not a bucket switch
                else:
                    pwd[0] = str(pwd[0]) + str(path)

                # root directory fixed for repetition if full path
                pwd[0] = (pwd[0]).replace("//","/")
                # if full path added to partway in path, ensure all / sections unique
                sections = (pwd[0]).split("/")
                pwd_str = ""
                for section in sections:
                    if section in pwd_str:
                        pass
                    else:
                        pwd_str = pwd_str + "/" + section
                pwd[0] = pwd_str
                
                dirs = path.split("/")
                # check for bucket only in path switch
                if len(dirs) < 3:
                    response = s3.head_bucket(
                        Bucket=bucket
                    )  
                    response = response['ResponseMetadata']['HTTPStatusCode']
                    # 200 OK response means bucket exists, keep pwd as is
                    if response == 200:
                        return 0
                    else:
                        print("Error: Bucket", bucket, "does not exist or cannot be accessed.")
                        pwd[0] = orig_pwd
                        return 1
            except:
                # the repeated code here should have been a function but I didn't have time for that
                pwd[0] = str(pwd[0]) + str(path)

                # root directory fixed for repetition if full path
                pwd[0] = (pwd[0]).replace("//","/")
                # if full path added to partway in path, ensure all / sections unique
                sections = (pwd[0]).split("/")
                pwd_str = ""
                for section in sections:
                    if section in pwd_str:
                        pass
                    else:
                        pwd_str = pwd_str + "/" + section
                pwd[0] = pwd_str
                
                dirs = path.split("/")
                # check for bucket only in path switch
                if len(dirs) < 3:
                    response = s3.head_bucket(
                        Bucket=bucket
                    )  
                    response = response['ResponseMetadata']['HTTPStatusCode']
                    # 200 OK response means bucket exists, keep pwd as is
                    if response == 200:
                        return 0
                    else:
                        print("Error: Bucket", bucket, "does not exist or cannot be accessed.")
                        pwd[0] = orig_pwd
                        return 1
                
            # check that bucket and non-bucket objects in path switch exist
            else:
                response = s3.head_bucket(
                    Bucket=bucket
                )  
                response = response['ResponseMetadata']['HTTPStatusCode']
                if response == 200:
                    # check objects in the directories (dirs) list
                    end = len(dirs)
                    dirs = dirs[2:end]
                    for dir in dirs:
                        if dir != "" and dir != " ":
                            try:
                                obj_response = s3.head_object(
                                    Bucket=bucket,
                                    Key=dir
                                )
                                obj_response = obj_response['ResponseMetadata']['HTTPStatusCode']
                                if obj_response != 200:
                                    print("Error: Object", dir, "does not exist or cannot be accessed.")
                                    pwd[0] = orig_pwd
                                    return 1
                                else:
                                    return 0
                                
                            # check for object being a directory; if both fail, return 1
                            except:
                                try:
                                    dir = dir + "/"
                                    obj_response = s3.head_object(
                                        Bucket=bucket,
                                        Key=dir
                                    )
                                    obj_response = obj_response['ResponseMetadata']['HTTPStatusCode']
                                    if obj_response != 200:
                                        print("Error: Object", dir, "does not exist or cannot be accessed.")
                                        pwd[0] = orig_pwd
                                        return 1
                                    else:
                                        return 0
                                except:
                                    print("Error: Object", dir, "does not exist or cannot be accessed.")
                                    pwd[0] = orig_pwd
                                    return 1
                else:
                    print("Error: Bucket", bucket, "does not exist or cannot be accessed.")
                    pwd[0] = orig_pwd
                    return 1 

        #.. or ../.. commands - if going backwards, assuming backtracked dirs have been checked when initially entered
        else:
            if pwd[0] == "/":
                print("Error: Cannot move up the hierarchy, as you are already in the root directory.")
                pwd[0] = orig_pwd
                return 1
            
            # count number of ".." in path and move back that number
            else:
                split_path = pwd[0].split("/")
                # handle first / blank separation
                split_dirs = split_path[1:len(split_path)]
                num_dirs = len(split_dirs)
                dot_count = path.count("..")

                # make sure we don't try to go back more directories than we have
                if dot_count <= num_dirs:
                    remaining = num_dirs - dot_count
                    count = 0
                    pwd_temp = ""

                    while count < remaining:
                        pwd_temp = pwd_temp + "/" + split_dirs[count]
                        count += 1
                    if pwd_temp == "":
                        pwd[0] = "/"
                    else:
                        pwd[0] = pwd_temp

                else:
                    print("Error: Cannot move back", dot_count, "directories, as only", num_dirs, "previous director(y/ies) exist(s).")
                    pwd[0] = orig_pwd
                    return 1
        return 0
    
    except:
        print("Error: Cannot change to location " + path + ". Please ensure you add a / at the start of the path and that the directory exists.")
        pwd[0] = orig_pwd
        return 1
    

# pwd equivalent for S3 directories - directory changes are manually tracked in chlocn
def s3pwd(pwd):
    try:
        print(pwd[0])
        return 0
    except:
        print("Error: Cannot return current working directory in S3 space.")
        return 1
    

# list current directory items
def list_dir(s3, s3_res, opt, pwd):
    buckets = []
    path = opt.split(" ")
    if len(path) > 1:
        path = path[1]
    else:
        path = pwd[0]

    try:
        empty = True
        if path == "/":
            response = s3.list_buckets()
            for bucket in response['Buckets']:
                buckets.append(bucket["Name"])
            for bucket in buckets:
                print(bucket)
                empty = False
            if (empty == True):
                print("There is nothing under the root directory yet.")
            return 0

        # list objects in the buckets depending on pwd
        else:
            # handle going between buckets off the rip, check if pwd is a different bucket
            try:
                possible_bucket = pwd[0].split("/")
                possible_bucket = possible_bucket[1]

                test = s3.head_bucket(
                        Bucket=possible_bucket
                    )  
                test = test['ResponseMetadata']['HTTPStatusCode']
                if test == 200:
                    path = str(path)
                # existing directories added to if relative path and not a bucket switch
                else:
                    path = str(pwd[0]) + str(path)

                 # root directory fixed for repetition if full path
                path = path.replace("//","/")
                # if full path added to partway in path, ensure all / sections unique
                sections = (path).split("/")
                path_str = ""
                for section in sections:
                    if section in path_str:
                        pass
                    else:
                        path_str = path_str + "/" + section
                if path_str == "":
                    path_str = "/"
                path = path_str

                bucket = (path.split("/", 2))[1]
                dirs = path.split("/")

                # handle just bucket 
                if (len(dirs) < 3):
                    printed = False
                    for obj in s3_res.Bucket(bucket).objects.all():
                        slashes = obj.key.count("/")
                        #folder under bucket cannot have a period in it
                        if (slashes == 1) and ("." not in obj.key):
                            print(obj.key)
                            printed = True
                        elif slashes < 1:
                            print(obj.key)
                            printed = True
            
                # handle bucket plus additional directories
                else:
                    printed = False
                    nonbucket_path = (path.split("/", 2))[2]
                    nonbucket_path_s = nonbucket_path + "/"

                    # get directory's contents
                    for obj in s3_res.Bucket(bucket).objects.filter(Prefix=nonbucket_path_s):
                        if obj.key != nonbucket_path_s and obj.key != nonbucket_path:
                            # get file/folder name without path from key
                            name_list = (obj.key).split("/")
                            if name_list[-1] != "" and name_list[-1] != " ":
                                print(name_list[-1])
                                printed = True
                            else:
                                print(name_list[-2])
                                printed = True
                    
                    # if not a directory (would have a slash at the end), there cannot be anything below it, therefore leaf node
                    if (printed == False):
                        print("Error: The specified path", path, "doesn't exist, designates a file object, or is an empty directory. There are no objects under it to list.")
                        return 1
                return 0
            
            # repeat code here should be a function, but other priorities
            except:
                path = str(pwd[0]) + str(path)
                # root directory fixed for repetition if full path
                path = path.replace("//","/")
                # if full path added to partway in path, ensure all / sections unique
                sections = (path).split("/")
                path_str = ""
                for section in sections:
                    if section in path_str:
                        pass
                    else:
                        path_str = path_str + "/" + section
                if path_str == "":
                    path_str = "/"
                path = path_str

                bucket = (path.split("/", 2))[1]
                dirs = path.split("/")

                # handle just bucket 
                if (len(dirs) < 3):
                    printed = False
                    for obj in s3_res.Bucket(bucket).objects.all():
                        slashes = obj.key.count("/")
                        #folder under bucket cannot have a period in it
                        if (slashes == 1) and ("." not in obj.key):
                            print(obj.key)
                            printed = True
                        elif slashes < 1:
                            print(obj.key)
                            printed = True

                    if (printed == False):
                        print("Error: The specified path", path, "doesn't exist, designates a file object, or is an empty directory. There are no objects under it to list.")
                        return 1
            
                # handle bucket plus additional directories
                else:
                    printed = False
                    nonbucket_path = (path.split("/", 2))[2]
                    nonbucket_path_s = nonbucket_path + "/"

                    # get directory's contents
                    for obj in s3_res.Bucket(bucket).objects.filter(Prefix=nonbucket_path_s):
                        if obj.key != nonbucket_path_s and obj.key != nonbucket_path:
                            # get file/folder name without path from key
                            name_list = (obj.key).split("/")
                            if name_list[-1] != "" and name_list[-1] != " ":
                                print(name_list[-1])
                                printed = True
                            else:
                                print(name_list[-2])
                                printed = True
                    
                    # if not a directory (would have a slash at the end), there cannot be anything below it, therefore leaf node
                    if (printed == False):
                        print("Error: The specified path", path, "doesn't exist, designates a file object, or is an empty directory. There are no objects under it to list.")
                        return 1
                return 0
    except:
        print("Error: Cannot list contents of", path, "S3 location. Please ensure you put a forward slash at the front of the path and that you have permission to access the objects on it.")
        return 1    
    
#END
