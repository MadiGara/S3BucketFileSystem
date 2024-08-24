#!/usr/bin/env python3

# Madison Gara
# Code referenced: listBuckets.py and createBucket.py written and distributed by professor Yan Yan, W24

import os
import boto3
import configparser
import A1functions as a1f

# determine which method is being called and execute it
def execute(opt, s3, s3_res, pwd):
    custom_opt = opt.split(" ")[0]
    if custom_opt == "create_bucket":
        a1f.create_bucket(s3, opt, pwd)
    elif custom_opt == "list":
        a1f.list_dir(s3, s3_res, opt, pwd)
    elif custom_opt == "locs3cp":
        a1f.copy_local(s3_res, opt)
    elif custom_opt == "cwlocn":
        a1f.s3pwd(pwd)
    elif custom_opt == "chlocn":
        a1f.s3cd(s3, opt, pwd)
    elif "cd" in opt:
        try:
            if opt == "cd ..":
                pwd = os.path.normpath(os.getcwd())
                dir = (pwd.rsplit("/", 1))[0]
            else:
                opt = os.path.normpath(opt)
                dir = (opt.split(" ", 1))[1]
            os.chdir(dir)  
        except:
            print("Error: Could not perform local directory operation.")
            return 1
    else:
        os.system(opt)

    
# run the shell and validate s3 session
def main():
    pwd = ["/"]
    #find access keys from configuration file
    config = configparser.ConfigParser()
    try:
        config.read("S5-S3.conf")

        access_key = config['default']['aws_access_key_id']
        secret_access_key = config['default']['aws_secret_access_key']

        # establish boto3 session
        try:
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key
            )
            s3 = session.client('s3')
            s3_res = session.resource('s3')
            print("Welcome to the AWS S3 Storage Shell (S5).\nYou are now connected to your S3 storage!")
        except:
            print("Welcome to the AWS S3 Storage Shell (S5).\nYou could not be connected to your S3 storage.")
            print("Please review procedures for authenticating your account on AWS S3.")
    except:
        print("Welcome to the AWS S3 Storage Shell (S5).\nYou could not be connected to your S3 storage.")
        print("Please review procedures for authenticating your account on AWS S3.")
        print("An error occurred while reading your .conf file.")

    # loop the shell for user input
    while True:
        opt = input("S5> ")
        # exit conditions
        if opt == "exit" or opt == "quit":
            break
        # execute command
        else:
            execute(opt, s3, s3_res, pwd)
            
                          
if __name__ == "__main__":
    main()

#END
