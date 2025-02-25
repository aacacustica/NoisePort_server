import os
import boto3
import pandas as pd
import re
from logging_config import setup_logging
from config import *
from tqdm import tqdm




def download_new_files(bucket_name, home_dir, logger, local_dir='RESULTADOS', processed_list='processed_files.txt'):
    """
    Downloads new files from the S3 bucket into the folder ~/RESULTADOS.
    Keeps track of downloaded files in the processed_files.txt file.
    """
    logger.info("Initializing S3")
    logger.info(f"Home directory: {home_dir}")

    s3 = boto3.client('s3')

    # tracking already downloaded files
    already_processed = set()
    if os.path.exists(processed_list):
        with open(processed_list, 'r') as f:
            for line in f:
                already_processed.add(line.strip())

    logger.info("\nListing files in bucket")
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' not in response:
        logger.warning("No files in S3 bucket.")
        return


    # for k, v in response.items():
    #     #print(k)
    
    # #print(response.keys())
    # #print(response.values())
    # #print(response.items())
    # #print(response.get('Contents'))
    
    # #print(response['Contents'][0])
    # len_response = len(response['Contents'])
    # #print(len_response)

    # contents = response.get('Contents', [])
    # paths = [item['Key'] for item in contents]
    # #print(contents)
    # #print()
    
    
    # path = paths[0] 
    # #print(path)
    # #print(type(path))
    # path = path.split("/")[:-1]
    # #print(path)
    # path = os.path.join(*path)
    # #print(path)

    # exit()

    # #print()
    # #print(home_dir)
    

    # download path: <PROYECT>/RESULTADOS
    # download_path = os.path.join(home_dir, local_dir)
    # os.makedirs(download_path, exist_ok=True)
    # logger.info(f"Downloading files into: {download_path}\n")

    # downloading each file that has not been processed yet
    for obj in tqdm(response['Contents']):
        key = obj['Key']
        logger.info(f"Key: {key}")
        #print(f"Key: {key}")
        

        if key not in already_processed:
            # last part of the key as the file name
            file_name = key.split("/")[-1]
            #print(f"Filename is --> {file_name}")
            # if the key contains subdirectories, mirror that structure locally
            sub_dirs = key.split("/")[:-1]
            sub_path = os.path.join(*sub_dirs) if sub_dirs else ""
            #print(home_dir)
            #print(sub_path)
            
            
            # local_sub_dir = os.path.join(download_path, sub_path)
            local_sub_dir = os.path.join(home_dir, sub_path)
            #print(local_sub_dir)
            
            os.makedirs(local_sub_dir, exist_ok=True)
            local_file_path = os.path.join(local_sub_dir, file_name)
            #print(local_file_path)
            
            
            
            logger.info(f"Downloading {key} to {local_file_path}")
            s3.download_file(bucket_name, key, local_file_path)
            
            # mark file as processed
            with open(processed_list, 'a') as f:
                f.write(local_file_path + "\n")


        else:
            logger.warning(f"Already processed: {key}")







def main():
    # initialize logger
    logger = setup_logging('retrive_data')
    home_dir = os.getenv("HOME")
    

    # {1}
    download_new_files(BUCKET_NAME, home_dir,logger)
    

if __name__ == "__main__":
    main()
