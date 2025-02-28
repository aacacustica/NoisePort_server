import os
import boto3
import pandas as pd
import re
from logging_config import setup_logging
from config import *
from tqdm import tqdm




def download_new_files(bucket_name, home_dir, logger, processed_list='processed_files.txt'):
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



    # downloading each file that has not been processed yet
    for obj in tqdm(response['Contents']):
        key = obj['Key']
        logger.info(f"Key: {key}")
        

        if key not in already_processed:
            # last part of the key as the file name
            file_name = key.split("/")[-1]
            logger.info(f"Filename is --> {file_name}")


            try:
                dt = datetime.datetime.strptime(file_name.split('.')[0], '%Y%m%d_%H%M%S')
                hour_folder = dt.strftime('%Y%m%d_%H')
            except Exception as e:
                logger.warning(f"Error parsing date from filename {file_name}: {e}")
                hour_folder = 'unknown_hour'

            # hour_folder


            # if the key contains subdirectories, mirror that structure locally
            sub_dirs = key.split("/")[:-1]
            sub_path = os.path.join(*sub_dirs) if sub_dirs else ""
            logger.info(sub_path)
            
            
            # local_sub_dir = os.path.join(download_path, sub_path)
            local_sub_dir = os.path.join(home_dir, sub_path)
            logger.info(local_sub_dir)
            
            os.makedirs(local_sub_dir, exist_ok=True)
            local_file_path = os.path.join(local_sub_dir, file_name)
            logger.info(local_file_path)
            
            
            
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
    
    download_new_files(BUCKET_NAME, home_dir,logger)
    

if __name__ == "__main__":
    main()
