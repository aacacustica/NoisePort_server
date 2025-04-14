import os
import boto3
import pandas as pd
import re
from logging_config import setup_logging
from config import *
from tqdm import tqdm
import datetime



def load_downloaded_file(downloaded_file_path):
    """Load the set of downloaded filenames from a text file."""
    if os.path.exists(downloaded_file_path):
        with open(downloaded_file_path, "r") as f:
            return {line.strip() for line in f if line.strip()}
    return set()



def update_downloaded_file(downloaded_file_path, filename):
    """Append a downloaded filename to the text file."""
    with open(downloaded_file_path, "a") as f:
        f.write(filename + "\n")



def download_new_files(bucket_name, home_dir, logger, downloaded_list='downloaded_files.txt'):
    """
    Downloads new files from the S3 bucket into the folder ~/RESULTADOS.
    Keeps track of downloaded files in the downloaded_files.txt file.
    """
    logger.info("Initializing S3")
    logger.info(f"Home directory: {home_dir}")
    cwd = os.getcwd()
    logger.info(f"Current dir: {cwd}")


    # ---------------------------
    # INIZIALATIN PROCESSING FILE
    # ---------------------------
    downloaded_files_path = os.path.join(cwd, "01_retrieve_data")
    
    if os.path.exists(downloaded_files_path):
        download_txt_path = os.path.join(downloaded_files_path, downloaded_list) # "downloaded_acoustic.txt"
        logger.info(f"Downloaded file saved at: {download_txt_path}")
    else:
        logger.Error(f"downloaded_files_path doesn not exist {download_txt_path}")
    logger.info(f"Saving the downloaded file txt here --> {download_txt_path}")
    
    
    download_files = load_downloaded_file(download_txt_path)
    
    
    # ---------------------------
    # INIZIALATIN S3
    # ---------------------------
    s3 = boto3.client('s3')

    logger.info("\nListing files in bucket")
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' not in response:
        logger.warning("No files in S3 bucket.")
        return



    # downloading each file that has not been downloaded yet
    for obj in tqdm(response['Contents']):
        key = obj['Key']
        logger.info(f"Key: {key}")
        # print(f"Processing audio file --> {key}")

        if key in download_files:
            logger.info(f"Skipping {key}, already processed.")
            continue


        # last part of the key as the file name
        file_name = key.split("/")[-1]
        logger.info(f"Filename is --> {file_name}")
        # print(f"Filename is --> {file_name}")
        


        try:
            dt = datetime.datetime.strptime(file_name.split('.')[0], '%Y%m%d_%H%M%S')
            hour_folder = dt.strftime('%Y%m%d_%H')
        except Exception as e:
            logger.warning(f"Error parsing date from filename {file_name}: {e}")
            hour_folder = 'unknown_hour'



        # if the key contains subdirectories, mirror that structure locally
        sub_dirs = key.split("/")[:-1]
        sub_path = os.path.join(*sub_dirs) if sub_dirs else ""
        logger.info(sub_path)
        
        
        local_sub_dir = os.path.join(home_dir, sub_path, hour_folder)
        logger.info(f"Local subdirectory: {local_sub_dir}")
        
        
        os.makedirs(local_sub_dir, exist_ok=True)
        local_file_path = os.path.join(local_sub_dir, file_name)
        logger.info(local_file_path)
        

        #-----------------
        #DOWNLOAD
        #-----------------
        logger.info(f"Downloading {key} to {local_file_path}")
        s3.download_file(bucket_name, key, local_file_path)


        #-----------------
        #DELETE
        #-----------------
        # logger.info(f"Deleting {key} from S3 bucket {bucket_name}")
        # s3.delete_object(Bucket=bucket_name, Key=key)

        if file_name.lower().endswith('.wav'):
            logger.info(f"Deleting {key} from S3 bucket {bucket_name}")
            s3.delete_object(Bucket=bucket_name, Key=key)
        else:
            logger.info(f"Skipping deletion for non-wav file: {file_name}")



        #-------------------------------------
        #MARKING THE AUFIO FILE AS DOWNLOADED
        #-------------------------------------
        update_downloaded_file(download_txt_path, key)
        download_files.add(local_file_path)
            



def main():
    # initialize logger
    logger = setup_logging('retrive_data')
    home_dir = os.getenv("HOME")
    
    download_new_files(BUCKET_NAME, home_dir,logger)
    

if __name__ == "__main__":
    main()
