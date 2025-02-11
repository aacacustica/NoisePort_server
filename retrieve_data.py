import os
import boto3
import pandas as pd
import re
from logging_config import setup_logging
from config import *




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

    # download path: HOME/RESULTADOS
    download_path = os.path.join(home_dir, local_dir)
    os.makedirs(download_path, exist_ok=True)
    logger.info(f"Downloading files into: {download_path}\n")

    # downloading each file that has not been processed yet
    for obj in response['Contents']:
        key = obj['Key']
        logger.info(f"Key: {key}")

        if key not in already_processed:
            # last part of the key as the file name
            file_name = key.split("/")[-1]
            # if the key contains subdirectories, mirror that structure locally
            sub_dirs = key.split("/")[:-1]
            sub_path = os.path.join(*sub_dirs) if sub_dirs else ""
            local_sub_dir = os.path.join(download_path, sub_path)
            os.makedirs(local_sub_dir, exist_ok=True)
            
            local_file_path = os.path.join(local_sub_dir, file_name)
            
            logger.info(f"Downloading {key} to {local_file_path}")
            s3.download_file(bucket_name, key, local_file_path)
            
            # mark file as processed
            with open(processed_list, 'a') as f:
                f.write(key + "\n")
        else:
            logger.warning(f"Already processed: {key}")




def concatenate_acous_csv_by_hour(folder_path: str, logger, output_subdir='hourly', concatenated_list='concatenated_files.txt'):
    """
    Recursively finds all CSV files in folder_path that match the pattern
    'YYYYMMDD_HHMMSS.csv', concatenates their data, groups the rows by hour
    (based on the 'Timestamp' column), and writes one CSV per hour (named 'YYYY-MM-DD_HH.csv')
    into an output subfolder. It also tracks which CSV files have already been processed
    using a tracking file located in the current directory.
    """
    pattern = re.compile(r'^\d{8}_\d{6}\.csv$')

    concatenated_list_file = os.path.join(os.getcwd(), concatenated_list)
    
    already_concatenated = set()
    if os.path.exists(concatenated_list_file):
        with open(concatenated_list_file, 'r') as f:
            for line in f:
                already_concatenated.add(line.strip())
    


    csv_dataframes = []
    new_files = [] 
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if pattern.match(file):
                full_path = os.path.join(root, file)
                acoustic_path = root
          
                # relative path for tracking purposes
                rel_path = os.path.relpath(full_path, folder_path)
                
                # skip files that have been processed before
                if rel_path in already_concatenated:
                    logger.info(f"Skipping already concatenated CSV: {rel_path}")
                    continue
                
                
                try:
                    df = pd.read_csv(full_path)
                    # df['id'] = "24F3190361CBDDE4"
                    # # save
                    # df.to_csv(full_path, index=False)
                    csv_dataframes.append(df)
                    new_files.append(rel_path)  #new file
                
                except Exception as e:
                    logger.warning(f"Error reading {full_path}: {e}")
    
    if not csv_dataframes:
        logger.warning("No new CSV files found for concatenation.")
        return

    # concatenate all 
    all_data = pd.concat(csv_dataframes, ignore_index=True)
    if 'Timestamp' not in all_data.columns:
        logger.error("Error: 'Timestamp' column not found in the data.")
        return

    try:
        all_data['Timestamp'] = pd.to_datetime(all_data['Timestamp'])
    
    except Exception as e:
        logger.error(f"Error converting 'Timestamp' to datetime: {e}")
        return

    
    
    output_dir = os.path.join(acoustic_path, output_subdir)
    os.makedirs(output_dir, exist_ok=True)



    # floor timestamp to the hour 
    all_data['Hour'] = all_data['Timestamp'].dt.floor('h')

    for hour, group in all_data.groupby('Hour'):
        group = group.sort_values(by='Timestamp')
        
        # filename
        filename = hour.strftime("%Y-%m-%d_%H") + ".csv"
        output_file = os.path.join(output_dir, filename)
        logger.info(f"Writing {len(group)} records to {output_file}")
        
        # remove the 'Hour' column before saving
        group = group.drop(columns=['Hour'])
        #save
        group.to_csv(output_file, index=False)
        logger.info("Save csv final hourly file")


    # update tracking file
    if new_files:
        with open(concatenated_list_file, 'a') as f:
            for file in new_files:
                f.write(file + "\n")
    logger.info("Updating the tracking file")



def main():
    # initialize logger
    logger = setup_logging('retrive_data.log')
    home_dir = os.getenv("HOME")
    resultados_folder = os.path.join(home_dir, "RESULTADOS")
    
    # {1}
    # download_new_files(BUCKET_NAME, home_dir,logger)
    
    
    # [2]
    concatenate_acous_csv_by_hour(resultados_folder,logger)



if __name__ == "__main__":
    main()
