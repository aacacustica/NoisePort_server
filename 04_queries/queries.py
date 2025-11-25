import json
import mysql.connector
import paho.mqtt.client as mqtt
import os
import pandas as pd
import tqdm 
import decimal
import ssl
import time
import sys
import shutil
import re
import shutil
import re

sys.path.insert(0, "/home/aac_s3_test/noisePort_server/04_queries")

from processing import *
from logging_config import *
from utils import *
from config import *
from utils_queries import *
from time_slop_fix import *

PATH = SANDISK_PATH_LINUX
ISDIR = os.path.isdir(PATH)

ID_MICRO, LOCATION_RECORD, LOCATION_PLACE, LOCATION_POINT, \
AUDIO_SAMPLE_RATE, AUDIO_WINDOW_SIZE, AUDIO_CALIBRATION_CONSTANT,\
STORAGE_S3_BUCKET_NAME, STORAGE_OUTPUT_WAV_FOLDER, \
STORAGE_OUTPUT_ACOUSTIC_FOLDER = load_config_acoustic('config.yaml')

FILENAME_TS_RE = re.compile(r'(\d{8}_\d{6})')  # extrae 'YYYYMMDD_HHMMSS'
logger = setup_logging('query_automatize')

def acoustic_processing(folder_days,folder,db,logger, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt):
    
    folder_days = get_desired_query_folder(folder_days,folder)                  
    logger.info("Starting ACOUSTIC FOLDER processing")
    start_time = time.time()                 
    process_acoustic_folder(db,logger,folder_days, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt)                 
    
    end_time =  round(time.time() - start_time,2)
    return end_time

def wav_processing(folder_days,folder,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt):

    folder_days = get_desired_query_folder(folder_days,folder)                   
    logger.info("Starting WAV FOLDER processing")
    start_time = time.time()                   
    process_wav_folder(db,logger,folder_days, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)                   
    
    end_time =  round(time.time() - start_time,2)
    return end_time

def prediction_processing(folder_days,folder,db,logger, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt):
    
    folder_days = get_desired_query_folder(folder_days,folder)                 
    logger.info("Starting PREDICTION FOLDER processing")
    start_time = time.time()
    process_pred_folder(db,logger,folder_days, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt)                 
    end_time =  round(time.time() - start_time,2)
    return end_time

def sonometer_processing(folder,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt):
    
    sonometer_path = point + f'/{folder}'                    
    logger.info("Starting PREDICTION FOLDER processing")
    start_time = time.time()
    process_sonometer_folder(db,logger,sonometer_path,query_sonometer_folder,processed_folder_sonometer_txt)
    end_time =  round(time.time() - start_time,2)
    return end_time



def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    
    
    
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True)

    logger.info("[Queries] Initializing database!")
    
    if DB_INIT_SWITCH: initialize_database(db, logger)

    logger.info("[Queries] Starting!!")
     
    if ISDIR:
        logger.info(f"PATH exists --> {PATH}")
    else:
        raise ValueError(f'PATH ({PATH}) doesnt exist.')
    
    points = load_points()
    logger.info(f"[Queries] Points to query: {points}")

    all_info = []
    for point in tqdm.tqdm(points, desc="Processing points", unit="point"):
        if "TENERIFE_SEND_MQTT" in point:
            try:
                
                # ---------------------------
                # GET ACOUSTIC FOLDER PATH AND POINT NAME STRING
                # ---------------------------
                (
                    point_str,
                    acoust_folder

                ) = get_acoust_and_point(logger,point)
              
                if os.path.isdir(acoust_folder):
                    logger.info(f"Folder exists: {acoust_folder}")
                else:
                    logger.warning(f"Folder does not exist: {acoust_folder}")
                    continue
                
                # ---------------------------
                # CREATING QUERY FOLDERS IF THEY DONT EXIST
                # ---------------------------

                (
                    query_acoustic_folder,
                    query_pred_folder,
                    query_wav_folder,
                    query_sonometer_folder

                ) = create_query_folders(point,logger)

                # ---------------------------
                # INIZIALATIN PROCESSING FILES
                # ---------------------------
        
                (
                    processed_folder_acoustic_txt,
                    processed_folder_predictions_txt,
                    processed_folder_wav_txt,processed_folder_sonometer_txt,
                    processed_acoustics,
                    processed_predictions,
                    processed_wavs,
                    processed_sonometers

                 )  = initialize_process_files(query_acoustic_folder,query_pred_folder,query_wav_folder,query_sonometer_folder,logger)

                logger.info(f"Saving the processed list of predictions files txt here --> {processed_folder_predictions_txt}")
                logger.info(f"Saving the processed list of acoustics files txt here -->  {processed_folder_acoustic_txt}")
                logger.info(f"Saving the processed list of wav files txt here -->  {processed_folder_predictions_txt}")
                logger.info(f"Saving the processed list of sonometer files txt here -->  {processed_folder_sonometer_txt}")

            except Exception as e:
                logger.error(f"Error setting up folders: {e}")
                continue



            try:

                # ------------------------------------
                #   Filter the FILES, JUST THE FOLDERS
                #   Filter the FILES, JUST THE FOLDERS
                # ------------------------------------

                logger.info("")
                folder_days = os.listdir(acoust_folder)
                folder_days = [day_folder for day_folder in folder_days if os.path.isdir(os.path.join(acoust_folder, day_folder))]
                logger.info("Folder days in %s: %s", acoust_folder, folder_days)
                
                folder_days_acoustics_predictions = [os.path.join(acoust_folder, day_folder) for day_folder in folder_days if 'fixed' in day_folder]
                
                folder_days_wavs = [os.path.join(acoust_folder,day_folder) for day_folder in folder_days ]
                folder_days_wavs = [file.replace('acoustic_params','wav_files') for file in folder_days_wavs]
                folder_days_wavs = [file.replace('fixed_','') for file in folder_days_wavs]
                
                folder_days_acoustics_predictions = [os.path.join(acoust_folder, day_folder) for day_folder in folder_days if 'fixed' in day_folder]
                
                folder_days_wavs = [os.path.join(acoust_folder,day_folder) for day_folder in folder_days ]
                folder_days_wavs = [file.replace('acoustic_params','wav_files') for file in folder_days_wavs]
                folder_days_wavs = [file.replace('fixed_','') for file in folder_days_wavs]
            except Exception as e:
                logger.error(f"Error listing folder days: {e}")
                continue



            try:

                # --------------------------------------------------
                #   FIX OF THE EXTRA SECONDS IN MINUTE PROBLEM     
                # --------------------------------------------------
                time_slop_fix(point)
            
            except Exception as e:

                logger.error(f"Error while applying the time slop fix to csv records: {e}")
                continue

                # -.--------------------
                # PROCESSING
                # ----------------------
            
            try:

                whole_start_time = time.time()
                
                for folder in os.listdir(point):
                    
                    
                    if folder == 'acoustic_params'  and ACOUSTIC_QUERY_SWITCH:
                        
                        logger.info("Starting ACOUSTIC processing")
                        end_time = 0
                        #end_time = acoustic_processing(folder_days_acoustics_predictions,folder,db,logger, all_info, query_acoustic_folder, processed_wavs, processed_folder_acoustic_txt)
                        print(" --- %s seconds in execution ---" %end_time)                      
                    
                    if folder == 'predictions_litle' and PREDICT_QUERY_SWITCH:
                        
                        logger.info("Starting PREDICTIONS processing")
                        end_time = 0
                        #end_time = prediction_processing(folder_days_acoustics_predictions,folder,db,logger, all_info, query_pred_folder, processed_wavs, processed_folder_predictions_txt)
                        print(" --- %s seconds in execution ---" %end_time)  
                    
                    if folder == 'wav_files' and WAV_QUERY_SWITCH:
                        
                        logger.info("Starting WAV FILES processing")
                        end_time = 0 
                        #wav_processing(folder_days_wavs,folder,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)
                        print(" --- %s seconds in execution ---" %end_time)  
            
                    if folder == 'sonometer_files' and SONOMETER_QUERY_SWITCH:  
                        
                        logger.info("Starting SONOMETER FOLDER processing")
                        end_time = sonometer_processing(folder,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt)
                    
                        power_avg_results = power_laeq_avg(db,logger,table_name=SONOMETER_TABLE_NAME) 
                        records_sent_txt = "/mnt/sandisk/CONTENEDORES/CONTENEDORES/5-Resultados/TENERIFE_SEND_MQTT/SPL/SONOMETER/sonometer_acoustics_query/records_sent.txt"
                        send_mqtt_data(power_avg_results,logger,records_sent_txt)
                        
                        print(" --- %s seconds in execution ---" %end_time)
                
                    print(" --- %s seconds in total execution ---" % round(time.time() - whole_start_time,2))
        
            except Exception as e:
                logger.error(f"Error while processing folders: {e}")
        
        
        else:
            logger.info("Folder not found")
            continue

    # ------------------------------------
    #   Save all_info to json
    # ------------------------------------
    
    logger.info("")
    logger.info("Saving all_info to JSON")
    logger.info("all_info: %s", all_info)
    json.dump(all_info, sys.stdout, indent=4, default=decimal_to_native)
    json.dump(all_info, sys.stdout, indent=4, default=decimal_to_native)

    # ------------------------------------
    #   Adding the folder processed to the all_info
    # ------------------------------------
    
    all_info_path = os.path.join(query_acoustic_folder, f"{point_str}_all.json")
    with open(all_info_path, "w") as f:
        json.dump(all_info, f, indent=4, default=decimal_to_native)
    logger.info("Saved all_info to: %s", all_info_path)

        

    # ------------------------------------
    #   Closing DB
    # ------------------------------------
    
    try:
        logger.info("")
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)


if __name__ == "__main__":
    main()