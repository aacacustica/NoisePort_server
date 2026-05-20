import mysql.connector
import json
import os
import tqdm 
import time
import sys
import argparse

sys.path.insert(0, "/home/aac/NoisePort_server/04_queries")
from processing import *
from logging_config import *
from utils import *
from config import *
from utils_queries import *
from time_slop_fix import *


PATH = SANDISK_PATH_LINUX_NEW
ISDIR = os.path.isdir(PATH)

ID_MICRO, LOCATION_RECORD, LOCATION_PLACE, LOCATION_POINT, \
AUDIO_SAMPLE_RATE, AUDIO_WINDOW_SIZE, AUDIO_CALIBRATION_CONSTANT,\
STORAGE_S3_BUCKET_NAME, STORAGE_OUTPUT_WAV_FOLDER, \
STORAGE_OUTPUT_ACOUSTIC_FOLDER = load_config_acoustic('config.yaml')

logger = setup_logging('query_automatize')

def arg_parser():
    parser = argparse.ArgumentParser(description='Generating acoustics/wav/predictions/sonometer queries')
    parser.add_argument('-p', '--point', type=str, required=True, 
                        help='Desired point to process')
    parser.add_argument('-m', '--send_mqtt', action='store_true',
                        help='To send or not the data to mqtt')
    parser.add_argument('-w', '--window', type=int, required=False, default=3600, 
                        help='Window for sonometer (1 for 1 sec, 3600 for hour)')
    parser.add_argument('-t', '--time_slop', action='store_true', 
                        help='Add a time slop fix to the process')
    parser.add_argument('-s', '--sonometer', action='store_true',
                        help='To process sonometer data')
    

    return parser.parse_args()


def acoustic_processing(folder_days,folder,db,logger, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt):
    
    logger.info("Starting ACOUSTIC FOLDER processing")
    start_time = time.time()                 
    process_acoustic_folder(db,logger,folder_days, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt)                   
    end_time =  round(time.time() - start_time,2)
    return end_time

def wav_processing(folder_days,folder,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt):

    logger.info("Starting WAV FOLDER processing")
    start_time = time.time()                   
    process_wav_folder(db,logger,folder_days, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)                   
    end_time =  round(time.time() - start_time,2)
    return end_time

def prediction_processing(folder_days,folder,db,logger, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt):
    
    logger.info("Starting PREDICTION FOLDER processing")
    start_time = time.time()
    process_pred_folder(db,logger,folder_days, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt)                 
    end_time =  round(time.time() - start_time,2)
    return end_time

def sonometer_processing(folder,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt,window):
    
    logger.info("Starting PREDICTION FOLDER processing")
    start_time = time.time()
    process_sonometer_folder(db,logger,folder,query_sonometer_folder,processed_folder_sonometer_txt,window)
    end_time =  round(time.time() - start_time,2)
    return end_time



def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    
    args = arg_parser()
    
    point_to_process    = args.point
    sonometer_window    = args.window
    time_slop           = args.time_slop
    send_mqtt           = args.send_mqtt
    process_sonometer   = args.sonometer
    
    db = mysql.connector.connect(
            host=HOST_NEW,
            user=USER_NEW,
            password=PASSWORD_NEW,
            allow_local_infile=True
            )
    
    if DB_INIT_SWITCH: initialize_database(db, logger)
    
    logger.info("[Queries] Initializing database!")
    
    logger.info("[Queries] Starting!!")
     
    if ISDIR:
        logger.info(f"PATH exists --> {PATH}")
    else:
        raise ValueError(f'PATH ({PATH}) doesnt exist.')
    
    points = load_points()
    logger.info(f"[Queries] Points to query: {points}")

    all_info = []
    #For sending MQTT TCT Data
    #tct_points = ['P1_CONTENEDORES','P2_CONTENEDORES','P3_CONTENEDORES','P4_CONTENEDORES']
     
    for point in tqdm.tqdm(points, desc="Processing points", unit="point"):
        if str(point_to_process) in point.split('/'):
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
                    processed_folder_wav_txt,
                    processed_folder_sonometer_txt,
                    processed_mqtt_data_txt_sonometer,
                    processed_mqtt_data_txt_acoustics,
                    processed_acoustics,
                    processed_predictions,
                    processed_wavs,
                    processed_sonometers

                 )  = initialize_process_files(query_acoustic_folder,query_pred_folder,query_wav_folder,query_sonometer_folder,logger)

                logger.info(f"Saving the processed list of predictions files txt here   -->             {processed_folder_predictions_txt}" )
                logger.info(f"Saving the processed list of acoustics files txt here     -->             {processed_folder_acoustic_txt}"    )
                logger.info(f"Saving the processed list of wav files txt here           -->             {processed_folder_predictions_txt}" )
                logger.info(f"Saving the processed list of sonometer files txt here     -->             {processed_folder_sonometer_txt}"   )

            except Exception as e:
                logger.error(f"Error setting up folders: {e}")
                continue



            try:

                # ------------------------------------
                #   Filter the FILES, JUST THE FOLDERS
                # ------------------------------------


                (
                    _,
                    _,
                    _,
                    _,
                    acoustics_params_folder_path,
                    predictions_litle_folder_path,
                    _,
                    _,
                    _,
                    _

                ) =  get_sonometer_rasp_acoustics_preds_days_and_paths(logger,point) 
                #No queremos aún el resto de rutas porque recoge los archivos fixed y aun no se han generado
            except Exception as e:
                logger.error(f"Error listing folder days: {e}")
                continue



            try:

                # --------------------------------------------------
                #   FIX OF THE EXTRA SECONDS IN MINUTE PROBLEM     
                # --------------------------------------------------
                
                if time_slop: time_slop_fix(point,acoustics_params_folder_path,predictions_litle_folder_path)
                #Recall to this function as now it gives us the updated folder with the time slop fix for the days record
                (
                    spl_folder_path,
                    AI_MODEL_folder_path,
                    wavs_folder_path,
                    sonometer_files_folder_path,
                    acoustics_params_folder_path,
                    predictions_litle_folder_path,
                    days_folders_wavs,
                    days_folders_acoustics,
                    days_folders_predictions,
                    points_folders_sonometer

                ) =  get_sonometer_rasp_acoustics_preds_days_and_paths(logger,point)  
                
            except Exception as e:

                logger.error(f"Error while applying the time slop fix to csv records: {e}")
                continue

                # -.--------------------
                # PROCESSING
                # ----------------------
            
            try:

                whole_start_time = time.time()
                  
                if ACOUSTIC_QUERY_SWITCH:
                    
                    logger.info("Starting ACOUSTIC processing")
                    end_time = acoustic_processing(days_folders_acoustics,acoustics_params_folder_path,db,logger, all_info, query_acoustic_folder, processed_wavs, processed_folder_acoustic_txt)
                    
                    if send_mqtt:
                        sensor_id = get_sensor_id_and_filter_query(days_folders_acoustics[0])
                        power_avg_results = power_laeq_avg(db,logger,sensor_id,table_name=ACOUSTIC_TABLE_NAME) 
                        send_mqtt_data(power_avg_results,logger,processed_mqtt_data_txt_acoustics)
                    
                    print("[acoustic_processing] --- %s seconds in execution ---" %end_time)                      
                
                if PREDICT_QUERY_SWITCH:
                    
                    logger.info("Starting PREDICTIONS processing")
                    end_time = prediction_processing(days_folders_predictions,predictions_litle_folder_path,db,logger, all_info, query_pred_folder, processed_wavs, processed_folder_predictions_txt)
                    print("[preditions_processing] --- %s seconds in execution ---" %end_time)  
                
                if WAV_QUERY_SWITCH:
                    
                    logger.info("Starting WAV FILES processing")
                    end_time = wav_processing(days_folders_wavs,wavs_folder_path,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)
                    print("[wavs_processing] --- %s seconds in execution ---" %end_time)  
        
                if SONOMETER_QUERY_SWITCH and process_sonometer:  
                    
                    logger.info("Starting SONOMETER FOLDER processing")
                    end_time = sonometer_processing(sonometer_files_folder_path,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt,sonometer_window)

                    if send_mqtt:
                        power_avg_results = power_laeq_avg(db,logger,None,table_name=SONOMETER_TABLE_NAME) 
                        send_mqtt_data(power_avg_results,logger,processed_mqtt_data_txt_acoustics)
                    
                    print("[sonometer_processing] --- %s seconds in execution ---" %end_time)
                
                print(  "\n"
                        "\n"
                        " --- %s seconds in total execution ---" "\n"
                         % round(time.time() - whole_start_time,2))
        
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
    """"
    all_info_path = os.path.join(query_acoustic_folder, f"{point_str}_all.json")
    with open(all_info_path, "w") as f:
        json.dump(all_info, f, indent=4, default=decimal_to_native)
    logger.info("Saved all_info to: %s", all_info_path)
"""
        

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