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

sys.path.insert(0, "/home/aac_s3_test/noisePort_server/04_queries")

from processing import *
from logging_config import *
from utils import *
from config import *



def initialize_database(db, logger):
    """Ensure that the database and table exist, recreating them from scratch."""
    cursor = None
    try:
        logger.info("Ensuring database and tables exist (recreating tables)…")
        cursor = db.cursor(buffered=True)

        # 1) Create the database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};")
        logger.info(f"Created database if not exists: {DATABASE_NAME}")

        cursor.execute(f"USE {DATABASE_NAME};")
        logger.info(f"Using database: {DATABASE_NAME}")

        # 2) Drop any existing tables (updated schema)
        for table_name in TABLES:
            logger.info(f"Dropping table if exists: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

        # 3) Recreate tables from your TABLES dict
        for table_name, create_stmt in TABLES.items():
            logger.info(f"Creating table → {table_name}")
            cursor.execute(create_stmt)
            cursor.execute(f"DESCRIBE {table_name};")
            structure = cursor.fetchall()
            logger.info(f"Structure for {table_name}: {structure}")

        db.commit()
        logger.info("Database and tables have been recreated successfully.")

    except mysql.connector.Error as err:
        logger.error("Error initializing database: %s", err)
        # re-raise or sys.exit here if this is fatal. think of it

    finally:
        if cursor:
            try:
                cursor.close()
            except mysql.connector.Error as err:
                logger.error("Error closing cursor: %s", err)



def load_data_db(db, data_path, logger, table_name=ACOUSTIC_TABLE_NAME):
    cursor = db.cursor(dictionary=True)
    

    if table_name == ACOUSTIC_TABLE_NAME: query_load = QUERYS['load_acoustics_db'].format(data_path=data_path,table_name=table_name)
    if table_name == WAV_TABLE_NAME: query_load = QUERYS['load_wavs_db'].format(data_path=data_path,table_name=table_name)
    if table_name == PREDICT_TABLE_NAME: query_load = QUERYS['load_preds_db'].format(data_path=data_path,table_name=table_name)
    if table_name == SONOMETER_TABLE_NAME: query_load = QUERYS['load_sonometers_db'].format(data_path=data_path,table_name=table_name) 
    
    try:
        # execute the query to load data.
        cursor.execute(query_load)
        # commit the transaction so changes are saved.
        db.commit()
        logger.info("Data loaded successfully")
    except mysql.connector.Error as err:
        logger.error("Error loading data: %s", err)
        db.rollback()
    finally:
        cursor.close()
        # db.close() 


def get_columns_for_table(table_name):
    """
    Devuelve la lista de columnas para cada tabla como strings,
    para usar en LOAD DATA LOCAL INFILE.
    """
    
    if table_name == ACOUSTIC_TABLE_NAME:
        return [
            "sensor_id", "Filename", "Timestamp", "Unixtimestamp",
            "LA","LC","LZ","LAmax","LAmin",
            "`1/3 LZeq 6.3`", "`1/3 LZeq 8.0`", "`1/3 LZeq 10.0`", "`1/3 LZeq 12.5`","`1/3 LZeq 16.0`",
            "`1/3 LZeq 20.0`", "`1/3 LZeq 25.0`", "`1/3 LZeq 31.5`", "`1/3 LZeq 40.0`", "`1/3 LZeq 50.0`",
            "`1/3 LZeq 63.0`", "`1/3 LZeq 80.0`", "`1/3 LZeq 100`", "`1/3 LZeq 125`", "`1/3 LZeq 160`",
            "`1/3 LZeq 200`", "`1/3 LZeq 250`", "`1/3 LZeq 315`", "`1/3 LZeq 400`", "`1/3 LZeq 500`",
            "`1/3 LZeq 630`", "`1/3 LZeq 800`", "`1/3 LZeq 1000`", "`1/3 LZeq 1250`", "`1/3 LZeq 1600`",
            "`1/3 LZeq 2000`", "`1/3 LZeq 2500`", "`1/3 LZeq 3150`"," `1/3 LZeq 4000`"," `1/3 LZeq 5000`",
            "`1/3 LZeq 6300`", "`1/3 LZeq 8000`", "`1/3 LZeq 10000`", "`1/3 LZeq 12500`", "`1/3 LZeq 16000`"," `1/3 LZeq 20000`"

        ]
    elif table_name == PREDICT_TABLE_NAME:
        return ["id","Prediction_1","Prediction_2","Prediction_3",
                "Prob_1","Prob_2","Prob_3","Filename","Timestamp"]
    elif table_name == WAV_TABLE_NAME:
        return ["filename","timestamp","duration"]
    elif table_name == SONOMETER_TABLE_NAME:
        return [
            "sensor_id", "Filename", "Timestamp", "Unixtimestamp",
            "LA","LC","LAmax","LAmin",
            "`1/3 LZeq 6.3`", "`1/3 LZeq 8.0`", "`1/3 LZeq 10.0`", "`1/3 LZeq 12.5`","`1/3 LZeq 16.0`",
            "`1/3 LZeq 20.0`", "`1/3 LZeq 25.0`", "`1/3 LZeq 31.5`", "`1/3 LZeq 40.0`", "`1/3 LZeq 50.0`",
            "`1/3 LZeq 63.0`", "`1/3 LZeq 80.0`", "`1/3 LZeq 100`", "`1/3 LZeq 125`", "`1/3 LZeq 160`",
            "`1/3 LZeq 200`", "`1/3 LZeq 250`", "`1/3 LZeq 315`", "`1/3 LZeq 400`", "`1/3 LZeq 500`",
            "`1/3 LZeq 630`", "`1/3 LZeq 800`", "`1/3 LZeq 1000`", "`1/3 LZeq 1250`", "`1/3 LZeq 1600`",
            "`1/3 LZeq 2000`", "`1/3 LZeq 2500`", "`1/3 LZeq 3150`"," `1/3 LZeq 4000`"," `1/3 LZeq 5000`",
            "`1/3 LZeq 6300`", "`1/3 LZeq 8000`", "`1/3 LZeq 10000`", "`1/3 LZeq 12500`", "`1/3 LZeq 16000`"," `1/3 LZeq 20000`"
        ]
    else:
        return []



def power_laeq_avg(db, logger, table_name=ACOUSTIC_TABLE_NAME):
    cursor = db.cursor(dictionary=True)
    if table_name == SONOMETER_TABLE_NAME:
        query = f"""
        SELECT
        sensor_id,
        MIN(Unixtimestamp)                  AS unixtimestamp,
        10 * LOG10(AVG(POWER(10, LAeq/10)))   AS AVG_LAeq,
        MAX(LAmax)                          AS max_LAmax,
        MIN(LAmin)                          AS min_LAmin
        FROM {table_name}
        GROUP BY sensor_id;
        """
    else:
            query = f"""
        SELECT
        sensor_id,
        MIN(Unixtimestamp)                  AS unixtimestamp,
        10 * LOG10(AVG(POWER(10, LA/10)))   AS AVG_LAeq,
        MAX(LAmax)                          AS max_LAmax,
        MIN(LAmin)                          AS min_LAmin
        FROM {table_name}
        GROUP BY sensor_id;
        """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Computed overall averages for {cursor.rowcount} sensors")
        return rows
    except mysql.connector.Error as err:
        logger.error("Error executing query: %s", err)
        return None
    finally:
        cursor.close()




def send_mqtt_data(data, logger):
    payload = json.dumps(data, default=str)
    
    try:
        if data and isinstance(data, list) and isinstance(data[0], dict) and "sensor_id" in data[0]:
            sensor_id = data[0]["sensor_id"]
        else:
            sensor_id = "unknown"
    except Exception as e:
        logger.error("Error extracting sensor_id: %s", e)
        sensor_id = "unknown"
    
    # topic using the sensor_id
    topic = f"aacacustica/{sensor_id}"
    
    
    #MQTT client and connect. Version1 and 2 compatibility to avoid deprecation warning

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except:
        client = mqtt.Client()
    if DEMO:
        # ensure port is an integer 
        port = int(MQTT_PORT_DEMO)
        
        client.connect(MQTT_BROKER_DEMO, port, 60)
    
        # Publish payload to the topic, save the info inside the broker
        # client.publish(topic, payload)
        client.publish(topic, payload, qos=1, retain=True) # keep it false
        logger.info("Connected to MQTT broker at %s:%s", MQTT_BROKER_DEMO, port)
        logger.info("Published data to topic '%s': %s", topic, payload)
    
    
    else:
        # ensure port is an integer 
        port = int(MQTT_PORT_MUUTECH)

        
        # connect to the broker using TLS trusting the server certificate
        client.username_pw_set(MQTT_USER_MUUTECH, MQTT_PASSWORD_MUUTECH)
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)


        #connect & publish
        client.connect(MQTT_BROKER_MUUTECH, port, keepalive=60)
        client.publish(topic, payload, qos=1, retain=True)
        logger.info("Connected to MQTT broker at %s:%s", MQTT_BROKER_MUUTECH, port)
        logger.info("Published data to topic '%s' via TLS: %s", topic, payload)
        
    client.disconnect()



def load_processed_folder(processed_folder_path):
    """Load the set of processed filenames from a text file."""
    if os.path.exists(processed_folder_path):
        with open(processed_folder_path, "r") as f:
            return {line.strip() for line in f if line.strip()}
    return set()



def update_processed_folder(processed_folder_path, filename):
    """Append a processed filename to the text file."""
    with open(processed_folder_path, "a") as f:
        f.write(filename + "\n")


def decimal_to_native(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")




def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    
    logger = setup_logging('query_automatize')
    
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True)

    logger.info("Initializing database!")
    
    if DB_INIT_SWITCH: initialize_database(db, logger)

    logger.info("Starting!!")
    logger.info("")
    
    try:
        
        logger.info("Getting the element form the yamnl file")
        id_micro, location_record, location_place, location_point, \
        audio_sample_rate, audio_window_size, audio_calibration_constant,\
        storage_s3_bucket_name, storage_output_wav_folder, \
        storage_output_acoust_folder = load_config_acoustic('config.yaml')
        logger.info("Config loaded successfully")   
    
    except Exception as e:
        
        logger.error(f"Error loading config: {e}")
        
        return

    
    path = SANDISK_PATH_LINUX
        
    isdir = os.path.isdir(path)
    
    if isdir:
        logger.info(f"Path exists --> {path}")
    else:
        logger.warning(f"Path does not exist --> {path}")
        path = SANDISK_PATH_WINDOWS
        isdir = os.path.isdir(path)
        if isdir:
            logger.info(f"Path exists --> {path}")
        else:
            raise ValueError(f'Path ({path}) doesnt exist.')

    
    logger.info("")
    points = [point for point in os.listdir(path)]
    points = [os.path.join(path, point) for point in points]
    logger.info(f"These are the points: {points}")

    all_info = []
    for point in tqdm.tqdm(points, desc="Processing points", unit="point"):
        if "P2_CONTENEDORES" in point:
            try:
                
                
                point_str = point.split("/")[-1]
                acoust_folder = os.path.join(point, storage_output_acoust_folder)
                logger.info(f"Acoustic params folder: {acoust_folder}")
                                
                if os.path.isdir(acoust_folder):
                    logger.info(f"Folder exists: {acoust_folder}")
                else:
                    logger.warning(f"Folder does not exist: {acoust_folder}")
                    continue


                logger.info(f"")
                
                # ---------------------------
                # CREATING QUERY FOLDERS IF THEY DONT EXIST
                # ---------------------------
                
                query_acoustic_folder = os.path.join(point, "acoustic_params_query")
                query_pred_folder = os.path.join(point, "predictions_litle_query")
                query_acoustic_folder = os.path.join(point, "acoustic_params_query")
                query_wav_folder = os.path.join(point, "wav_files_query")
                query_sonometer_folder = os.path.join(point,"sonometer_acoustics_query")

                if not os.path.exists(query_acoustic_folder):
                    os.makedirs(query_acoustic_folder)
                    logger.info(f"Created Query folde: {query_acoustic_folder}")
                else:
                    logger.info(f"Folder query already exists: {query_acoustic_folder}")
              
                if not os.path.exists(query_pred_folder):
                    os.makedirs(query_pred_folder)
                    logger.info(f"Created output query_pred_folder: {query_pred_folder}")
                else:
                    logger.info(f"Folder predictions already exists: {query_pred_folder}")
                
                if not os.path.exists(query_wav_folder):
                    os.makedirs(query_wav_folder)
                    logger.info(f"Created output query_wav_folder: {query_wav_folder}")
                else:
                    logger.info(f"Folder wav_files already exists: {query_wav_folder}")


                # ---------------------------
                # INIZIALATIN PROCESSING FILES
                # ---------------------------

                
                processed_folder_acoustic_txt = os.path.join(query_acoustic_folder, "processed_acoustics.txt")
                processed_folder_predictions_txt = os.path.join(query_pred_folder, "processed_predictions.txt")
                processed_folder_wav_txt = os.path.join(query_wav_folder, "processed_wavs.txt")
                processed_folder_sonometer_txt = os.path.join(query_sonometer_folder,"processed_sonometers.txt")

                logger.info(f"Saving the proicessed file txt here --> {processed_folder_acoustic_txt}")
                processed_acoustics = load_processed_folder(processed_folder_acoustic_txt)
                processed_predictions = load_processed_folder(processed_folder_predictions_txt)
                processed_wavs = load_processed_folder(processed_folder_wav_txt)

                logger.info(f"Saving the processed list of predictions files txt here --> {processed_folder_predictions_txt}")
                logger.info(f"Saving the processed list of acoustics files txt here -->  {processed_folder_acoustic_txt}")
                logger.info(f"Saving the processed list of wav files txt here -->  {processed_folder_predictions_txt}")


            except Exception as e:
                logger.error(f"Error setting up folders: {e}")
                continue



            try:

                # ------------------------------------
                #   Filter the FILES, FUST THE FOLDERS
                # ------------------------------------

                logger.info("")
                folder_days = os.listdir(acoust_folder)
                folder_days = [day_folder for day_folder in folder_days if os.path.isdir(os.path.join(acoust_folder, day_folder))]
                logger.info("Folder days in %s: %s", acoust_folder, folder_days)
                folder_days = [os.path.join(acoust_folder, day_folder) for day_folder in folder_days]

                wav_folder = point + '/' + storage_output_wav_folder
            except Exception as e:
                logger.error(f"Error listing folder days: {e}")
                continue


            # -.--------------------
            # PROCESSING
            # ----------------------
            
            logger.info("")
            
            query_pred_folder = point + '/' + 'predictions_litle_query'
            query_acoustic_folder = point + '/' + 'acoustic_params_query'
            query_wav_folder = point + '/' + 'wav_files_query'
            
            whole_start_time = time.time()
            
            
            for folder in os.listdir(point):

                if folder == 'acoustic_params'  and ACOUSTIC_QUERY_SWITCH:
                    
                    folder_days = get_desired_query_folder(folder_days,folder)                  
                    logger.info("Starting ACOUSTIC FOLDER processing")
                    start_time = time.time()                 
                    process_acoustic_folder(db,logger,folder_days, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt)                 
                    
                    print(" --- %s seconds in execution ---" % round(time.time() - start_time,2))
                    logger.info("Finished ACOUSTIC FOLDER processing")
                
                if folder == 'predictions_litle' and PREDICT_QUERY_SWITCH:
                    
                    folder_days = get_desired_query_folder(folder_days,folder)                 
                    logger.info("Starting PREDICTION FOLDER processing")
                    start_time = time.time()
                    process_pred_folder(db,logger,folder_days, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt)                 
                    
                    print(" --- %s seconds in execution ---" % round(time.time() - start_time,2))
                    logger.info("Finished PREDICTION FOLDER processing")
                
                if folder == 'wav_files' and WAV_QUERY_SWITCH:
                    
                    folder_days = get_desired_query_folder(folder_days,folder)                   
                    logger.info("Starting WAV FOLDER processing")
                    start_time = time.time()                   
                    process_wav_folder(db,logger,folder_days, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)                   
                    
                    print(" --- %s seconds in execution ---" % round(time.time() - start_time,2))
                    logger.info("Finished WAV FOLDER processing")
        
                if folder == 'sonometer_files' and SONOMETER_QUERY_SWITCH:  
                   
                    logger.info("Starting SONOMETER FOLDER processing")
                    start_time = time.time()
                    sonometer_path = point + f'/{folder}'                   
                    process_sonometer_folder(db,logger,sonometer_path,processed_folder_sonometer_txt)
                    
                    print(" --- %s seconds in execution ---" % round(time.time() - start_time,2))

            print(" --- %s seconds in total execution ---" % round(time.time() - whole_start_time,2))
        
        
        
        
        else:
            logger.info("Folder not found")
            continue

    # ------------------------------------
    #   Save all_info to json
    # ------------------------------------
    
    logger.info("")
    logger.info("Saving all_info to JSON")
    logger.info("all_info: %s", all_info)

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