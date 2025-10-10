import json
import mysql.connector
import paho.mqtt.client as mqtt
import os
import pandas as pd
import tqdm 
import decimal
import ssl


from logging_config import setup_logging
from utils import *
from config import *

from ast import literal_eval

import wave
import contextlib
from datetime import datetime

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



def load_data_db(db, data_path, logger, table_name):
    cursor = db.cursor()
    try:


        # 2) load new CSV
        if table_name == ACOUSTIC_TABLE_NAME:
            query_load = f"""
            LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (
            sensor_id,
            Filename,
            Timestamp,
            Unixtimestamp,
            LA, LC, LZ, LAmax, LAmin,
            `12.6Hz`, `15.8Hz`, `20.0Hz`, `25.1Hz`, `31.6Hz`,
            `39.8Hz`, `50.1Hz`, `63.1Hz`, `79.4Hz`, `100.0Hz`,
            `125.9Hz`, `158.5Hz`, `199.5Hz`, `251.2Hz`, `316.2Hz`,
            `398.1Hz`, `501.2Hz`, `631.0Hz`, `794.3Hz`, `1000.0Hz`,
            `1258.9Hz`, `1584.9Hz`, `1995.3Hz`, `2511.9Hz`,
            `3162.3Hz`, `3981.1Hz`, `5011.9Hz`, `6309.6Hz`,
            `7943.3Hz`, `10000.0Hz`, `12589.3Hz`, `15848.9Hz`
            );
            """
            cursor.execute(query_load)
            db.commit()
            logger.info("Acoustic data successfully loaded in the DB")
        elif table_name == PREDICT_TABLE_NAME:
            query_load = f""" 
            LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE {table_name} 
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"' 
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (
            id,
             Prediction_1,
             Prediction_2, 
             Prediction_3, 
             Prob_1, 
             Prob_2, 
             Prob_3, 
             Filename, 
             Timestamp 
             );
            """
            cursor.execute(query_load)
            db.commit()
            logger.info("Predictios data successfully loaded in the DB")
        elif table_name == WAV_TABLE_NAME:
            query_load = f"""
            LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE {WAV_TABLE_NAME}
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (filename, timestamp, duration);           
            """
            cursor.execute(query_load)
            db.commit()
            logger.info("WAV data successfully loaded in the DB")

    except mysql.connector.Error as err:
        logger.error("Error loading data: %s", err)
        db.rollback()

    finally:
        cursor.close()



def power_laeq_avg(db, logger, table_name=ACOUSTIC_TABLE_NAME):
    cursor = db.cursor(dictionary=True)
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



def process_acoustic_folder(db,logger,folder_days,all_info,query_folder,processed_folder,processed_folder_txt):
    # 1) clear old data --> comment this when db is working fine
    for day in tqdm.tqdm(folder_days, desc="[Acoustics] Processing days", unit="day"):
        # checking if the day is already processed
        if day in processed_folder:
            logger.info("[Acoustics] Already processed: %s", day)
            continue

        # ------------------------------------
        # 1-Taking day string to save the concat file
        # ------------------------------------
        try:
            day_str = day.split("/")[-1]
            logger.info("[Acoustics] Processing day_hour: %s", day_str)
            logger.info("[Acoustics] Processing: %s", day)
        except Exception as e:
            logger.error(f"[Acoustics] Error processing day: {e}")
            continue

        # ------------------------------------
        # 2-Appending to list csv files in csv per day folder
        # ------------------------------------
        try:
            csv_files = os.listdir(day)
            csv_files = [csv_file for csv_file in csv_files if csv_file.endswith(".csv")]
            logger.info("[Acoustics] CSV files in %s: %s", day, csv_files)
            csv_files = [os.path.join(day, csv_file) for csv_file in csv_files]
        except Exception as e:
            logger.error(f"[Acoustics] Error listing CSV files: {e}")
            continue

        # ------------------------------------
        # 3-Concatenation of csv files for one hour processing
        # ------------------------------------
        try:
            logger.info("")
            # concatenating the csv files
            logger.info("[Acoustics] Trying to concatenate the csv files to process one hour of audio data recordings")
            df_day = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files], ignore_index=True)
        except Exception as e:
            logger.error(f"[Acoustics] Error concatenating CSV files: {e}")
            continue

        # ------------------------------------
        # 4-ordering by timestamp and turning the result into a csv so we can use it
        # ------------------------------------
        try:
            df_day = df_day.sort_values(by=["Timestamp"])
            # round LA values to 2 decimal places
            df_day["LA"] = df_day["LA"].round(1)
            # print(df_day)
            # exit()

            # make result csv_file
            csv_concat_path = os.path.join(query_folder, f"{day_str}.csv")
            logger.info("[Acoustics] Concatenated CSV file path: %s", csv_concat_path)

            # save csv file
            df_day.to_csv(os.path.join(query_folder, f"{day_str}.csv"), index=False)
            logger.info("[Acoustics] Concatenated CSV files, saved as: %s", csv_concat_path)
        except Exception as e:
            logger.error(f"[Acoustics] Error saving concatenated CSV file: {e}")
            continue

        # ------------------------------------
        # 5-Loading ACOUSTIC csv into the DB table
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Acoustics] Loading data into TABLE")
            load_data_db(db, csv_concat_path, logger,table_name=ACOUSTIC_TABLE_NAME)
            cur = db.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {ACOUSTIC_TABLE_NAME}")
            n = cur.fetchone()[0]
            logger.info(f"[Acoustics] → {ACOUSTIC_TABLE_NAME} contains {n} rows after LOAD DATA")
            cur.close()
        except Exception as e:
            logger.error(f"[Acoustics] Error loading data into database: {e}")
            continue

        # ------------------------------------
        # 6-query and convert results to json
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Acoustics] Query and Convert Results to JSON")
            avg_results = power_laeq_avg(db, logger)
            # print(avg_results)
            logger.info(avg_results)
            # addig the "day", which is "/mnt/sandisk/CONTENEDORES/CONTENEDORES/P2_CONTENEDORES/acoustic_params/20250407_03" to the avg_results
            for result in avg_results:
                result["day_path"] = day

            logger.info("[Acoustics] Power LAeq Average Results:")
            logger.info(avg_results)
            # exit()

            if avg_results is not None:
                logger.info("[Acoustics] Power LAeq Average Results:")
                # send the data MQTT
                send_mqtt_data(avg_results, logger)
            else:
                logger.warning("[Acoustics] No results returned from power_laeq_avg query.")
        except Exception as e:
            logger.error(f"[Acoustics] Error querying and converting results to JSON: {e}")
            continue

        # append the avg_results to the all_info list
        all_info.append(avg_results)
        # print(all_info)

        # ------------------------------------
        # 7-Update processed folder
        # ------------------------------------
        try:
            logger.info("")
            update_processed_folder(processed_folder_txt, day)
            processed_folder = load_processed_folder(processed_folder_txt)
            logger.info("[Acoustics] Added to processed files: %s", day)
        except Exception as e:
            logger.error(f"[Acoustics] Error updating processed files: {e}")
            continue

def process_pred_folder(db,logger,folder_days, all_info, query_folder, processed_folder, processed_folder_txt):
    
    # 1) clear old data --> comment this when db is working fine
    

    for day in tqdm.tqdm(folder_days, desc="[Predictions] Processing days", unit="day"):
        if day in processed_folder:
            logger.info("Already processed: %s", day)
            continue
        # ------------------------------------
        # 1-Taking day string to save the concat file
        # ------------------------------------

        try:
            day_str = day.split("/")[-1]
            logger.info("[Predictions] Processing day_hour: %s", day_str)
            logger.info("[Predictions] Processing: %s", day)
        except Exception as e:
            logger.error(f"[Predictions] Error processing day: {e}")
            continue
        # ------------------------------------
        # 2-Appending to list csv files in csv per day folder
        # ------------------------------------
        try:
            csv_files = os.listdir(day)
            csv_files = [csv_file for csv_file in csv_files if csv_file.endswith("1.0.csv")]
            logger.info("[Predictions] CSV files in %s: %s", day, csv_files)
            csv_files = [os.path.join(day, csv_file) for csv_file in csv_files]
        except Exception as e:
            logger.error(f"[Predictions] Error listing CSV files: {e}")
            continue
        # ------------------------------------
        # 3-Concatenation of csv files for one hour processing
        # ------------------------------------
        try:
            logger.info("")
            # concatenating the csv files
            logger.info("[Predictions] Trying to concatenate the csv files to process one hour of audio data recordings")
            df_day = pd.concat([pd.read_csv(csv_file,converters={
                'class': literal_eval,
                'probability': literal_eval}) for csv_file in csv_files], ignore_index=True)
        except Exception as e:
            logger.error(f"[Predictions] Error concatenating CSV files: {e}")
            continue
        # ------------------------------------
        # 4-ordering by timestamp 
        # exploding prediction and probability columns  
        # turning the result into a csv so we can use it
        # rearranging df columns so it fits in the table
        # ------------------------------------
        try:
            df_day = df_day.sort_values(by=["Timestamp"])
            df_day["prediction1"],df_day["prediction2"],df_day["prediction3"] = zip(*list(df_day['class'].values))
            df_day['probability1'],df_day['probability2'],df_day['probability3'] = zip(*list(df_day['probability'].values))
            
            df_out = df_day.rename(columns={
                'prediction1': 'Prediction_1',
                'prediction2': 'Prediction_2',
                'prediction3': 'Prediction_3',
                'probability1': 'Prob_1',
                'probability2': 'Prob_2',
                'probability3': 'Prob_3'
            })

            cols = ['Prediction_1','Prediction_2','Prediction_3',
                    'Prob_1','Prob_2','Prob_3',
                    'Filename','Timestamp']

            df_out = df_out[cols]
            
            # make result csv_file
            csv_concat_path = os.path.join(query_folder, f"{day_str}.csv")
            
            logger.info("[Predictions] Concatenated CSV file path: %s", csv_concat_path)

            # save csv file
            df_out.to_csv(os.path.join(query_folder, f"{day_str}.csv"), index=False)
            logger.info("[Predictions] Concatenated CSV files, saved as: %s", csv_concat_path)
        except Exception as e:
            logger.error(f"[Predictions] Error saving concatenated CSV file: {e}")
            continue
        # ------------------------------------
        # 5-Loading PREDICTIONS csv into the DB table
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Predictions] Loading data into TABLE")
            load_data_db(db, csv_concat_path, logger,table_name=PREDICT_TABLE_NAME)
            cur = db.cursor()
            cur.execute(f"USE {DATABASE_NAME}")
            cur.execute(f"SELECT COUNT(*) FROM {PREDICT_TABLE_NAME}")
            n = cur.fetchone()[0]
            logger.info(f"[Predictions] → {PREDICT_TABLE_NAME} contains {n} rows after LOAD DATA")
            cur.close()
        except Exception as e:
            logger.error(f"[Predictions] Error loading data into database: {e}")
            continue
        # ------------------------------------
        # 6-query and convert results to json
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Predictions] Query and Convert Results to JSON")
            avg_results = power_laeq_avg(db, logger)
            # print(avg_results)
            logger.info(avg_results)
            # addig the "day", which is "/mnt/sandisk/CONTENEDORES/CONTENEDORES/P2_CONTENEDORES/acoustic_params/20250407_03" to the avg_results
            for result in avg_results:
                result["day_path"] = day

            logger.info("[Predictions] Power LAeq Average Results:")
            logger.info(avg_results)
            # exit()

            if avg_results is not None:
                logger.info("[Predictions] Power LAeq Average Results:")
                # send the data MQTT
                send_mqtt_data(avg_results, logger)
            else:
                logger.warning("[Predictions] No results returned from power_laeq_avg query.")
        except Exception as e:
            logger.error(f"[Predictions] Error querying and converting results to JSON: {e}")
            continue
        # ------------------------------------
        # 7-Update processed folder #TODO: check if this is necessary
        # ------------------------------------
        try:
            logger.info("")
            update_processed_folder(processed_folder_txt, day)
            processed_folder = load_processed_folder(processed_folder_txt)
            logger.info("[Predictions] Added to processed files: %s", day)
        except Exception as e:
            logger.error(f"[Predictions] Error updating processed files: {e}")
            continue

def process_wav_folder(db,logger,folder_days, all_info, query_folder, processed_folder, processed_folder_txt):

    for day in tqdm.tqdm(folder_days, desc="[Wave Files] Processing days", unit="day"):
        if day in processed_folder:
            logger.info("Already processed: %s", day)
            continue
        # ------------------------------------
        # 1-Taking day string to save the concat file
        # ------------------------------------

        try:
            day_str = day.split("/")[-1]
            logger.info("[Wave Files] Processing day_hour: %s", day_str)
            logger.info("[Wave Files] Processing: %s", day)
        except Exception as e:
            logger.error(f"[Wave Files] Error processing day: {e}")
            continue
        # ------------------------------------
        # 2-Reading wav time lengths from wav folder
        # ------------------------------------
        try:
            duration = []
            
            for wavfile in os.listdir(day):
                if wavfile.endswith(".wav"):
                    with contextlib.closing(wave.open(os.path.join(day,wavfile),'r')) as f:
                            frames = f.getnframes()
                            rate = f.getframerate()
                            duration_wav = frames / float(rate)
                            duration.append(duration_wav)
        except Exception as e:
            logger.error(f"[Wave Files] Error listing CSV files: {e}")
            continue
        # ------------------------------------
        # 3-Creating csvs with filename, timestamp and duration
        # ------------------------------------
        try:
            logger.info("")
            # concatenating the csv files
            logger.info("[Wave Files] Trying to create csv files with filename, timestamp and duration")
            df_day = pd.DataFrame(columns=['Filename','Timestamp','Duration'])

            df_day['Filename'] = os.listdir(day)
            df_day['Duration'] = duration
            df_day['Timestamp'] = pd.to_datetime(
                df_day['Filename'].astype(str).str.replace('.wav', '', regex=False),
                format='%Y%m%d_%H%M%S',
                errors='raise'  # usa 'coerce' si quieres NaT cuando no coincida
            )

        except Exception as e:
            logger.error(f"[Predictions] Error concatenating CSV files: {e}")
            continue
        # ------------------------------------
        # 4-ordering by timestamp 
        # saving csv to wav_files_query folder
        # ------------------------------------
        try:
            df_day = df_day.sort_values(by=["Timestamp"])


            # make result csv_file
            csv_concat_path = os.path.join(query_folder, f"{day_str}.csv")
            
            # save csv file
            df_day.to_csv(os.path.join(query_folder, f"{day_str}.csv"), index=False)
            logger.info(f"[Wave Files] Concatenated CSV files, saved as:{csv_concat_path}" )

        except Exception as e:
            logger.error(f"[Wave Files] Error saving concatenated CSV file: {e}")
            continue
        # ------------------------------------
        # 5-Loading PREDICTIONS csv into the DB table
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Wave Files] Loading data into TABLE")
            load_data_db(db, csv_concat_path, logger,table_name=WAV_TABLE_NAME)
            cur = db.cursor(buffered=True)
            cur.execute(f"USE {DATABASE_NAME}")
            cur.execute(f"SELECT COUNT(*) FROM {WAV_TABLE_NAME}")
            n = cur.fetchone()[0]
            logger.info(f"[Wave Files] → {WAV_TABLE_NAME} contains {n} rows after LOAD DATA")
            cur.close()
        except Exception as e:
            logger.error(f"[Wave Files] Error loading data into database: {e}")
            continue
        # ------------------------------------
        # 6-query and convert results to json
        # ------------------------------------
        try:
            logger.info("")
            logger.info("[Wave Files] Query and Convert Results to JSON")
            avg_results = power_laeq_avg(db, logger)
            # print(avg_results)
            logger.info(avg_results)
            # addig the "day", which is "/mnt/sandisk/CONTENEDORES/CONTENEDORES/P2_CONTENEDORES/acoustic_params/20250407_03" to the avg_results
            for result in avg_results:
                result["day_path"] = day

            logger.info("[Wave Files] Power LAeq Average Results:")
            logger.info(avg_results)
            # exit()

            if avg_results is not None:
                logger.info("[Wave Files] Power LAeq Average Results:")
                # send the data MQTT
                send_mqtt_data(avg_results, logger)
            else:
                logger.warning("[Wave Files] No results returned from power_laeq_avg query.")
        except Exception as e:
            logger.error(f"[Wave Files] Error querying and converting results to JSON: {e}")
            continue
        # ------------------------------------
        # 7-Update processed folder #TODO: check if this is necessary
        # ------------------------------------
        try:
            logger.info("")
            update_processed_folder(processed_folder_txt, day)
            processed_folder = load_processed_folder(processed_folder_txt)
            logger.info("[Wave Files] Added to processed files: %s", day)
        except Exception as e:
            logger.error(f"[Wave Files] Error updating processed files: {e}")
            continue


def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    # logger
    logger = setup_logging('query_automatize')

    # database
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True)

    logger.info("Initializing database!")
    # testing the query database
    if DB_INIT_SWITCH: initialize_database(db, logger)


    # paths and processed csv_files
    logger.info("Starting!!")
    logger.info("")
    

    try:
        # config
        logger.info("Getting the element form the yamnl file")
        id_micro, location_record, location_place, location_point, \
        audio_sample_rate, audio_window_size, audio_calibration_constant,\
        storage_s3_bucket_name, storage_output_wav_folder, \
        storage_output_acoust_folder = load_config_acoustic('config.yaml')
        logger.info("Config loaded successfully")   
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return


    # [1] setup the folder to process
    path = SANDISK_PATH_LINUX
    
    # check if it exist
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
    # exit()
    all_info = []
    for point in tqdm.tqdm(points, desc="Processing points", unit="point"):
        if "P2_CONTENEDORES" in point:
            try:
                # ---------------------------
                point_str = point.split("/")[-1]
                acoust_folder = os.path.join(point, storage_output_acoust_folder)
                logger.info(f"Acoustic params folder: {acoust_folder}")
                # checking if the folder exist
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
                logger.info("")
                folder_days = os.listdir(acoust_folder)
                # filter the FILES, FUST THE FOLDERS
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
            
            
            for folder in os.listdir(point):

                if folder == 'acoustic_params'  and ACOUSTIC_QUERY_SWITCH:
                    folder_days = get_desired_query_folder(folder_days,folder)
                    logger.info("Starting ACOUSTIC FOLDER processing")
                    process_acoustic_folder(db,logger,folder_days, all_info, query_acoustic_folder, processed_acoustics, processed_folder_acoustic_txt)
                    logger.info("Finished ACOUSTIC FOLDER processing")
                if folder == 'predictions_litle' and PREDICT_QUERY_SWITCH:
                    folder_days = get_desired_query_folder(folder_days,folder)
                    logger.info("Starting PREDICTION FOLDER processing")
                    process_pred_folder(db,logger,folder_days, all_info, query_pred_folder, processed_predictions, processed_folder_predictions_txt)
                    logger.info("Finished PREDICTION FOLDER processing")
                if folder == 'wav_files' and WAV_QUERY_SWITCH:
                    folder_days = get_desired_query_folder(folder_days,folder)
                    logger.info("Starting WAV FOLDER processing")
                    process_wav_folder(db,logger,folder_days, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)
                    logger.info("Finished WAV FOLDER processing")
        else:
            logger.info("Folder not found")
            continue

    # ------------------------------------
    # save all_info to json
    # ------------------------------------
    logger.info("")
    logger.info("Saving all_info to JSON")
    logger.info("all_info: %s", all_info)

    # adding the folder processed to the all_info

    all_info_path = os.path.join(query_acoustic_folder, f"{point_str}_all.json")
    with open(all_info_path, "w") as f:
        json.dump(all_info, f, indent=4, default=decimal_to_native)
    logger.info("Saved all_info to: %s", all_info_path)

        

    #CLOSING THE DB
    try:
        logger.info("")
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)


if __name__ == "__main__":
    main()