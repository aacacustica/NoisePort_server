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

sys.path.insert(0, "/home/aac_s3_test/noisePort_server/04_queries")

from processing import *
from logging_config import *
from utils import *
from config import *

PATH = SANDISK_PATH_LINUX
ISDIR = os.path.isdir(PATH)

ID_MICRO, LOCATION_RECORD, LOCATION_PLACE, LOCATION_POINT, \
AUDIO_SAMPLE_RATE, AUDIO_WINDOW_SIZE, AUDIO_CALIBRATION_CONSTANT,\
STORAGE_S3_BUCKET_NAME, STORAGE_OUTPUT_WAV_FOLDER, \
STORAGE_OUTPUT_ACOUSTIC_FOLDER = load_config_acoustic('config.yaml')

FILENAME_TS_RE = re.compile(r'(\d{8}_\d{6})')  # extrae 'YYYYMMDD_HHMMSS'

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
        cursor.execute(query_load)
        db.commit()
        logger.info("Data loaded successfully")
    except mysql.connector.Error as err:
        logger.error("Error loading data: %s", err)
        db.rollback()
    finally:
        cursor.close()



def get_columns_for_table(table_name):
    
    """
    Devuelve la lista de columnas para cada tabla como strings,
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

def load_points():

    points = [point for point in os.listdir(PATH)]
    points = [os.path.join(PATH, point) for point in points]
    
    return points

def get_acoust_and_point(logger,point):
    
    point_str = point.split("/")[-1]
    acoust_folder = os.path.join(point, STORAGE_OUTPUT_ACOUSTIC_FOLDER)
    logger.info(f"Acoustic params folder: {acoust_folder}")

    return point_str,acoust_folder


def initialize_process_files(query_acoustic_folder,query_pred_folder,query_wav_folder,query_sonometer_folder,logger):

    processed_folder_acoustic_txt = os.path.join(query_acoustic_folder, "processed_acoustics.txt")
    processed_folder_predictions_txt = os.path.join(query_pred_folder, "processed_predictions.txt")
    processed_folder_wav_txt = os.path.join(query_wav_folder, "processed_wavs.txt")
    processed_folder_sonometer_txt = os.path.join(query_sonometer_folder,"processed_sonometers.txt")

    logger.info(f"Saving the proicessed file txt here --> {processed_folder_acoustic_txt}")
    
    processed_acoustics = load_processed_folder(processed_folder_acoustic_txt)
    processed_predictions = load_processed_folder(processed_folder_predictions_txt)
    processed_wavs = load_processed_folder(processed_folder_wav_txt)
    processed_sonometers = load_processed_folder(processed_folder_sonometer_txt)

    return processed_folder_acoustic_txt,processed_folder_predictions_txt,processed_folder_wav_txt,processed_folder_sonometer_txt,processed_acoustics,processed_predictions,processed_wavs,processed_sonometers

def create_query_folders(point,logger):
        
        point_path_results = point.replace("3-Medidas","5-Resultados")

        query_acoustic_folder = os.path.join(point_path_results,'SPL', "acoustic_params_query")
        query_pred_folder = os.path.join(point_path_results,'AI_MODEL', "predictions_litle_query")
        query_acoustic_folder = os.path.join(point_path_results,'SPL', "acoustic_params_query")
        query_wav_folder = os.path.join(point_path_results,'SPL', "wav_files_query")
        query_sonometer_folder = os.path.join(point_path_results,'SPL',"SONOMETER","sonometer_acoustics_query") #TEMPORAL FILE PATHING, CHANGE THIS WHEN FINAL STRUCTURE IS DONE

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

        if not os.path.exists(query_sonometer_folder):
            os.makedirs(query_sonometer_folder)
            logger.info(f"Created output query_sonometer_folder: {query_sonometer_folder}")
        else:
            logger.info(f"Folder sonometer_files already exists: {query_sonometer_folder}")

        return query_acoustic_folder, query_pred_folder, query_wav_folder,query_sonometer_folder

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
    process_sonometer_folder(db,logger,query_sonometer_folder,processed_folder_sonometer_txt)
    end_time =  round(time.time() - start_time,2)
    return end_time

# ---------------- HELPERS ----------------
def safe_read_timestamp_series(csv_path,logger, nrows=10):
    try:
        df = pd.read_csv(csv_path, nrows=nrows)
    except Exception as e:
        logger.debug(f"safe_read_timestamp_series: error leyendo {csv_path}: {e}")
        return None
    if df.empty or 'Timestamp' not in df.columns:
        logger.debug(f"safe_read_timestamp_series: {csv_path} vacío o sin 'Timestamp'.")
        return None
    ts = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
    if ts.isna().all():
        logger.debug(f"safe_read_timestamp_series: {csv_path} Timestamps no parseables.")
        return None
    return ts

def get_csv_reference_time(csv_path,logger):
    ts_series = safe_read_timestamp_series(csv_path,logger, nrows=10)
    if ts_series is None:
        return None
    first_valid = ts_series.dropna().iloc[0]
    return pd.to_datetime(first_valid, utc=True)

def sort_csvs_by_content_timestamp(folder,logger):
    csvs = [f for f in os.listdir(folder) if f.lower().endswith('.csv')]
    def _key(fname):
        p = os.path.join(folder, fname)
        ts = get_csv_reference_time(p,logger)
        return ts if ts is not None else pd.Timestamp.max
    return sorted(csvs, key=_key)

def get_next_hour_bucket(bucket_day_hour):
    try:
        day, hour = bucket_day_hour.split('_')
        hour = int(hour)
    except Exception:
        return None
    next_hour = hour + 1
    if next_hour < 24:
        return f"{day}_{next_hour:02d}"
    return None

def detect_minute_jump_by_content(prev_path, curr_path,logger, threshold_seconds=70):
    tprev = get_csv_reference_time(prev_path,logger)
    tcurr = get_csv_reference_time(curr_path,logger)
    if tprev is None or tcurr is None:
        return False
    diff = (tcurr - tprev).total_seconds()
    return diff > threshold_seconds

def get_extra_seconds_indices(csv_path,logger):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"get_extra_seconds_indices: error leyendo {csv_path}: {e}")
        return 0, []
    if df.empty or 'Timestamp' not in df.columns:
        return 0, []
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
    if df['Timestamp'].isna().all():
        return 0, []
    first_valid_idx = df['Timestamp'].first_valid_index()
    if first_valid_idx is None:
        return 0, []
    official_minute = df.at[first_valid_idx, 'Timestamp'].minute
    idxs = df[df['Timestamp'].dt.minute != official_minute].index.tolist()
    return len(idxs), idxs

def append_extra_seconds(previous_csv_path, current_csv_path, row_indices, fixed_folder_parent, bucket_name,logger):
    if not row_indices:
        return
    fixed_output_folder = os.path.join(fixed_folder_parent, f"fixed_{bucket_name}")
    os.makedirs(fixed_output_folder, exist_ok=True)
    prev_name = os.path.basename(previous_csv_path)
    curr_name = os.path.basename(current_csv_path)
    df_prev = pd.read_csv(previous_csv_path)
    df_curr = pd.read_csv(current_csv_path)
    try:
        rows_to_move = df_prev.loc[row_indices].copy()
    except Exception:
        rows_to_move = df_prev.iloc[row_indices].copy()
    df_prev = df_prev.drop(index=row_indices).reset_index(drop=True)
    df_curr = pd.concat([df_curr, rows_to_move], ignore_index=True)
    df_curr['Timestamp'] = pd.to_datetime(df_curr['Timestamp'], utc=True, errors='coerce')
    df_curr = df_curr.sort_values('Timestamp').reset_index(drop=True)
    df_curr.to_csv(os.path.join(fixed_output_folder, curr_name), index=False)
    df_prev['Timestamp'] = pd.to_datetime(df_prev['Timestamp'], utc=True, errors='coerce')
    df_prev = df_prev.sort_values('Timestamp').reset_index(drop=True)
    df_prev.to_csv(os.path.join(fixed_output_folder, prev_name), index=False)
    logger.info(f"append_extra_seconds: movidos {len(rows_to_move)} filas de {prev_name} -> {curr_name} en {fixed_output_folder}")

def last_file_trim_overflow(last_csv_path,logger):
    try:
        df = pd.read_csv(last_csv_path)
    except Exception as e:
        logger.error(f"last_file_trim_overflow: error leyendo {last_csv_path}: {e}")
        return pd.DataFrame()
    if df.empty or 'Timestamp' not in df.columns:
        return pd.DataFrame()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
    if df['Timestamp'].isna().all():
        return pd.DataFrame()
    first_valid_idx = df['Timestamp'].first_valid_index()
    if first_valid_idx is None:
        return pd.DataFrame()
    file_hour = df.at[first_valid_idx, 'Timestamp'].hour
    overflow_mask = df['Timestamp'].dt.hour != file_hour
    extra_rows = df[overflow_mask].copy()
    df_remain = df[~overflow_mask].copy()
    df_remain.to_csv(last_csv_path, index=False)
    logger.info(f"last_file_trim_overflow: {len(extra_rows)} filas overflow recortadas de {os.path.basename(last_csv_path)}")
    return extra_rows

def build_bucket_key_from_df_rows(rows_df):
    if rows_df.empty or 'Timestamp' not in rows_df.columns:
        return None, None
    rows_df['Timestamp'] = pd.to_datetime(rows_df['Timestamp'], utc=True, errors='coerce')
    first_valid_idx = rows_df['Timestamp'].first_valid_index()
    if first_valid_idx is None:
        return None, None
    ts0 = rows_df.at[first_valid_idx, 'Timestamp']
    bucket_day = ts0.strftime('%Y%m%d')
    bucket_hour = ts0.hour
    return f"{bucket_day}_{bucket_hour:02d}", ts0

def append_leftover_rows_to_next_bucket(leftover_df, next_fixed_folder_path,logger):
    os.makedirs(next_fixed_folder_path, exist_ok=True)
    files_next = sorted([f for f in os.listdir(next_fixed_folder_path) if f.lower().endswith('.csv')])
    if leftover_df.empty:
        logger.info(f"append_leftover_rows_to_next_bucket: no hay filas para {next_fixed_folder_path}")
        return
    next_hour_str = os.path.basename(next_fixed_folder_path).split('_')[-1]
    try:
        next_hour = int(next_hour_str)
    except ValueError:
        next_hour = None
    leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], utc=True, errors='coerce')
    if next_hour is not None:
        correct_rows = leftover_df[leftover_df['Timestamp'].dt.hour == next_hour].copy()
        if correct_rows.empty:
            logger.info(f"append_leftover_rows_to_next_bucket: ninguna fila pertenece a hora {next_hour} para {next_fixed_folder_path}; descartado")
            return
    else:
        correct_rows = leftover_df.copy()
    if not files_next:
        new_path = os.path.join(next_fixed_folder_path, f'generated_{correct_rows.iloc[0]["Timestamp"].strftime("%Y%m%d_%H%M%S")}.csv')
        correct_rows.to_csv(new_path, index=False)
        logger.info(f"append_leftover_rows_to_next_bucket: creado {new_path} con {len(correct_rows)} filas")
        return
    first_csv_path = os.path.join(next_fixed_folder_path, files_next[0])
    df_next = pd.read_csv(first_csv_path)
    df_next['Timestamp'] = pd.to_datetime(df_next['Timestamp'], utc=True, errors='coerce')
    merged = pd.concat([correct_rows, df_next], ignore_index=True)
    merged = merged.sort_values('Timestamp').reset_index(drop=True)
    merged.to_csv(first_csv_path, index=False)
    logger.info(f"append_leftover_rows_to_next_bucket: añadidos {len(correct_rows)} filas a {first_csv_path}")


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
        if "P5_TEST" in point:
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


            except Exception as e:
                logger.error(f"Error setting up folders: {e}")
                continue



            try:

                # ------------------------------------
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
            except Exception as e:
                logger.error(f"Error listing folder days: {e}")
                continue


            # --------------------------------------------------
            #   #TODO FIX OF THE EXTRA SECONDS IN MINUTE PROBLEM
            #   #HOW -> 
            #   EXAMPLE:
            #
            #   20250401_125933.csv in acoust_params is supposed:
            #                       to have the time range of [12:59:33 - 12:59:59]
            #                       but it has 33 seconds of the minute 13:00
            #   
            #   20250401_130034.csv in acoust_params is wanted:
            #                       to have the time range of [13:00:00 - 13:00:59]
            #                       but the minute starts with 34 seconds of delay
            #
            #   Solution -> take the last 34 rows of 
            #                   20250401_125933.csv
            #               append them to the start of
            #                   20250401_130034.csv
            #               change 20250401_130034.csv to
            #                   20250401_130000.csv
            #               
            #               apply this while
            #               iterating through consequent pairs
            #               
            # --------------------------------------------------

            """
            point: path base que contiene carpetas tipo 'acoustic_params' y 'predictions_litle'
            Recorre y crea carpetas fixed_YYYYMMDD_HH y ajusta segundos/minutos sobrantes.
            """
            for measurement_folder in sorted(os.listdir(point)):
                if measurement_folder not in ['acoustic_params', 'predictions_litle']:
                    continue
                measurement_path = os.path.join(point, measurement_folder)
                if not os.path.isdir(measurement_path):
                    continue

                leftover_buckets = {}  # reiniciar por measurement_folder para evitar mezcla entre acoustic/predictions

                # procesamos los buckets (p. ej. '20250401_12') en orden por nombre (esto está bien)
                for bucket in tqdm.tqdm(sorted(os.listdir(measurement_path)), desc=f'Fixing time slops {measurement_folder}'):
                    if 'fixed' in bucket or bucket.endswith('.txt'):
                        continue
                    bucket_path = os.path.join(measurement_path, bucket)
                    if not os.path.isdir(bucket_path):
                        continue

                    # recoger CSVs originales
                    csv_files = [f for f in os.listdir(bucket_path) if f.lower().endswith('.csv')]
                    if measurement_folder == 'predictions_litle':
                        csv_files = [f for f in csv_files if f.endswith('w_1.0.csv')]
                    csv_files = sorted(csv_files)
                    if not csv_files:
                        logger.info(f"No hay CSVs en {bucket_path}; siguiente")
                        continue

                    fixed_bucket_folder = os.path.join(measurement_path, f'fixed_{bucket}')
                    os.makedirs(fixed_bucket_folder, exist_ok=True)

                    # COPIAR todos los CSV a fixed_ (trabajamos sobre copia)
                    for fname in csv_files:
                        src = os.path.join(bucket_path, fname)
                        dst = os.path.join(fixed_bucket_folder, fname)
                        if os.path.exists(src) and not os.path.exists(dst):
                            shutil.copy(src, dst)

                    # ORDENAR los CSV de fixed_ por contenido
                    fixed_csvs_ordered = sort_csvs_by_content_timestamp(fixed_bucket_folder,logger)

                    # PROCESAR pares por contenido
                    for prev_name, curr_name in zip(fixed_csvs_ordered, fixed_csvs_ordered[1:]):
                        prev_path = os.path.join(fixed_bucket_folder, prev_name)
                        curr_path = os.path.join(fixed_bucket_folder, curr_name)

                        # minute jump detect
                        if detect_minute_jump_by_content(prev_path, curr_path,logger):
                            logger.info(f"Minute jump detected between {prev_name} and {curr_name} in {fixed_bucket_folder}")
                            try:
                                df_prev = pd.read_csv(prev_path)
                            except Exception as e:
                                logger.error(f"Error leyendo {prev_path}: {e}")
                                continue
                            if df_prev.empty or 'Timestamp' not in df_prev.columns:
                                continue
                            df_prev['Timestamp'] = pd.to_datetime(df_prev['Timestamp'], utc=True, errors='coerce')
                            if df_prev['Timestamp'].isna().all():
                                continue

                            first_valid_idx = df_prev['Timestamp'].first_valid_index()
                            if first_valid_idx is None:
                                continue
                            missing_minute = df_prev.at[first_valid_idx, 'Timestamp'].minute

                            # filas con ese minuto (posibles leftovers)
                            leftover_rows = df_prev[df_prev['Timestamp'].dt.minute == missing_minute].copy()
                            if leftover_rows.empty:
                                continue

                            leftover_sorted = leftover_rows.sort_values('Timestamp').reset_index(drop=True)
                            real_bucket_key, ref_ts = build_bucket_key_from_df_rows(leftover_sorted)
                            if real_bucket_key is None:
                                logger.warning("No se pudo determinar bucket real del leftover; se descarta")
                                continue

                            dict_key = (real_bucket_key, measurement_folder)
                            if dict_key in leftover_buckets:
                                leftover_buckets[dict_key] = pd.concat([leftover_buckets[dict_key], leftover_sorted], ignore_index=True).sort_values('Timestamp').reset_index(drop=True)
                            else:
                                leftover_buckets[dict_key] = leftover_sorted

                            logger.info(f"Added {len(leftover_sorted)} leftover rows to bucket {dict_key} from {prev_name}")

                        # mover segundos extra del prev al curr (si hay)
                        _, extra_idx = get_extra_seconds_indices(prev_path,logger)
                        if extra_idx:
                            append_extra_seconds(prev_path, curr_path, extra_idx, measurement_path, bucket,logger)

                    # Último archivo: recortar overflow de HORA siguiente
                    last_original_name = csv_files[-1]
                    last_fixed_path = os.path.join(fixed_bucket_folder, last_original_name)
                    if not os.path.exists(last_fixed_path):
                        original_candidate = os.path.join(bucket_path, last_original_name)
                        if os.path.exists(original_candidate):
                            shutil.copy(original_candidate, last_fixed_path)
                        else:
                            logger.warning(f"Último CSV {original_candidate} no encontrado; salto trim para {bucket}")
                            continue

                    extra_rows = last_file_trim_overflow(last_fixed_path,logger)
                    if not extra_rows.empty:
                        leftover_sorted = extra_rows.sort_values('Timestamp').reset_index(drop=True)
                        real_bucket_key, _ = build_bucket_key_from_df_rows(leftover_sorted)
                        if real_bucket_key is None:
                            logger.warning("No se pudo determinar bucket real del overflow; se descarta")
                        else:
                            dict_key = (real_bucket_key, measurement_folder)
                            if dict_key in leftover_buckets:
                                leftover_buckets[dict_key] = pd.concat([leftover_buckets[dict_key], leftover_sorted], ignore_index=True).sort_values('Timestamp').reset_index(drop=True)
                            else:
                                leftover_buckets[dict_key] = leftover_sorted
                            logger.info(f"Added {len(leftover_sorted)} overflow rows to bucket {dict_key} from last file {os.path.basename(last_fixed_path)}")

                # ---------------- DISTRIBUIR leftovers para este measurement_folder (UNA VEZ procesados todos los buckets) ----------------
                # Recorremos una copia de las claves
                for (bucket_day_hour, m_folder), leftover_df in list(leftover_buckets.items()):
                    # bucket siguiente respecto al bucket real del leftover
                    next_bucket = get_next_hour_bucket(bucket_day_hour)
                    if not next_bucket:
                        logger.info(f"No hay bucket siguiente para {bucket_day_hour}; posible fin de día.")
                        continue

                    next_fixed_folder = os.path.join(point, m_folder, f"fixed_{next_bucket}")

                    if os.path.exists(next_fixed_folder):
                        append_leftover_rows_to_next_bucket(leftover_df, next_fixed_folder,logger)
                    else:
                        os.makedirs(next_fixed_folder, exist_ok=True)
                        leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], utc=True, errors='coerce')
                        next_hour = int(next_bucket.split('_')[-1])
                        filtered = leftover_df[leftover_df['Timestamp'].dt.hour == next_hour].copy()
                        if filtered.empty:
                            logger.info(f"No se creará prepended para {next_fixed_folder}: no hay filas pertenecientes a la hora {next_hour}")
                            # si quieres forzar creación incluso si filtered.empty, cambia aquí
                            continue
                        filename = f"prepended_{filtered.iloc[0]['Timestamp'].strftime('%Y%m%d_%H%M%S')}.csv"
                        new_path = os.path.join(next_fixed_folder, filename)
                        filtered.to_csv(new_path, index=False)
                        logger.info(f"Creado {new_path} con {len(filtered)} filas sobrantes.")
                
                
                # -.--------------------
                # PROCESSING
                # ----------------------
                
            whole_start_time = time.time()
            
            for folder in os.listdir(point):
                
                if folder == 'acoustic_params'  and ACOUSTIC_QUERY_SWITCH:
                    
                    logger.info("Starting ACOUSTIC processing")
                    end_time = acoustic_processing(folder_days_acoustics_predictions,folder,db,logger, all_info, query_acoustic_folder, processed_wavs, processed_folder_acoustic_txt)
                    print(" --- %s seconds in execution ---" %end_time)                      
                
                if folder == 'predictions_litle' and PREDICT_QUERY_SWITCH:
                    
                    logger.info("Starting PREDICTIONS processing")
                    end_time = prediction_processing(folder_days_acoustics_predictions,folder,db,logger, all_info, query_pred_folder, processed_wavs, processed_folder_predictions_txt)
                    print(" --- %s seconds in execution ---" %end_time)  
                
                if folder == 'wav_files' and WAV_QUERY_SWITCH:
                    
                    logger.info("Starting WAV FILES processing")
                    end_time = 0 
                    #wav_processing(folder_days_wavs,folder,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)
                    print(" --- %s seconds in execution ---" %end_time)  
        
                if folder == 'sonometer_files' and SONOMETER_QUERY_SWITCH:  
                   
                    logger.info("Starting SONOMETER FOLDER processing")
                    end_time =0
                    #sonometer_processing(folder,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt)
                    print(" --- %s seconds in execution ---" %end_time)
            
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