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

PATH = SANDISK_PATH_LINUX
ISDIR = os.path.isdir(PATH)

ID_MICRO, LOCATION_RECORD, LOCATION_PLACE, LOCATION_POINT, \
AUDIO_SAMPLE_RATE, AUDIO_WINDOW_SIZE, AUDIO_CALIBRATION_CONSTANT,\
STORAGE_S3_BUCKET_NAME, STORAGE_OUTPUT_WAV_FOLDER, \
STORAGE_OUTPUT_ACOUSTIC_FOLDER = load_config_acoustic('config.yaml')

FILENAME_TS_RE = re.compile(r'(\d{8}_\d{6})')  # extrae 'YYYYMMDD_HHMMSS'
logger = setup_logging('query_automatize')


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
        record_id,
        sensor_id,
        MIN(Unixtimestamp)                  AS unixtimestamp,
        10 * LOG10 ( AVG(POWER(10,LAeq/10))) AS AVG_LAeq,
        MAX(LAmax)                          AS max_LAmax,
        MIN(LAmin)                          AS min_LAmin
        FROM {table_name}
        GROUP BY record_id,sensor_id,Timestamp
        """
    else:
            query = f"""
        SELECT
        sensor_id,
        MIN(Unixtimestamp)                  AS unixtimestamp,
        10 * LOG10 (AVG(POWER(10, LA/10)))   AS AVG_LAeq,
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




def send_mqtt_data(data, logger, sent_Records_txt):

    # Asegurarse de que el archivo exista
    if not os.path.exists(sent_Records_txt):
        open(sent_Records_txt, 'w').close()

    # Leer los record_id ya enviados
    with open(sent_Records_txt) as f:
        sent_ids = set(f.read().splitlines())

    # Crear cliente MQTT
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except:
        client = mqtt.Client()

    # Conexión al broker
    if DEMO:
        port = int(MQTT_PORT_DEMO)
        client.connect(MQTT_BROKER_DEMO, port, keepalive=60)
        logger.info("Connected to MQTT broker DEMO at %s:%s", MQTT_BROKER_DEMO, port)
    else:
        port = int(MQTT_PORT_MUUTECH)
        client.username_pw_set(MQTT_USER_MUUTECH, MQTT_PASSWORD_MUUTECH)
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
        client.connect(MQTT_BROKER_MUUTECH, port, keepalive=60)
        logger.info("Connected to MQTT broker MUUTECH at %s:%s", MQTT_BROKER_MUUTECH, port)

    # Iniciar loop en background
    client.loop_start()

    for record in data:
        record_id = str(record.get('record_id', ''))
        if not record_id or record_id in sent_ids:
            continue

        sensor_id = record.get("sensor_id", "unknown")
        topic = f"aacacustica/{sensor_id}"
        payload = json.dumps(record, default=str)

        if sensor_id in ['0005884', '0005886']:
            print(f"Topic: {topic}")
            print(f"Sensor_id: {sensor_id}")
            print(f"Payload: {payload}")

        try:
            result = client.publish(topic, payload, qos=1, retain=True)
            result.wait_for_publish()  #Ahora puede recibir ACK
            logger.info("Published record %s to topic '%s'", record_id, topic)
            update_processed_folder(sent_Records_txt, record_id)
            sent_ids.add(record_id)
        except Exception as e:
            logger.error("Error publishing record %s: %s", record_id, e)

    # Detener loop y desconectar
    client.loop_stop()
    client.disconnect()
    logger.info("MQTT client disconnected")

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
    process_sonometer_folder(db,logger,sonometer_path,query_sonometer_folder,processed_folder_sonometer_txt)
    end_time =  round(time.time() - start_time,2)
    return end_time

def safe_read_timestamp_series(csv_path, nrows=10):
    """Read up to nrows timestamps from csv_path and return pd.Series."""
    try:
        df = pd.read_csv(csv_path, nrows=nrows)
    except Exception as e:
        logger.debug(f"safe_read_timestamp_series: error reading {csv_path}: {e}")
        return None
    if df.empty or 'Timestamp' not in df.columns:
        logger.debug(f"safe_read_timestamp_series: {csv_path} empty or missing 'Timestamp'.")
        return None
    ts = pd.to_datetime(df['Timestamp'], errors='coerce')
    if ts.isna().all():
        logger.debug(f"safe_read_timestamp_series: {csv_path} all Timestamps NaT after parsing.")
        return None
    return ts

def get_csv_first_valid_timestamp(csv_path):
    ts_series = safe_read_timestamp_series(csv_path, nrows=10)
    if ts_series is None:
        return None
    return ts_series.dropna().iloc[0]

def get_csv_last_valid_timestamp(csv_path):
    try:
        df = pd.read_csv(csv_path, usecols=['Timestamp'])
    except Exception as e:
        logger.debug(f"get_csv_last_valid_timestamp: error reading {csv_path}: {e}")
        return None
    if df.empty or 'Timestamp' not in df.columns:
        return None
    ts = pd.to_datetime(df['Timestamp'], errors='coerce').dropna()
    if ts.empty:
        return None
    return ts.iloc[-1]

def sort_csvs_by_content_timestamp(folder):
    csvs = [f for f in os.listdir(folder) if f.lower().endswith('.csv')]
    def _key(fname):
        ts = get_csv_first_valid_timestamp(os.path.join(folder, fname))
        return ts.timestamp() if ts is not None else float("inf")
    return sorted(csvs, key=_key)

def detect_minute_jump_by_content(prev_path, curr_path, threshold_seconds=70):
    tprev = get_csv_last_valid_timestamp(prev_path)
    tcurr = get_csv_first_valid_timestamp(curr_path)
    if tprev is None or tcurr is None:
        return False
    return (tcurr - tprev).total_seconds() > threshold_seconds

def get_extra_seconds_indices(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"get_extra_seconds_indices: error reading {csv_path}: {e}")
        return 0, []

    if df.empty or 'Timestamp' not in df.columns:
        return 0, []

    # Parsear a datetime (NaT donde no sea posible)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # Eliminar timezone sin cambiar la hora
    df['Timestamp'] = df['Timestamp'].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)

    # Evitar que un NaT rompa .dt
    timestamps_valid = df['Timestamp'].dropna()
    if timestamps_valid.empty:
        return 0, []

    official_minute = timestamps_valid.iloc[0].minute
    idxs = df[timestamps_valid.dt.minute != official_minute].index.tolist()
    return len(idxs), idxs

def append_extra_seconds(fixed_folder_path, prev_name, curr_name, row_indices):
    if not row_indices:
        return
    prev_path = os.path.join(fixed_folder_path, prev_name)
    curr_path = os.path.join(fixed_folder_path, curr_name)
    df_prev = pd.read_csv(prev_path)
    df_curr = pd.read_csv(curr_path)
    rows_to_move = df_prev.loc[row_indices].copy()
    df_prev = df_prev.drop(index=row_indices).reset_index(drop=True)
    df_curr = pd.concat([df_curr, rows_to_move], ignore_index=True).sort_values('Timestamp')
    df_prev.to_csv(prev_path, index=False)
    df_curr.to_csv(curr_path, index=False)
    logger.info(f"append_extra_seconds: moved {len(rows_to_move)} rows from {prev_name} -> {curr_name}")

def last_file_trim_overflow(last_csv_path):
    try:
        df = pd.read_csv(last_csv_path)
    except Exception as e:
        logger.error(f"last_file_trim_overflow: error reading {last_csv_path}: {e}")
        return pd.DataFrame()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    first_hour = df['Timestamp'].iloc[0].hour
    overflow_mask = df['Timestamp'].dt.hour != first_hour
    extra_rows = df[overflow_mask].copy()
    df[~overflow_mask].to_csv(last_csv_path, index=False)
    return extra_rows

def build_bucket_key_from_df_rows(rows_df):
    if rows_df.empty or 'Timestamp' not in rows_df.columns:
        return None, None
    ts0 = rows_df['Timestamp'].dropna().iloc[0]
    bucket_day = ts0.strftime('%Y%m%d')
    bucket_hour = ts0.hour
    return f"{bucket_day}_{bucket_hour:02d}", ts0

def append_leftover_rows_to_next_bucket(leftover_df, next_fixed_folder_path):
    """
    Prepend leftover_df to first CSV in next_fixed_folder_path, filtered by next hour.
    """
    if leftover_df.empty:
        return
    os.makedirs(next_fixed_folder_path, exist_ok=True)
    next_hour = int(os.path.basename(next_fixed_folder_path).split('_')[-1])
    
    # Convert leftover timestamps
    leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], errors='coerce')
    rows_for_hour = leftover_df[leftover_df['Timestamp'].dt.hour == next_hour].copy()
    if rows_for_hour.empty:
        return

    csvs = sort_csvs_by_content_timestamp(next_fixed_folder_path)
    if not csvs:
        fname = f"generated_{rows_for_hour.iloc[0]['Timestamp'].strftime('%Y%m%d_%H%M%S')}_tflt_w_1.0.csv"
        rows_for_hour.to_csv(os.path.join(next_fixed_folder_path, fname), index=False)
        logger.info(f"append_leftover_rows_to_next_bucket: created {fname} with {len(rows_for_hour)} rows")
        return

    first_csv_path = os.path.join(next_fixed_folder_path, csvs[0])
    df_next = pd.read_csv(first_csv_path)

    # Ensure Timestamp is converted in df_next too!
    df_next['Timestamp'] = pd.to_datetime(df_next['Timestamp'], errors='coerce')
    rows_for_hour['Timestamp'] = pd.to_datetime(rows_for_hour['Timestamp'], errors='coerce')
    
    merged = pd.concat([rows_for_hour, df_next], ignore_index=True).sort_values('Timestamp').reset_index(drop=True)
    merged.to_csv(first_csv_path, index=False)
    logger.info(f"append_leftover_rows_to_next_bucket: prepended {len(rows_for_hour)} rows to {first_csv_path}")

def get_next_hour_bucket(bucket):
    day, hour = bucket.split('_')
    hour = int(hour)
    if hour < 23:
        return f"{day}_{hour+1:02d}"
    # rollover
    next_day = (pd.to_datetime(day) + pd.Timedelta(days=1)).strftime('%Y%m%d')
    return f"{next_day}_00"

def get_last_minute_leftovers(df):
    if df.empty or 'Timestamp' not in df.columns:
        return pd.DataFrame()

    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # si todo es NaT → no se puede sacar el último minuto
    if df['Timestamp'].dropna().empty:
        return pd.DataFrame()

    last_minute = df['Timestamp'].dropna().iloc[-1].minute
    return df[df['Timestamp'].dt.minute == last_minute].copy()

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

# ------------------- Main loop con nuevos leftovers -------------------
            measurement_folders = sorted([d for d in os.listdir(point) if os.path.isdir(os.path.join(point, d))])
            measurement_folders = [m for m in measurement_folders if m in ('acoustic_params', 'predictions_litle')]

            for measurement_folder in measurement_folders:
                measurement_path = os.path.join(point, measurement_folder)
                leftover_buckets = {}  # <-- corrected: placed outside bucket loop

                buckets = sorted([b for b in os.listdir(measurement_path) if os.path.isdir(os.path.join(measurement_path, b))])
                for bucket in tqdm.tqdm(buckets, desc=f'Fixing time slops {measurement_folder}'):
                    if 'fixed' in bucket or bucket.endswith('.txt'):
                        continue
                    bucket_path = os.path.join(measurement_path, bucket)
                    csv_files = [f for f in os.listdir(bucket_path) if f.lower().endswith('.csv')]
                    if measurement_folder == 'predictions_litle':
                        csv_files = [f for f in csv_files if f.endswith('w_1.0.csv')]
                    if not csv_files:
                        continue

                    fixed_folder = os.path.join(measurement_path, f'fixed_{bucket}')
                    os.makedirs(fixed_folder, exist_ok=True)

                    for fname in csv_files:
                        dst = os.path.join(fixed_folder, fname)
                        if not os.path.exists(dst):
                            shutil.copy(os.path.join(bucket_path, fname), dst)

                    fixed_csvs = sort_csvs_by_content_timestamp(fixed_folder)
                    for prev_name, curr_name in zip(fixed_csvs, fixed_csvs[1:]):
                        prev_path = os.path.join(fixed_folder, prev_name)
                        curr_path = os.path.join(fixed_folder, curr_name)
                        if detect_minute_jump_by_content(prev_path, curr_path):
                            df_prev = pd.read_csv(prev_path)
                            df_prev['Timestamp'] = pd.to_datetime(df_prev['Timestamp'])
                            prev_hour = df_prev['Timestamp'].iloc[0].hour
                            leftover_rows = df_prev[df_prev['Timestamp'].dt.hour != prev_hour]
                            if not leftover_rows.empty:
                                df_prev.drop(index=leftover_rows.index).to_csv(prev_path, index=False)
                                bucket_key, _ = build_bucket_key_from_df_rows(leftover_rows)
                                leftover_buckets.setdefault((bucket_key, measurement_folder), pd.DataFrame())
                                leftover_buckets[(bucket_key, measurement_folder)] = pd.concat(
                                    [leftover_buckets[(bucket_key, measurement_folder)], leftover_rows],
                                    ignore_index=True
                                ).sort_values('Timestamp')
                                
                        _, extra_idx = get_extra_seconds_indices(prev_path)
                        if extra_idx:
                            append_extra_seconds(fixed_folder, prev_name, curr_name, extra_idx)

                    fixed_csvs = sort_csvs_by_content_timestamp(fixed_folder)
                    if not fixed_csvs:
                        continue
                    last_name = fixed_csvs[-1]
                    last_path = os.path.join(fixed_folder, last_name)

                    df_last = pd.read_csv(last_path)
                    df_last['Timestamp'] = pd.to_datetime(df_last['Timestamp'], errors='coerce')
                    minute_leftovers = get_last_minute_leftovers(df_last)
                    if not minute_leftovers.empty:
                        df_last.drop(index=minute_leftovers.index).to_csv(last_path, index=False)
                        next_bucket = get_next_hour_bucket(bucket)
                        if next_bucket:
                            leftover_buckets.setdefault((next_bucket, measurement_folder), pd.DataFrame())
                            leftover_buckets[(next_bucket, measurement_folder)] = pd.concat(
                                [leftover_buckets[(next_bucket, measurement_folder)], minute_leftovers],
                                ignore_index=True
                            ).sort_values('Timestamp')

                # >> Finalmente, distribuir leftovers acumulados
                for (bucket_key, m_folder), leftover_df in leftover_buckets.items():
                    next_folder = os.path.join(point, m_folder, f"fixed_{bucket_key}")
                    os.makedirs(next_folder, exist_ok=True)
                    append_leftover_rows_to_next_bucket(leftover_df, next_folder)
            
            
                # -.--------------------
                # PROCESSING
                # ----------------------
                
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