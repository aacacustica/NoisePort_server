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

def extract_minute_from_filename(fname):
    """
    Extrae el minuto (dos dígitos) del nombre de fichero con patrón YYYYMMDD_HHMMSS.
    Devuelve string 'MM' o None si no puede extraerlo.
    """
    m = FILENAME_TS_RE.search(fname)
    if not m:
        return None
    ts_part = m.group(1)           # 'YYYYMMDD_HHMMSS'
    hhmmss = ts_part.split('_')[1] # 'HHMMSS'
    return hhmmss[2:4]             # 'MM'

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
    process_sonometer_folder(db,logger,sonometer_path,query_sonometer_folder,processed_folder_sonometer_txt)
    end_time =  round(time.time() - start_time,2)
    return end_time
    

def get_extra_seconds_row(csv_path,logger):
    """
    Devuelve (count_extra_seconds, indexs_extra_rows).
    Detecta filas cuyo minuto difiere del minuto representado en el nombre del fichero.
    """
    csv_name = os.path.basename(csv_path)
    csv_minute = extract_minute_from_filename(csv_name)
    if csv_minute is None:
        logger.warning(f"No se pudo extraer minuto del nombre {csv_name}. Se asumirá que no hay extras.")
        return 0, []

    df = pd.read_csv(csv_path)
    indexs_extra_rows = []
    for index, row in df.iterrows():
        try:
            minute = pd.to_datetime(row['Timestamp']).strftime('%M')
        except Exception as e:
            logger.error(f"Error parsing Timestamp en {csv_path} fila {index}: {e}")
            continue
        if str(minute) != csv_minute:
            indexs_extra_rows.append(index)

    return len(indexs_extra_rows), indexs_extra_rows



def append_extra_seconds(previous_csv_path, current_csv_path, row_indexs, days_folder, day):
    """
    Mueve filas de previous -> current (ya en /fixed_<day>/).
    days_folder: path hasta measurement_folder (ej: /.../acoustic_params)
    day: day_folder (ej: '20250401_12')
    """
    if not row_indexs:
        return

    output_path = os.path.join(days_folder, f'fixed_{day}')
    os.makedirs(output_path, exist_ok=True)

    name_previous = os.path.basename(previous_csv_path)
    name_current = os.path.basename(current_csv_path)

    df_previous = pd.read_csv(previous_csv_path)
    df_current = pd.read_csv(current_csv_path)

    # Extraer por posición (loc usa labels; aquí usamos iloc si row_indexs son posiciones)
    try:
        df_previous_rows_to_move = df_previous.loc[row_indexs].copy()
    except Exception:
        # fallback por posiciones
        df_previous_rows_to_move = df_previous.iloc[row_indexs].copy()

    # Eliminar esas filas del previo
    df_previous = df_previous.drop(index=row_indexs).reset_index(drop=True)

    # Adjuntar al current y ordenar
    df_current = pd.concat([df_current, df_previous_rows_to_move], ignore_index=True)
    df_current['Timestamp'] = pd.to_datetime(df_current['Timestamp'], utc=True)
    df_current = df_current.sort_values('Timestamp').reset_index(drop=True)
    df_current.to_csv(os.path.join(output_path, name_current), index=False)

    # Guardar previo recortado
    df_previous['Timestamp'] = pd.to_datetime(df_previous['Timestamp'], utc=True)
    df_previous = df_previous.sort_values('Timestamp').reset_index(drop=True)
    df_previous.to_csv(os.path.join(output_path, name_previous), index=False)
    

    

def last_day_round_with_extra(last_hour_path,logger):
    """
    Corta el CSV en la primera fila cuyo minuto no coincida con el minuto del nombre.
    Sobrantes se devuelven como DataFrame (puede estar vacío).
    """
    csv_name = os.path.basename(last_hour_path)
    csv_minute = extract_minute_from_filename(csv_name)
    if csv_minute is None:
        return pd.DataFrame()

    df = pd.read_csv(last_hour_path)
    extra_rows = pd.DataFrame()

    for index, row in df.iterrows():
        try:
            minute = pd.to_datetime(row['Timestamp']).strftime('%M')
        except Exception as e:
            logger.error(f"Error parsing Timestamp en {last_hour_path} fila {index}: {e}")
            continue
        if str(minute) != csv_minute:
            extra_rows = df.loc[index:].copy()
            df = df.loc[:index-1].copy() if index > 0 else df.iloc[0:0].copy()
            break

    # Guardar el fichero recortado (in-place)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True)
    df = df.sort_values('Timestamp').reset_index(drop=True)
    df.to_csv(last_hour_path, index=False)

    if not extra_rows.empty:
        extra_rows['Timestamp'] = pd.to_datetime(extra_rows['Timestamp'], utc=True)
        extra_rows = extra_rows.sort_values('Timestamp').reset_index(drop=True)

    return extra_rows

def get_next_day_hour_folder(curr_folder):
    """
    Dado 'YYYYMMDD_HH' devuelve siguiente hora 'YYYYMMDD_HH+1' o None si cambia de día.
    (Se puede extender para soportar cambio de día si lo necesitas).
    """
    try:
        day, hour = curr_folder.split('_')
        hour = int(hour)
    except Exception:
        return None
    next_hour = hour + 1
    if next_hour < 24:
        return f"{day}_{next_hour:02d}"
    return None

def append_leftover_rows(leftover_df, next_fixed_folder_path, logger):
    """
    Prepend leftover_df al primer CSV de next_fixed_folder_path (ordenado por nombre).
    Si no existe ningún CSV, crea uno nuevo con leftover_df.
    """
    os.makedirs(next_fixed_folder_path, exist_ok=True)
    files_next = sorted([f for f in os.listdir(next_fixed_folder_path) if f.lower().endswith('.csv')])
    if not files_next:
        # Crear un CSV nuevo si no hay ninguno
        new_path = os.path.join(next_fixed_folder_path, f'generated_{leftover_df.iloc[0]["Timestamp"].strftime("%Y%m%d_%H%M%S")}.csv')
        leftover_df.to_csv(new_path, index=False)
        logger.info(f"No había CSV en {next_fixed_folder_path}; creado {new_path} con {len(leftover_df)} filas sobrantes.")
        return

    first_csv = files_next[0]
    first_csv_path = os.path.join(next_fixed_folder_path, first_csv)
    df_next = pd.read_csv(first_csv_path)

    leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], utc=True)
    df_next['Timestamp'] = pd.to_datetime(df_next['Timestamp'], utc=True)

    df_merged = pd.concat([leftover_df, df_next], ignore_index=True)
    df_merged = df_merged.sort_values('Timestamp').reset_index(drop=True)
    df_merged.to_csv(first_csv_path, index=False)
    logger.info(f"Sobrantes añadidos a {first_csv_path}: {len(leftover_df)} filas")



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

            leftover_seconds = {}

            for measurement_folder in tqdm.tqdm(sorted(os.listdir(point)),desc=f'Fixing time slops {measurement_folder}',unit='measurement_folder'):
                if measurement_folder not in ['acoustic_params', 'predictions_litle']:
                    continue

                measurement_path = os.path.join(point, measurement_folder)
                if not os.path.isdir(measurement_path):
                    continue

                # procesamos los day_folders en orden cronológico
                for day_folder in sorted(os.listdir(measurement_path)):
                    if 'fixed' in day_folder or day_folder.endswith('.txt'):
                        continue

                    day_path = os.path.join(measurement_path, day_folder)
                    if not os.path.isdir(day_path):
                        continue

                    files_day = [f for f in os.listdir(day_path) if f.lower().endswith('.csv')]
                    if measurement_folder == 'predictions_litle':
                        files_day = [f for f in files_day if f.endswith('w_1.0.csv')]

                    files_day = sorted(files_day)
                    if not files_day:
                        logger.info(f"No hay CSVs en {day_path}; siguiente")
                        continue

                    fixed_folder_path = os.path.join(measurement_path, f'fixed_{day_folder}')
                    os.makedirs(fixed_folder_path, exist_ok=True)

                    # Procesar pares consecutivos
                    for previous, current in zip(files_day, files_day[1:]):
                        prev_orig = os.path.join(day_path, previous)
                        curr_orig = os.path.join(day_path, current)

                        prev_dest = os.path.join(fixed_folder_path, previous)
                        curr_dest = os.path.join(fixed_folder_path, current)

                        # Copiar si no existen aún en fixed_
                        if not os.path.exists(prev_dest):
                            shutil.copy(prev_orig, prev_dest)
                        if not os.path.exists(curr_dest):
                            shutil.copy(curr_orig, curr_dest)

                        # Detectar y mover segundos extra entre prev_dest -> curr_dest
                        _, index_rows = get_extra_seconds_row(prev_dest,logger)
                        if index_rows:
                            append_extra_seconds(prev_dest, curr_dest, index_rows, measurement_path, day_folder)

                    # Último archivo de la carpeta: recortarlo y guardar sobrantes
                    last_csv_name = files_day[-1]
                    last_csv_path = os.path.join(fixed_folder_path, last_csv_name)
                    # Si el archivo no existe todavía en fixed (por ejemplo carpeta creada y no copiados), copiarlo
                    if not os.path.exists(last_csv_path):
                        # Copiamos desde la carpeta original si existe
                        orig_last = os.path.join(day_path, last_csv_name)
                        if os.path.exists(orig_last):
                            shutil.copy(orig_last, last_csv_path)
                        else:
                            logger.warning(f"Último CSV {orig_last} no encontrado; saltando last_day_round para {day_folder}")
                            continue

                    extra_rows = last_day_round_with_extra(last_csv_path,logger)
                    if not extra_rows.empty:
                        key = (day_folder, measurement_folder)
                        if key in leftover_seconds:
                            # concatenar si ya hay sobrantes guardados
                            leftover_seconds[key] = pd.concat([leftover_seconds[key], extra_rows], ignore_index=True).sort_values('Timestamp').reset_index(drop=True)
                        else:
                            leftover_seconds[key] = extra_rows

            # ------------------ PASO FINAL: SEGUNDOS SOBRANTES A LA SIGUIENTE HORA ------------------

            for (day_folder, measurement_folder), extra_df in leftover_seconds.items():
                next_folder = get_next_day_hour_folder(day_folder)
                if not next_folder:
                    logger.info(f"No hay carpeta siguiente para {day_folder}. Posible fin de día.")
                    continue

                next_fixed_path = os.path.join(point, measurement_folder, f'fixed_{next_folder}')
                if os.path.exists(next_fixed_path):
                    append_leftover_rows(extra_df, next_fixed_path, logger)
                else:
                    # Si la carpeta siguiente aún no existe, la creamos y escribimos un CSV con los sobrantes
                    os.makedirs(next_fixed_path, exist_ok=True)
                    # Generar un nombre seguro para el CSV
                    timestamp_tag = extra_df.iloc[0]['Timestamp'].strftime('%Y%m%d_%H%M%S')
                    new_fname = f'prepended_{timestamp_tag}.csv'
                    new_path = os.path.join(next_fixed_path, new_fname)
                    extra_df.to_csv(new_path, index=False)
                    logger.info(f"Carpeta {next_fixed_path} no existía; creado {new_path} con {len(extra_df)} filas sobrantes.")
                
                
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
                    end_time = wav_processing(folder_days_wavs,folder,db,logger, all_info, query_wav_folder, processed_wavs, processed_folder_wav_txt)
                    print(" --- %s seconds in execution ---" %end_time)  
        
                if folder == 'sonometer_files' and SONOMETER_QUERY_SWITCH:  
                   
                    logger.info("Starting SONOMETER FOLDER processing")
                    end_time = sonometer_processing(folder,point,db,logger,query_sonometer_folder,processed_folder_sonometer_txt)
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