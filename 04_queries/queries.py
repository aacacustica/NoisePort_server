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

def safe_read_timestamp(csv_path, nrows=1):
    """
    Lee las primeras nrows filas y devuelve una Serie de Timestamps (datetime64[ns, UTC]).
    Si el CSV está vacío o no tiene columna 'Timestamp', devuelve None.
    """
    try:
        df = pd.read_csv(csv_path, nrows=nrows)
    except Exception as e:
        logger.error(f"Error leyendo {csv_path}: {e}")
        return None

    if df.empty or 'Timestamp' not in df.columns:
        logger.warning(f"{csv_path} vacío o sin columna 'Timestamp'.")
        return None

    try:
        ts = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
        if ts.isna().all():
            logger.warning(f"{csv_path} contiene Timestamps no parseables.")
            return None
        return ts
    except Exception as e:
        logger.error(f"Error parseando Timestamp en {csv_path}: {e}")
        return None


def get_csv_reference_time(csv_path):
    """
    Devuelve el timestamp (datetime) del primer registro válido del CSV.
    Si falla devuelve None.
    """
    ts = safe_read_timestamp(csv_path, nrows=10)
    if ts is None:
        return None
    # encontrar primer no-nulo
    first_valid = ts.dropna().iloc[0]
    return pd.to_datetime(first_valid, utc=True)


def detect_minute_jump(prev_path, curr_path, threshold_seconds=70):
    """
    Detecta si hay un salto entre el primer timestamp de prev_path y el primer timestamp de curr_path.
    threshold_seconds por defecto 70 (permite tolerancia a pequeños desfases).
    prev_path y curr_path deberían ser rutas completas a archivos.
    """
    tprev = get_csv_reference_time(prev_path)
    tcurr = get_csv_reference_time(curr_path)
    if tprev is None or tcurr is None:
        return False
    diff = (tcurr - tprev).total_seconds()
    return diff > threshold_seconds


def get_extra_seconds_row(csv_path, logger):
    """
    Devuelve (count_extra_seconds, indexs_extra_rows).
    Ahora determina el 'minuto del archivo' a partir del PRIMER timestamp del archivo (contenido).
    Marca como 'extra' las filas cuyo .dt.minute difiera del minuto real (del primer registro).
    """
    csv_name = os.path.basename(csv_path)
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Error leyendo {csv_path}: {e}")
        return 0, []

    if df.empty or 'Timestamp' not in df.columns:
        return 0, []

    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
    if df['Timestamp'].isna().all():
        return 0, []

    # minuto oficial del fichero (primer timestamp válido)
    first_valid_idx = df['Timestamp'].first_valid_index()
    csv_minute = df.at[first_valid_idx, 'Timestamp'].minute

    # indices donde el minuto difiere
    indexs_extra_rows = df[df['Timestamp'].dt.minute != csv_minute].index.tolist()
    return len(indexs_extra_rows), indexs_extra_rows


def append_extra_seconds(previous_csv_path, current_csv_path, row_indexs, days_folder, day):
    """
    Mueve filas de previous -> current (ya en /fixed_<day>/).
    days_folder: path hasta measurement_folder (ej: /.../acoustic_params)
    day: day_folder (ej: '20250401_12')
    Ambos previous_csv_path y current_csv_path deben existir en el disco (copiados a fixed_).
    """
    if not row_indexs:
        return

    output_path = os.path.join(days_folder, f'fixed_{day}')
    os.makedirs(output_path, exist_ok=True)

    name_previous = os.path.basename(previous_csv_path)
    name_current = os.path.basename(current_csv_path)

    df_previous = pd.read_csv(previous_csv_path)
    df_current = pd.read_csv(current_csv_path)

    # Extraer por posiciones/labels
    try:
        df_previous_rows_to_move = df_previous.loc[row_indexs].copy()
    except Exception:
        df_previous_rows_to_move = df_previous.iloc[row_indexs].copy()

    # Eliminar esas filas del previo
    df_previous = df_previous.drop(index=row_indexs).reset_index(drop=True)

    # Adjuntar al current y ordenar
    df_current = pd.concat([df_current, df_previous_rows_to_move], ignore_index=True)
    df_current['Timestamp'] = pd.to_datetime(df_current['Timestamp'], utc=True, errors='coerce')
    df_current = df_current.sort_values('Timestamp').reset_index(drop=True)
    df_current.to_csv(os.path.join(output_path, name_current), index=False)

    # Guardar previo recortado
    df_previous['Timestamp'] = pd.to_datetime(df_previous['Timestamp'], utc=True, errors='coerce')
    df_previous = df_previous.sort_values('Timestamp').reset_index(drop=True)
    df_previous.to_csv(os.path.join(output_path, name_previous), index=False)


def last_day_round_with_extra(last_hour_path, logger):
    """
    Recorta el archivo CSV en la primera fila que pertenece a la HORA SIGUIENTE a la hora que
    indica el PRIMER timestamp del fichero (no usa el nombre del fichero).
    Devuelve las filas que pertenecen a la hora siguiente (extra_rows). Actualiza el fichero original.
    """
    try:
        df = pd.read_csv(last_hour_path)
    except Exception as e:
        logger.error(f"Error leyendo {last_hour_path}: {e}")
        return pd.DataFrame()

    if df.empty or 'Timestamp' not in df.columns:
        return pd.DataFrame()

    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True, errors='coerce')
    if df['Timestamp'].isna().all():
        return pd.DataFrame()

    # hora oficial del fichero tomada del primer timestamp válido
    first_valid_idx = df['Timestamp'].first_valid_index()
    file_hour = df.at[first_valid_idx, 'Timestamp'].hour

    # Filas que están en una hora distinta a la del primer timestamp (overflow)
    overflow_mask = df['Timestamp'].dt.hour != file_hour
    extra_rows = df[overflow_mask].copy()
    df_remaining = df[~overflow_mask].copy()

    # Guardamos el recorte actualizado (sobreescribimos el fichero original)
    df_remaining.to_csv(last_hour_path, index=False)
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
    Prepend leftover_df al primer CSV de next_fixed_folder_path (ordenado por nombre),
    solo si las filas pertenecen a la hora correspondiente al folder siguiente.
    Si no existe ningún CSV, crea uno nuevo con leftover_df filtrado.
    """
    os.makedirs(next_fixed_folder_path, exist_ok=True)
    files_next = sorted([f for f in os.listdir(next_fixed_folder_path) if f.lower().endswith('.csv')])

    if leftover_df.empty:
        logger.info(f"No hay filas sobrantes para añadir a {next_fixed_folder_path}")
        return

    # Determinar la hora del folder siguiente; el folder debe tener formato fixed_YYYYMMDD_HH
    next_hour_str = os.path.basename(next_fixed_folder_path).split('_')[-1]
    try:
        next_hour = int(next_hour_str)
    except ValueError:
        logger.warning(f"No se pudo determinar la hora del folder {next_fixed_folder_path}; se añaden todas las filas")
        next_hour = None

    leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], utc=True, errors='coerce')
    if next_hour is not None:
        correct_rows = leftover_df[leftover_df['Timestamp'].dt.hour == next_hour].copy()
        if correct_rows.empty:
            logger.info(f"Ninguna fila sobrante pertenece a la hora {next_hour} en {next_fixed_folder_path}; se descartan.")
            return
    else:
        correct_rows = leftover_df.copy()

    if not files_next:
        # Crear un CSV nuevo si no hay ninguno
        new_path = os.path.join(
            next_fixed_folder_path,
            f'generated_{correct_rows.iloc[0]["Timestamp"].strftime("%Y%m%d_%H%M%S")}.csv'
        )
        correct_rows.to_csv(new_path, index=False)
        logger.info(f"No había CSV en {next_fixed_folder_path}; creado {new_path} con {len(correct_rows)} filas sobrantes.")
        return

    # Prepend al primer CSV existente
    first_csv = files_next[0]
    first_csv_path = os.path.join(next_fixed_folder_path, first_csv)
    df_next = pd.read_csv(first_csv_path)
    df_next['Timestamp'] = pd.to_datetime(df_next['Timestamp'], utc=True, errors='coerce')

    df_merged = pd.concat([correct_rows, df_next], ignore_index=True)
    df_merged = df_merged.sort_values('Timestamp').reset_index(drop=True)
    df_merged.to_csv(first_csv_path, index=False)
    logger.info(f"Sobrantes añadidos a {first_csv_path}: {len(correct_rows)} filas filtradas por hora {next_hour}")


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
            leftover_seconds = {}

            for measurement_folder in sorted(os.listdir(point)):
                if measurement_folder not in ['acoustic_params', 'predictions_litle']:
                    continue

                measurement_path = os.path.join(point, measurement_folder)
                if not os.path.isdir(measurement_path):
                    continue

                # procesamos los day_folders en orden cronológico
                for day_folder in tqdm.tqdm(sorted(os.listdir(measurement_path)), desc=f'Fixing time slops {measurement_folder}'):
                    if 'fixed' in day_folder or day_folder.endswith('.txt'):
                        continue

                    day_path = os.path.join(measurement_path, day_folder)
                    if not os.path.isdir(day_path):
                        continue

                    files_day = [f for f in os.listdir(day_path) if f.lower().endswith('.csv')]
                    if measurement_folder == 'predictions_litle':
                        files_day = [f for f in files_day if f.endswith('w_1.0.csv')]
                    else:
                        files_day = [f for f in os.listdir(day_path) if f.lower().endswith('.csv')]
                    files_day = sorted(files_day)
                    if not files_day:
                        logger.info(f"No hay CSVs en {day_path}; siguiente")
                        continue

                    fixed_folder_path = os.path.join(measurement_path, f'fixed_{day_folder}')
                    os.makedirs(fixed_folder_path, exist_ok=True)

                    # Copia inicial de todos (si quieres copiar sólo según el flujo original, puedes cambiar)
                    for f in files_day:
                        src = os.path.join(day_path, f)
                        dest = os.path.join(fixed_folder_path, f)
                        if not os.path.exists(dest) and os.path.exists(src):
                            shutil.copy(src, dest)

                    # Procesar pares consecutivos usando rutas completas en fixed_
                    fixed_files_day = sorted([f for f in os.listdir(fixed_folder_path) if f.lower().endswith('.csv')])

                    for previous, current in zip(fixed_files_day, fixed_files_day[1:]):
                        prev_dest = os.path.join(fixed_folder_path, previous)
                        curr_dest = os.path.join(fixed_folder_path, current)

                        # Detectar salto real por contenido
                        if detect_minute_jump(prev_dest, curr_dest):
                            logger.warning(f"Detected minute jump between {previous} and {current}")
                            # obtener leftover del previo
                            try:
                                df_prev = pd.read_csv(prev_dest)
                            except Exception as e:
                                logger.error(f"Error leyendo {prev_dest}: {e}")
                                continue
                            if 'Timestamp' not in df_prev.columns or df_prev.empty:
                                continue
                            df_prev['Timestamp'] = pd.to_datetime(df_prev['Timestamp'], utc=True, errors='coerce')
                            if df_prev['Timestamp'].isna().all():
                                continue

                            # minuto faltante = minuto del primer registro del previo
                            first_valid_idx = df_prev['Timestamp'].first_valid_index()
                            missing_minute = df_prev.at[first_valid_idx, 'Timestamp'].minute

                            # Filtrar filas del minuto faltante (del archivo previo)
                            leftover_rows = df_prev[df_prev['Timestamp'].dt.minute == missing_minute]

                            if not leftover_rows.empty:
                                key = (day_folder, measurement_folder)
                                leftover_sorted = leftover_rows.sort_values('Timestamp').reset_index(drop=True)

                                # Acumular como leftover
                                if key in leftover_seconds:
                                    leftover_seconds[key] = pd.concat(
                                        [leftover_seconds[key], leftover_sorted],
                                        ignore_index=True
                                    ).sort_values('Timestamp').reset_index(drop=True)
                                else:
                                    leftover_seconds[key] = leftover_sorted

                                logger.info(f"Added {len(leftover_sorted)} leftover rows from {previous} (minute jump detected)")

                        # Detectar y mover segundos extra entre prev_dest -> curr_dest
                        _, index_rows = get_extra_seconds_row(prev_dest, logger)
                        if index_rows:
                            append_extra_seconds(prev_dest, curr_dest, index_rows, measurement_path, day_folder)

                    # Último archivo de la carpeta: recortarlo y guardar sobrantes
                    last_csv_name = files_day[-1]
                    last_csv_path = os.path.join(fixed_folder_path, last_csv_name)
                    # Si un original no fue copiado por alguna razón, asegurar su existencia
                    if not os.path.exists(last_csv_path):
                        orig_last = os.path.join(day_path, last_csv_name)
                        if os.path.exists(orig_last):
                            shutil.copy(orig_last, last_csv_path)
                        else:
                            logger.warning(f"Último CSV {orig_last} no encontrado; saltando last_day_round para {day_folder}")
                            continue

                    extra_rows = last_day_round_with_extra(last_csv_path, logger)
                    if not extra_rows.empty:
                        key = (day_folder, measurement_folder)
                        if key in leftover_seconds:
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
                    # Generar un nombre seguro para el CSV (usar timestamp del primer registro)
                    extra_df['Timestamp'] = pd.to_datetime(extra_df['Timestamp'], utc=True, errors='coerce')
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