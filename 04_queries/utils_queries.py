import mysql
import json
import os
import decimal
import ssl
import paho.mqtt.client as mqtt

from config import * 
from utils import * 
from utils_queries import * 
PATH = SANDISK_PATH_LINUX_NEW

ID_MICRO, LOCATION_RECORD, LOCATION_PLACE, LOCATION_POINT, \
AUDIO_SAMPLE_RATE, AUDIO_WINDOW_SIZE, AUDIO_CALIBRATION_CONSTANT,\
STORAGE_S3_BUCKET_NAME, STORAGE_OUTPUT_WAV_FOLDER, \
STORAGE_OUTPUT_ACOUSTIC_FOLDER = load_config_acoustic('config.yaml')

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

def initialize_process_files(query_acoustic_folder,query_pred_folder,query_wav_folder,query_sonometer_folder,logger):

    processed_folder_acoustic_txt       = os.path.join(query_acoustic_folder, "processed_acoustics.txt")
    processed_folder_predictions_txt    = os.path.join(query_pred_folder, "processed_predictions.txt")
    processed_folder_wav_txt            = os.path.join(query_wav_folder, "processed_wavs.txt")
    processed_folder_sonometer_txt      = os.path.join(query_sonometer_folder,"processed_sonometers.txt")
    processed_mqtt_data_txt_sonometer   = os.path.join(query_sonometer_folder,"records_sent.txt")
    #processed_mqtt_data_txt_spl         = os.path.join()

    logger.info(f"[Acoustics] Saving the processed file txt here -->    {processed_folder_acoustic_txt}")
    logger.info(f"[Predictions] Saving the processed file txt here -->  {processed_folder_acoustic_txt}")
    logger.info(f"[WAVs] Saving the processed file txt here -->         {processed_folder_acoustic_txt}")
    logger.info(f"[Sonometers] Saving the processed file txt here -->   {processed_folder_acoustic_txt}")
    logger.info(f"[MQTT] Saving the processed file txt here -->         {processed_folder_acoustic_txt}")
    logger.info(f"[MQTT SPL] Saving the processed file txt here -->     {processed_folder_acoustic_txt}")

    processed_acoustics                 = load_processed_folder(processed_folder_acoustic_txt)
    processed_predictions               = load_processed_folder(processed_folder_predictions_txt)
    processed_wavs                      = load_processed_folder(processed_folder_wav_txt)
    processed_sonometers                = load_processed_folder(processed_folder_sonometer_txt)
    

    return processed_folder_acoustic_txt,processed_folder_predictions_txt,processed_folder_wav_txt,processed_folder_sonometer_txt,processed_acoustics,processed_predictions,processed_wavs,processed_sonometers

def create_query_folders(point,logger):
        
        point_path_results = point.replace('3-Medidas','5-Resultados')

        query_acoustic_folder = os.path.join(point_path_results,'SPL','queries', "acoustic_params_query")
        query_pred_folder = os.path.join(point_path_results,'AI_MODEL', "predictions_litle_query")
        query_acoustic_folder = os.path.join(point_path_results,'SPL','queries', "acoustic_params_query")
        query_wav_folder = os.path.join(point_path_results,'SPL','queries', "wav_files_query")
        query_sonometer_folder = os.path.join(point_path_results,'SPL',"queries","sonometer_acoustics_query")

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

def get_sonometer_rasp_acoustics_preds_days_and_paths(logger,point):


    #sonometer_folder_path = os.path.join(point,'SONOMETRO')
    #raspberry_folder_path = os.path.join(point,'RASPBERRY')

    spl_folder_path = os.path.join(point.replace('3-Medidas','5-Resultados'),'SPL')
    AI_MODEL_folder_path = os.path.join(point.replace('3-Medidas','5-Resultados'),'AI_MODEL')

    #measurements_folder_path = os.path.join(point,'SPL','measurements')
    
    
    wavs_folder_path = os.path.join(point,'wav_files')
    sonometer_files_folder_path = os.path.join(point,'sonometer_files')
    acoustics_params_folder_path = os.path.join(point,'acoustic_params')
    predictions_litle_folder_path = os.path.join(AI_MODEL_folder_path,'predictions_litle')

    days_folders_wavs = [os.path.join(wavs_folder_path,file) for file in os.listdir(wavs_folder_path) if os.path.isdir(os.path.join(wavs_folder_path,file))]
    days_folders_acoustics = [os.path.join(acoustics_params_folder_path,file) for file in os.listdir(acoustics_params_folder_path) if 'fixed_' in file]
    days_folders_predictions = [os.path.join(predictions_litle_folder_path,file)  for file in os.listdir(predictions_litle_folder_path) if 'fixed_' in file]
    points_folders_sonometer = [os.path.join(sonometer_files_folder_path,file) for file in os.listdir(sonometer_files_folder_path)]

    return spl_folder_path,AI_MODEL_folder_path,wavs_folder_path,sonometer_files_folder_path,acoustics_params_folder_path, \
            predictions_litle_folder_path,days_folders_wavs,days_folders_acoustics,days_folders_predictions,points_folders_sonometer

    