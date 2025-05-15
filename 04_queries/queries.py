import json
import mysql.connector
import paho.mqtt.client as mqtt
import os
import pandas as pd
from logging_config import setup_logging
from utils import *
from config import *
import tqdm 




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
    cursor = db.cursor()
    try:
        # 1) clear old data
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        db.commit()
        logger.info("Old data truncated successfully")

        # 2) load new CSV
        query_load = f"""
        LOAD DATA LOCAL INFILE '{data_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (
          @id_micro,
          Filename,
          Timestamp,
          @unixts,
          LA, LC, LZ, LAmax, LAmin,
          `12.6Hz`, `15.8Hz`, `20.0Hz`, `25.1Hz`, `31.6Hz`,
          `39.8Hz`, `50.1Hz`, `63.1Hz`, `79.4Hz`, `100.0Hz`,
          `125.9Hz`, `158.5Hz`, `199.5Hz`, `251.2Hz`, `316.2Hz`,
          `398.1Hz`, `501.2Hz`, `631.0Hz`, `794.3Hz`, `1000.0Hz`,
          `1258.9Hz`, `1584.9Hz`, `1995.3Hz`, `2511.9Hz`,
          `3162.3Hz`, `3981.1Hz`, `5011.9Hz`, `6309.6Hz`,
          `7943.3Hz`, `10000.0Hz`, `12589.3Hz`, `15848.9Hz`
        )
        SET 
            sensor_id = TRIM(BOTH '\\r' FROM @id_micro),
            Unixtimestamp = @unixts;
        """
        cursor.execute(query_load)
        db.commit()
        logger.info("Data loaded successfully")

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
      CONCAT(DATE_FORMAT(Timestamp, '%Y-%m-%d %H:00:00'), ' CET')  AS hour,
      MIN(Unixtimestamp)                                        AS unixtimestamp,
      10 * LOG10(AVG(POWER(10, LA/10)))                          AS AVG_LAeq,
      MAX(LAmax)                                                AS max_LAmax,
      MIN(LAmin)                                                AS min_LAmin
    FROM {table_name}
    GROUP BY sensor_id, hour;
    """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"LOAD DATA inserted {cursor.rowcount} rows into {table_name}")
        logger.info("Query executed successfully.")
        return rows
    except mysql.connector.Error as err:
        logger.error("Error executing query: %s", err)
        return None
    finally:
        cursor.close()




def send_mqtt_data(data, logger, broker=MQTT_BROKER, port=MQTT_PORT):
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
    
    # ensure port is an integer 
    port = int(port)
    
    #MQTT client and connect.
    client = mqtt.Client()
    client.connect(broker, port, 60)
    
    # Publish payload to the topic
    client.publish(topic, payload)
    logger.info("Published data to topic '%s': %s", topic, payload)
    
    client.disconnect()



def load_processed_files(processed_file_path):
    """Load the set of processed filenames from a text file."""
    if os.path.exists(processed_file_path):
        with open(processed_file_path, "r") as f:
            return {line.strip() for line in f if line.strip()}
    return set()



def update_processed_files(processed_file_path, filename):
    """Append a processed filename to the text file."""
    with open(processed_file_path, "a") as f:
        f.write(filename + "\n")




def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    # logger
    logger = setup_logging('query_automatize.log')

    # database
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True)

    logger.info("Initializing database!")
    # testing the query database
    initialize_database(db, logger)


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
    for point in points:
        if "P2_CONTENEDORES" in point:
            print("P2_CONTENEDORES")

            # ---------------------------
            acoust_folder = os.path.join(point, storage_output_acoust_folder)
            logger.info(f"Acoustic params folder: {acoust_folder}")

            # change storage_output_acoust_folder to "acoustic_params_query"
            query_folder = os.path.join(point, "acoustic_params_query")
            logger.info(f"Query folder: {query_folder}")
            os.makedirs(query_folder, exist_ok=True)


            # checking if the folder exist
            if os.path.isdir(acoust_folder):
                logger.info(f"Folder exists: {acoust_folder}")
            else:
                logger.warning(f"Folder does not exist: {acoust_folder}")
                continue

            # ---------------------------
            # INIZIALATIN PROCESSING FILE
            # ---------------------------
            processed_files_txt = os.path.join(query_folder, "processed_acoustic_query.txt")
            logger.info(f"Saving the proicessed file txt here --> {processed_files_txt}")
            processed_files = load_processed_files(processed_files_txt)



            folder_days = os.listdir(acoust_folder)
            # filter the FILES, FUST THE FOLDERS
            folder_days = [day_folder for day_folder in folder_days if os.path.isdir(os.path.join(acoust_folder, day_folder))]
            logger.info("Folder days in %s: %s", acoust_folder, folder_days)
            folder_days = [os.path.join(acoust_folder, day_folder) for day_folder in folder_days]


            # -.--------------------
            # PROCESSING
            # --------------------
            logger.info("")
            for day in tqdm.tqdm(folder_days, desc="Processing days", unit="day"):
                #day string to save the concat file
                day_str = day.split("/")[-1]
                logger.info("Processing day_hour: %s", day_str)
                logger.info("Processing: %s", day)
                
                csv_files = os.listdir(day)
                csv_files = [csv_file for csv_file in csv_files if csv_file.endswith(".csv")]
                logger.info("CSV files in %s: %s", day, csv_files)
                csv_files = [os.path.join(day, csv_file) for csv_file in csv_files]
                

                # concatenating the csv files
                logger.info("Trying to concatenate the csv files to process one hour of audio data recordings")
                df_day = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files], ignore_index=True)
                
                # order by the timestamp
                df_day = df_day.sort_values(by=["Timestamp"])
                print(df_day)
                exit()

                # make result csv_file
                csv_concat_path = os.path.join(query_folder, f"{day_str}.csv")
                logger.info("Concatenated CSV file path: %s", csv_concat_path)

                # save csv file
                df_day.to_csv(os.path.join(query_folder, f"{day_str}.csv"), index=False)
                logger.info("Concatenated CSV files, saved as: %s", csv_concat_path)
                # exit()

                

                logger.info("Loading data into TABLE")
                load_data_db(db, csv_concat_path, logger)
                cur = db.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {ACOUSTIC_TABLE_NAME}")
                n = cur.fetchone()[0]
                logger.info(f"→ {ACOUSTIC_TABLE_NAME} contains {n} rows after LOAD DATA")
                cur.close()
                
                
                
                # ------------------------------------
                # Query and Convert Results to JSON
                # ------------------------------------
                logger.info("Query and Convert Results to JSON")
                avg_results = power_laeq_avg(db, logger)
                print(avg_results)
                logger.info(avg_results)
                exit()

                # if avg_results is not None:
                #     logger.info("Power LAeq Average Results:")                      
                #     # send the data MQTT
                #     send_mqtt_data(avg_results, logger)
                # else:
                #     logger.warning("No results returned from power_laeq_avg query.")



                # ------------------------------------
                # Update processed files
                # ------------------------------------
                # update the processed files
                # update_processed_files(processed_files_txt, csv_file)
                # logger.info("Updated processed files list with: %s", csv_file)
                # #add processed file
                # processed_files.add(csv_file)
                # logger.info("Added to processed files: %s", csv_file)



    #CLOSING THE DB
    try:
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)


if __name__ == "__main__":
    main()