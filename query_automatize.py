import json
import mysql.connector
import paho.mqtt.client as mqtt
import os
import re
import pandas as pd
from logging_config import setup_logging
from config import *



def initialize_database(db, logger):
    """Ensure that the database and table exist"""
    #this is to setting 
    cursor = None

    try:
        # ---------------------------------------------------------------------------
        # ENSURE DATABASE
        # ---------------------------------------------------------------------------
        logger.info("Ensure that the database and table exist")
        #avoid unread result issues
        cursor = db.cursor(buffered=True)

        query_create_db = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"
        cursor.execute(query_create_db)
        logger.info(f"Creating DATABASE --> {DATABASE_NAME}")

        cursor.execute(f"USE {DATABASE_NAME};")
        logger.info(f"Using --> {DATABASE_NAME}")


        # ---------------------------------------------------------------------------
        # ENSURE TABLES
        # ---------------------------------------------------------------------------
        for table_name, create_statement in TABLES.items():
            logger.info("Creating table --> %s", table_name)
            cursor.execute(create_statement)

            # describe the table 
            cursor.execute(f"DESCRIBE {table_name};")
            table_structure = cursor.fetchall()
            logger.info("Table structure for %s: %s", table_name, table_structure)

        db.commit()
        logger.info("Database and tables ensured.")


    #LAST BLOCK
    except mysql.connector.Error as err:
        logger.error("Error initializing database: %s", err)
    finally:
        # close cursor
        if cursor is not None:
            try:
                cursor.close()
            except mysql.connector.Error as err:
                logger.error("Error closing cursor: %s", err)
        # try:
        #     db.close()
        # except mysql.connector.Error as err:
        #     logger.error("Error closing database connection: %s", err)



def load_data_db(db, data_path, logger, table_name=ACOUSTIC_TABLE_NAME):
    cursor = db.cursor(dictionary=True)
    
    query_load = f"""
        LOAD DATA LOCAL INFILE '{data_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' 
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (LA, LC, LZ, LAmax, LAmin, `12.40Hz`, `15.62Hz`, `19.69Hz`, `24.80Hz`, `31.25Hz`, `39.37Hz`, `49.61Hz`, `62.50Hz`, `78.75Hz`, `99.21Hz`, `125.00Hz`, `157.49Hz`, `198.43Hz`, `250.00Hz`, `314.98Hz`, `396.85Hz`, `500.00Hz`, `629.96Hz`, `793.70Hz`, `1000.00Hz`, `1259.92Hz`, `1587.40Hz`, `2000.00Hz`, `2519.84Hz`, `3174.80Hz`, `4000.00Hz`, `5039.68Hz`, `6349.60Hz`, `8000.00Hz`, `10079.37Hz`, `12699.21Hz`, `16000.00Hz`, `20158.74Hz`, Filename, Timestamp, sensor_id);
    """

    
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



def power_laeq_avg(db, logger, table_name=ACOUSTIC_TABLE_NAME):
    """
    Execute a query that calculates the LAeq averages and returns the result.
    """
    cursor = db.cursor(dictionary=True)
    query = f"""
    SELECT 
        sensor_id,
        CONCAT(DATE_FORMAT(Timestamp, '%Y-%m-%d %H:00:00'), ' CET') AS hour,
        10 * LOG10(AVG(POWER(10, LA/10))) AS AVG_LAeq,
        MAX(LAmax) AS max_LAmax,
        MIN(LAmin) AS min_LAmin
    FROM {table_name}
    GROUP BY sensor_id, hour;
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
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




def main():
    # ------------------------------------
    # INITIALIZATION
    # ------------------------------------
    # logger
    logger = setup_logging('query_automatize.log')

    # paths and processed csv_files
    logger.info("Starting!!")
    home_dir = os.getenv("HOME")
    resultados_folder = os.path.join(home_dir, "RESULTADOS")
    processed_list='processed_csv.txt'
    
    # database
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True
    )

    logger.info("Initializing database!")
    # testing the query database
    initialize_database(db, logger)
    

    # ------------------------------------
    # PROCESSING
    # ------------------------------------
    logger.info("Processing!")

    already_processed = set()
    if os.path.exists(processed_list):
        with open(processed_list, 'r') as f:
            for line in f:
                already_processed.add(line.strip())


    # LOOPING OVER THE CSV FILES TO MAKE THEM HOURLY AND UPLOADING INTO THE TABLE
    # ACOUSTIC PARAMETERS
    for root, dirs, files in os.walk(resultados_folder):
        for folder in dirs:
            if folder == 'hourly':
                full_path = os.path.join(root, folder)
                csv_files = os.listdir(full_path)
                logger.info("CSV files in %s: %s", full_path, csv_files)

                for csv_file in csv_files:
                    # processing files that are not already processed
                    if csv_file not in already_processed:
                        full_csv_file = os.path.join(full_path, csv_file)
                        logger.info("Processing file: %s", full_csv_file)

                        df = pd.read_csv(full_csv_file)
                        
                        logger.info("Loading data into TABLE")
                        load_data_db(db, full_csv_file, logger)

                        # mark file as processed
                        # with open(processed_list, 'a') as f:
                        #     f.write(csv_file + "\n")


                    else:
                        logger.info("Skipping already processed file: %s", csv_file)



    # ------------------------------------
    # Query and Convert Results to JSON
    # ------------------------------------
    logger.info("Query and Convert Results to JSON")
    avg_results = power_laeq_avg(db, logger)
    if avg_results is not None:
        print("Power LAeq Average Results:")
        print(avg_results)
        
        # send the data via MQTT
        send_mqtt_data(avg_results, logger)
    else:
        print("No results returned from power_laeq_avg query.")


    # NOW --> CLOSING THE DB
    try:
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)


if __name__ == "__main__":
    main()