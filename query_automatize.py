import json
import mysql.connector
import paho.mqtt.client as mqtt
import os
import re
import pandas as pd
from logging_config import setup_logging
from config import *
from queries import *



def mqtt_send_data():
    # 1. connection MySQL
    db = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE_NAME,
        allow_local_infile=True  # equivalent to --local-infile=1 
    )
    cursor = db.cursor(dictionary=True)

    query = """
    SELECT 
        DATE_FORMAT(Timestamp, '%Y-%m-%d %H:00:00') AS hour,
        10 * LOG10(AVG(POWER(10, LA/10))) AS LAeq,
        MAX(LAmax) AS max_LAmax,
        MIN(LAmin) AS min_LAmin
    FROM acoustic_data
    GROUP BY hour;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    #convert the query result to JSON
    payload = json.dumps(rows)
    print("Data payload:", payload)



    # 2. connect to the MQTT broker
    mqtt_client = mqtt.Client()
    # username/password:
    # mqtt_client.username_pw_set("broker_username", "broker_password")
    mqtt_broker = "localhost"  # or the IP/hostname of broker
    mqtt_port = 1883
    mqtt_client.connect(mqtt_broker, mqtt_port, 60)



    # 3. publish the data to a topic
    topic = "acoustic/data"  # topic name
    mqtt_client.publish(topic, payload)
    print(f"Published data to topic '{topic}'")



    # 4. clean and disconnect
    mqtt_client.disconnect()
    cursor.close()
    db.close()





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
    logger.info(f"Database object: {db}")
    logger.info(f"Data file path: {data_path}")
    logger.info(f"Target table: {table_name}")

    cursor = db.cursor(dictionary=True)
    
    # Note: The data_path must be wrapped in quotes in the SQL query.
    query_load = f"""
    LOAD DATA LOCAL INFILE '{data_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    """
    
    try:
        # Execute the query to load data.
        cursor.execute(query_load)
        # Commit the transaction so changes are saved.
        db.commit()
        logger.info("Data loaded successfully")
    except mysql.connector.Error as err:
        logger.error("Error loading data: %s", err)
        db.rollback()  # Optionally roll back changes if an error occurs.
    finally:
        cursor.close()
        # db.close()  




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
    processed_list='processed_csvs.txt'
    
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
    
    # pattern to get the filename for acoustic csv fileas
    pattern = re.compile(r'^\d{8}_\d{6}\.csv$')


    # LOOPING OVER THE CSV FILES TO MAKE THEM HOURLY AND UPLOADING INTO THE TABLE
    # ACOUSTIC PARAMETERS
    for root, dirs, files in os.walk(resultados_folder):
        for folder in dirs:
            if folder == 'hourly':
                full_path = os.path.join(root, folder)
                
                csv_files = os.listdir(full_path)
                logger.info(f"These are the csv file --> {csv_files}")

                for csv_file in csv_files:
                    if csv_file not in already_processed:
                        full_csv_file = os.path.join(full_path, csv_file)
                        logger.info(f"CSV file full path --> {full_csv_file}")
                        df = pd.read_csv(full_csv_file)
                        
                        # query load data locally
                        logger.info("Loading data into TABLE")
                        load_data_db(db, full_csv_file, logger)
                        exit()



                        # mark file as processed
                        # with open(processed_list, 'a') as f:
                        #     f.write(csv_file + "\n")
                    else:
                        print(f"Already processed: {csv_file}")



    # NOW --> CLOSING THE DB
    try:
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)


if __name__ == "__main__":
    main()