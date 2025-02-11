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



def load_data_db(data_path: str):
    db = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE_NAME,
    allow_local_infile=True  # equivalent to --local-infile=1 in the CLI
    )

    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW TABLES;")
    tables_result = cursor.fetchall()
    print("Tables in the database:", tables_result)

    
    cursor.execute(f"DESCRIBE {TABLE};")
    desc_result = cursor.fetchall()
    print("Structure of acoustic_data:", desc_result)

    #----------------------------
    # LOAD DATA INTO TABLE (acoustic_data)
    #----------------------------
    query_load = f"""
    LOAD DATA LOCAL INFILE {data_path}
        INTO TABLE acoustic_data
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    """

    cursor.close()
    db.close()





def initialize_database(db, logger):
    """Ensure that the database and table exist"""
    try:
        logger.info("Ensure that the database and table exist")
        cursor = db.cursor()


        query_create_db = f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};
        """
        cursor.execute(query_create_db)
        logger.info(f"Creating DATABASE --> {DATABASE_NAME}")

        cursor.execute(f"USE {DATABASE_NAME};")
        logger.info(f"Using --> {DATABASE_NAME}")


        query_create_table = f"""
            CREATE TABLE {TABLE_NAME_ACOUST} (
                LA DECIMAL(10,2),
                LC DECIMAL(10,2),
                LZ DECIMAL(10,2),
                LAmax DECIMAL(10,2),
                LAmin DECIMAL(10,2),
                `12.40Hz` DECIMAL(10,2),
                `15.62Hz` DECIMAL(10,2),
                `19.69Hz` DECIMAL(10,2),
                `24.80Hz` DECIMAL(10,2),
                `31.25Hz` DECIMAL(10,2),
                `39.37Hz` DECIMAL(10,2),
                `49.61Hz` DECIMAL(10,2),
                `62.50Hz` DECIMAL(10,2),
                `78.75Hz` DECIMAL(10,2),
                `99.21Hz` DECIMAL(10,2),
                `125.00Hz` DECIMAL(10,2),
                `157.49Hz` DECIMAL(10,2),
                `198.43Hz` DECIMAL(10,2),
                `250.00Hz` DECIMAL(10,2),
                `314.98Hz` DECIMAL(10,2),
                `396.85Hz` DECIMAL(10,2),
                `500.00Hz` DECIMAL(10,2),
                `629.96Hz` DECIMAL(10,2),
                `793.70Hz` DECIMAL(10,2),
                `1000.00Hz` DECIMAL(10,2),
                `1259.92Hz` DECIMAL(10,2),
                `1587.40Hz` DECIMAL(10,2),
                `2000.00Hz` DECIMAL(10,2),
                `2519.84Hz` DECIMAL(10,2),
                `3174.80Hz` DECIMAL(10,2),
                `4000.00Hz` DECIMAL(10,2),
                `5039.68Hz` DECIMAL(10,2),
                `6349.60Hz` DECIMAL(10,2),
                `8000.00Hz` DECIMAL(10,2),
                `10079.37Hz` DECIMAL(10,2),
                `12699.21Hz` DECIMAL(10,2),
                `16000.00Hz` DECIMAL(10,2),
                `20158.74Hz` DECIMAL(10,2),
                Filename VARCHAR(255),
                Timestamp DATETIME
            );
        """
        cursor.execute(query_create_table)
        logger.info(f"Creating table --> {TABLE_NAME}")

        db.commit()
        logger.info("Database and table ensured.")
    
    
    except mysql.connector.Error as err:
        logger.error("Error initializing database:", err)
    finally:
        cursor.close()
        db.close()



def main():
    # initialize logger
    logger = setup_logging('quey_automatize.log')
    exit()
    # initialize database
    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True
    )

    # testing the query database
    initialize_database(db, logger)
    
    exit()
    

    home_dir = os.getenv("HOME")
    resultados_folder = os.path.join(home_dir, "RESULTADOS")
    processed_list='processed_csvs.txt'


    already_processed = set()
    if os.path.exists(processed_list):
        with open(processed_list, 'r') as f:
            for line in f:
                already_processed.add(line.strip())
    
    pattern = re.compile(r'^\d{8}_\d{6}\.csv$')

    for root, dirs, files in os.walk(resultados_folder):
        for folder in dirs:
            if folder == 'hourly':
                full_path = os.path.join(root, folder)
                print(full_path)
                
                csv_files = os.listdir(full_path)
                print(csv_files)
                for csv_file in csv_files:
                    if csv_file not in already_processed:
                        full_csv_file = os.path.join(full_path, csv_file)
                        print(full_csv_file)
                        df = pd.read_csv(full_csv_file)
                        # print(df)

                        # query load data locally
                        load_data_db(full_csv_file)



                        # mark file as processed
                        # with open(processed_list, 'a') as f:
                        #     f.write(csv_file + "\n")
                    else:
                        print(f"Already processed: {csv_file}")



if __name__ == "__main__":
    main()