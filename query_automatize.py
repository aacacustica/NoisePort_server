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

        cursor.execute(QUERY_CREATE_DATABASE)
        logger.info(f"Creating DATABASE --> {DATABASE_NAME}")

        cursor.execute(f"USE {DATABASE_NAME};")
        logger.info(f"Using --> {DATABASE_NAME}")

        cursor.execute(QUERY_CREATE_TABLE)
        logger.info(f"Creating table --> {TABLE}")

        db.commit()
        print("Database and table ensured.")
    
    
    except mysql.connector.Error as err:
        print("Error initializing database:", err)
    finally:
        cursor.close()
        db.close()



def main():
    # initialize logger
    logger = setup_logging()
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