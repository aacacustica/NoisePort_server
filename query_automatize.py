import json
import mysql.connector
import paho.mqtt.client as mqtt



HOST = "localhost"
USER = "root"
PASSWORD = ""
DATABASE_NAME = "noise_port_test"


QUERY_CREATE_DATABASE = f"""
    CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};
"""

QUERY_CREATE_TABLE = """
CREATE TABLE acoustic_data (
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


QUERY_LOAD_DATA = """
LOAD DATA LOCAL INFILE '/home/aac_s3_test/retrieve_Data/downloads/CONTENEDORES/P1_CONTENEDORES/acoustic_params/concatenated.csv'
    INTO TABLE acoustic_data
    FIELDS TERMINATED BY ',' 
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
IGNORE 1 LINES;
"""

QUERY_LAEQ = """
SELECT 
    DATE_FORMAT(Timestamp, '%Y-%m-%d %H:00:00') AS hour,
    10 * LOG10(AVG(POWER(10, LA/10))) AS LAeq,
    MAX(LAmax) AS max_LAmax,
    MIN(LAmin) AS min_LAmin
FROM acoustic_data
GROUP BY hour;
"""



def mqtt_send_data():
    # 1. Connect to MySQL
    db = mysql.connector.connect(
        host=HOST,       # adjust if your MySQL is on a different host
        user=USER,   # replace with your MySQL username
        password=PASSWORD,  # replace with your MySQL password
        database="noise_port_test"
    )
    cursor = db.cursor(dictionary=True)

    # Your aggregated query
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

    # Optionally, convert the query result to JSON
    payload = json.dumps(rows)
    print("Data payload:", payload)

    # 2. Connect to the MQTT broker
    mqtt_client = mqtt.Client()
    # If your broker requires username/password:
    # mqtt_client.username_pw_set("broker_username", "broker_password")
    mqtt_broker = "localhost"  # or the IP/hostname of your broker
    mqtt_port = 1883
    mqtt_client.connect(mqtt_broker, mqtt_port, 60)

    # 3. Publish the data to a topic
    topic = "acoustic/data"  # Choose a topic name that fits your use case
    mqtt_client.publish(topic, payload)
    print(f"Published data to topic '{topic}'")

    # 4. Clean up and disconnect
    mqtt_client.disconnect()
    cursor.close()
    db.close()




def main():
    home_dir = os.getenv("HOME")
    resultados_folder = os.path.join(home_dir, "RESULTADOS")

    print(resultados_folder)



if __name__ == "__main__":
    main()