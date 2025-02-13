# ----------------------------------------
# BUCKET S3
# ----------------------------------------
BUCKET_NAME = 'demo-prototype-aac-2025'

# ----------------------------------------
# MQTT
# ----------------------------------------
MQTT_BROKER = "mqtt.demo2.muutech.com"
MQTT_PORT = "12003"

# ----------------------------------------
# MYSQL
# ----------------------------------------
HOST = "localhost"
USER = "santi"
PASSWORD = "Mysql_Pssw2025!"


# ----------------------------------------
# DATABASE ATTRIBUTES 
# ----------------------------------------
DATABASE_NAME = "noise_port_test"

ACOUSTIC_TABLE_NAME = "acoustic_data"
PREDICT_TABLE_NAME = "predict_data"
WAV_TABLE_NAME = "wav_data"


TABLES = {
    "acoustic_data": """
        CREATE TABLE IF NOT EXISTS acoustic_data (
            record_id INT AUTO_INCREMENT PRIMARY KEY,
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
            Timestamp DATETIME,
            sensor_id VARCHAR(32)
        );
    """,


    "predict_data": """
        CREATE TABLE IF NOT EXISTS predict_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Prediction_1 VARCHAR(100),
            Prediction_2 VARCHAR(100),
            Prediction_3 VARCHAR(100),
            Prob_1 DECIMAL(5,2),
            Prob_2 DECIMAL(5,2),
            Prob_3 DECIMAL(5,2),
            Filename VARCHAR(255),
            Timestamp DATETIME
        );
    """,


    "wav_data": """
        CREATE TABLE IF NOT EXISTS wav_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Filename VARCHAR(255),
            Timestamp DATETIME,
            Duration DECIMAL(10,2)
        );
    """
}