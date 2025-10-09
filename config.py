# `20158.74Hz` DECIMAL(10,2),

MEDIDAS_FOLDER = "3-Medidas"
RESULTADOS_FOLDER = "5-Resultados"

# ----------------------------------------
#  SANDISK PATH
# ----------------------------------------
SANDISK_PATH_LINUX = "/mnt/sandisk/CONTENEDORES/CONTENEDORES/3-Medidas/"
SANDISK_PATH_WINDOWS = r"\\192.168.205.122\Contenedores"


# -------------------------
# ID MICRO
# -------------------------
ID_MICROPHONE = {
    "P1_CONTENEDORES": "RPI_1",
    "P2_CONTENEDORES": "RPI_2",
    "P3_CONTENEDORES": "RPI_3",
    "P4_CONTENEDORES": "RPI_4",
}


# -------------------------
# CALIBRATION CONSTANTS
# -------------------------
CALIBRATION_CONSTANTS = {
    # "P1_CONTENEDORES": -69.55, 
    "P1_CONTENEDORES": 0, 
    # "P2_CONTENEDORES": -68.74,
    "P2_CONTENEDORES": 0,
    # "P3_CONTENEDORES": -74.54,
    "P3_CONTENEDORES": 0,
    # "P4_CONTENEDORES": -82.24,
    "P4_CONTENEDORES": 0,
}



# ----------------------------------------
# BUCKET S3
# ----------------------------------------
BUCKET_NAME = 'demo-prototype-aac-2025'


# ----------------------------------------
# MQTT
# ----------------------------------------
# FLAG FOR DEMO
DEMO = False

# DEMO
MQTT_BROKER_DEMO = "mqtt.demo2.muutech.com"
MQTT_PORT_DEMO = "12003"


# FINAL ENDPOINT
MQTT_BROKER_MUUTECH = "mqtt.aacacustica.muutech.com"
MQTT_PORT_MUUTECH = "12081"
MQTT_USER_MUUTECH = "aac"
MQTT_PASSWORD_MUUTECH = "nrcXniqigJ1LEq2Dm_7y"


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

ACOUSTIC_TABLE_NAME = "noise_port_test.acoustic_data"
PREDICT_TABLE_NAME = "noise_port_test.predict_data"
WAV_TABLE_NAME = "noise_port_test.wav_data"

DB_INIT_SWITCH = False

ACOUSTIC_QUERY_SWITCH = True
PREDICT_QUERY_SWITCH = True
WAV_QUERY_SWITCH = False


TABLES = {
    "acoustic_data": """
        CREATE TABLE IF NOT EXISTS acoustic_data (
            record_id INT AUTO_INCREMENT PRIMARY KEY,
            LA DECIMAL(10,2),
            LC DECIMAL(10,2),
            LZ DECIMAL(10,2),
            LAmax DECIMAL(10,2),
            LAmin DECIMAL(10,2),
            `12.6Hz` DECIMAL(10,2),
            `15.8Hz` DECIMAL(10,2),
            `20.0Hz` DECIMAL(10,2),
            `25.1Hz` DECIMAL(10,2),
            `31.6Hz` DECIMAL(10,2),
            `39.8Hz` DECIMAL(10,2),
            `50.1Hz` DECIMAL(10,2),
            `63.1Hz` DECIMAL(10,2),
            `79.4Hz` DECIMAL(10,2),
            `100.0Hz` DECIMAL(10,2),
            `125.9Hz` DECIMAL(10,2),
            `158.5Hz` DECIMAL(10,2),
            `199.5Hz` DECIMAL(10,2),
            `251.2Hz` DECIMAL(10,2),
            `316.2Hz` DECIMAL(10,2),
            `398.1Hz` DECIMAL(10,2),
            `501.2Hz` DECIMAL(10,2),
            `631.0Hz` DECIMAL(10,2),
            `794.3Hz` DECIMAL(10,2),
            `1000.0Hz` DECIMAL(10,2),
            `1258.9Hz` DECIMAL(10,2),
            `1584.9Hz` DECIMAL(10,2),
            `1995.3Hz` DECIMAL(10,2),
            `2511.9Hz` DECIMAL(10,2),
            `3162.3Hz` DECIMAL(10,2),
            `3981.1Hz` DECIMAL(10,2),
            `5011.9Hz` DECIMAL(10,2),
            `6309.6Hz` DECIMAL(10,2),
            `7943.3Hz` DECIMAL(10,2),
            `10000.0Hz` DECIMAL(10,2),
            `12589.3Hz` DECIMAL(10,2),
            `15848.9Hz` DECIMAL(10,2),
            Filename VARCHAR(255),
            Timestamp DATETIME,
            Unixtimestamp BIGINT,
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