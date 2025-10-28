import os
import pandas as pd
import numpy as np
import openpyxl
import tqdm
import time
import sys

from logging_config import setup_logging
from config import *
import mysql.connector

path_to_queries = os.path.abspath('/home/aac_s3_test/noisePort_server/04_queries')
print(path_to_queries)
sys.path.append(path_to_queries)
from queries import load_data_db,initialize_database, power_laeq_avg,send_mqtt_data
from datetime import  datetime





output_csv_folder = os.getcwd() + '/05_sonometer_process_test/temp_csvs/'






if __name__ == "__main__":

    start_time = time.time()
    logger = setup_logging("sonometer_process")

    db = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            allow_local_infile=True)
    
    logger.info("Initializing database")
    # testing the query database
    if DB_INIT_SWITCH: initialize_database(db, logger)
    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT XLSX files]")

    path = SANDISK_PATH_LINUX



    try:
        logger.info("")
        db.close()
        logger.info("Database connection closed")
    except mysql.connector.Error as err:
        logger.error("Error closing database connection: %s", err)
