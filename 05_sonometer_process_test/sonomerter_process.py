import os
import pandas as pd
from logging_config import setup_logging

import regex as re
import datetime
from config import *

def index_transform(excel_index):
    match = re.match(r"^([a-z]+)(\d+)$", excel_index.lower())
    if not match:
        raise ValueError("Invalid index")

    x_cell = -1
    for idx, char in enumerate(match.group(1)[::-1]):
        x_cell += (26 ** idx) * (ord(char) - 96)  # ord('a') == 97

    y_cell = int(match.group(2)) - 1

    return y_cell, x_cell

def process_one_third_octave_xlsx(xlsx_path, output_folder="acoustics_sonometer"):
    """
    Reads and processes a 1/3 octave band data XLSX file.
    Saves the processed statistics to the specified output folder.
    """

    third_octaves = [
        '1/3 LZeq 6.3', '1/3 LZeq 8.0', '1/3 LZeq 10.0', '1/3 LZeq 12.5', '1/3 LZeq 16.0',
        '1/3 LZeq 20.0', '1/3 LZeq 25.0', '1/3 LZeq 31.5', '1/3 LZeq 40.0', '1/3 LZeq 50.0',
        '1/3 LZeq 63.0', '1/3 LZeq 80.0', '1/3 LZeq 100', '1/3 LZeq 125', '1/3 LZeq 160',
        '1/3 LZeq 200', '1/3 LZeq 250', '1/3 LZeq 315', '1/3 LZeq 400', '1/3 LZeq 500',
        '1/3 LZeq 630', '1/3 LZeq 800', '1/3 LZeq 1000', '1/3 LZeq 1250', '1/3 LZeq 1600',
        '1/3 LZeq 2000', '1/3 LZeq 2500', '1/3 LZeq 3150', '1/3 LZeq 4000', '1/3 LZeq 5000',
        '1/3 LZeq 6300', '1/3 LZeq 8000', '1/3 LZeq 10000', '1/3 LZeq 12500', '1/3 LZeq 16000', '1/3 LZeq 20000'
    ]
    XLS = pd.ExcelFile(xlsx_path)

    df_summary = pd.DataFrame()
    df_measurements = pd.DataFrame()

    # ---------------------------------------------------
    # 1- Processing Summary Sheet
    # ---------------------------------------------------

    df_summary['Filename'] = pd.read_excel(XLS,'Summary').iloc[index_transform(['B',3])].reset_index(drop=True)
    df_summary['Timestamp'] = pd.to_datetime(pd.read_excel(XLS,'Summary').iloc[index_transform(['B',14])], format='%Y%m%d_%H%M%S').reset_index(drop=True)
    df_summary['sensor_id'] = pd.read_excel(XLS,'Summary').iloc[index_transform(['B',4])].reset_index(drop=True)

    df_summary['Unixtimestamp'] = datetime.datetime(df_summary['Timestamp'].split('-')).strftime('%s')

    # ---------------------------------------------------
    # 2- Processing Measurements Sheet
    # ---------------------------------------------------

    df_measurements = pd.read_excel(XLS, 'Measurement History').loc[1:, 'AR':'CA'].reset_index(drop=True)


    # ---------------------------------------------------
    # 3- Concatenating Summary and Measurements DataFrames
    # ---------------------------------------------------

    df_final = pd.concat([df_final, df_measurements], axis=1)

    # ---------------------------------------------------
    # 4- Saving final dataframe to CSV
    # ---------------------------------------------------

    point = output_folder.split('/')[-1]
    df_final.to_csv(os.path.join(output_folder, f"{point}_processed.csv"), index=False)



if __name__ == "__main__":
    logger = setup_logging("sonometer_process")

    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT CSV files]")

    path = SANDISK_PATH_LINUX

    for point_folder in os.listdir(path):
        if point_folder == "P2_CONTENEDORES":
            points_folders = os.path.join(path, point_folder)
            if point_folder == "sonometer_files_test":
                for point in points_folders:
                    point_path = os.path.join("05_sonometer_process_test/sonometer_files_test", point)
                    output_point_folder = os.path.join("05_sonometer_process_test/sonometer_acoustics", point)
                    if os.path.isdir(point_path):
                        lxt_files = [f for f in os.listdir(point_path) if f.endswith('.csv')]
                        for file in lxt_files:
                            file_path = os.path.join(point_path, file)
                            logger.info(f"[SONOMETER] -> Processing file: {file_path}")
                            process_one_third_octave_xlsx(file_path, output_point_folder, point)
                            logger.info(f"[SONOMETER] -> Processed data saved at {output_point_folder}")


