import os
import pandas as pd
from logging_config import setup_logging

import regex as re
import datetime
from config import *

from xlsx2csv import Xlsx2csv

def index_transform(excel_index):
    match = re.match(r"^([a-z]+)(\d+)$", excel_index.lower())
    if not match:
        raise ValueError("Invalid index")

    x_cell = -1
    for idx, char in enumerate(match.group(1)[::-1]):
        x_cell += (26 ** idx) * (ord(char) - 96)  # ord('a') == 97

    y_cell = int(match.group(2)) - 1

    return y_cell, x_cell

def process_one_third_octave_xlsx(xlsx_path, output_folder):
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

    with Xlsx2csv(xlsx_path, outputencoding="utf-8") as xlsx2csv:

        try:
            xlsx2csv.convert(xlsx_path, sheetname="Measurement History", outputencoding="utf-8")
        except Exception as e:
            xlsx2csv.convert(xlsx_path, sheetname="Time History", outputencoding="utf-8")
        except Exception as e:
            logger.error(f"Error converting XLSX to CSV for {xlsx_path}: {e}")
            return

    df_summary = pd.DataFrame(columns=['Filename', 'Timestamp', 'Unixtimestamp', 'sensor_id'])
    df_measurements = pd.DataFrame(columns=third_octaves)

    df_final = pd.DataFrame()
    # ---------------------------------------------------
    # 1- Processing Summary Sheet
    # ---------------------------------------------------
    try:

        y, x = index_transform('B3')
        sensor_id = pd.read_excel(XLS, sheet_name='Summary').iloc[y, x]
        y, x = index_transform('B13')
        timestamp = pd.read_excel(XLS, sheet_name='Summary').iloc[y, x]
        y, x = index_transform('B13')
        unixtimestamp = timestamp.strftime('%s')
        y,x = index_transform('B2')
        filename = pd.read_excel(XLS, sheet_name='Summary').iloc[y, x]
        df_summary = pd.DataFrame({'Filename': [filename], 'Timestamp': [timestamp] , 'Unixtimestamp': [unixtimestamp] , 'sensor_id': [sensor_id] })

    except Exception as e:
        logger.error(f"Error processing Summary sheet in {xlsx_path}: {e}")
        return
    # ---------------------------------------------------
    # 2- Processing Measurements Sheet
    # ---------------------------------------------------
    try:
        
        df_measurements = pd.read_excel(XLS, 'Measurement History').loc[1:, '1/3 LZeq 6.3':'1/3 LZeq 20000'].reset_index(drop=True)

    except Exception as e:

        logger.error(f"Error processing Measurement History sheet in {xlsx_path}, trying Time History Sheet: {e}")
        df_measurements = pd.read_excel(XLS, 'Time History').loc[1:, '1/3 LZeq 6.3':'1/3 LZeq 20000'].reset_index(drop=True)


    except Exception as e:
        
        logger.error(f"Error processing Time History sheet in {xlsx_path}: {e}")
        
        return

    # ---------------------------------------------------
    # 3- Concatenating Summary and Measurements DataFrames
    # ---------------------------------------------------
    try:

        df_final = pd.concat([df_summary, df_measurements], axis=1)

    except Exception as e:

        logger.error(f"Error concatenating dataframes for {xlsx_path}: {e}")
        
        return
    # ---------------------------------------------------
    # 4- Saving final dataframe to CSV
    # ---------------------------------------------------
    try:

        point = file_path.split('/')[-2]
        df_final.to_csv(os.path.join(output_folder, f"{point}_processed.csv"), index=False)

    except Exception as e:  
        logger.error(f"Error saving processed data for {xlsx_path}: {e}")
        return
    





if __name__ == "__main__":
    logger = setup_logging("sonometer_process")

    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT CSV files]")

    path = SANDISK_PATH_LINUX

    for point_folder in os.listdir(path):
        if point_folder == "P2_CONTENEDORES":
            
            output_point_folder = os.path.join(path, point_folder,'sonometer_acoustics')
            points_folders = os.path.join(path, point_folder, "sonometer_files_test")
            
            for point in os.listdir(points_folders):
                    count = 0
                    lxt_files = [f for f in os.listdir(os.path.join(points_folders,point)) if f.endswith('.xlsx')]
                    for file in lxt_files:
                        file_path = os.path.join(points_folders,point, file)
                        logger.info(f"[SONOMETER] -> Processing file: {file_path}")
                        process_one_third_octave_xlsx(file_path, output_point_folder)
                        count += 1
                        logger.info(f"[SONOMETER] -> Processed data saved at {output_point_folder}")


