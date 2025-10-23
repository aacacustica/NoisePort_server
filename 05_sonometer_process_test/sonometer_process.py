import os
import pandas as pd
import numpy as np
import openpyxl
import tqdm
import time


from logging_config import setup_logging
from config import *
from datetime import  datetime



third_octaves = [
    '1/3 LZeq 6.3', '1/3 LZeq 8.0', '1/3 LZeq 10.0', '1/3 LZeq 12.5', '1/3 LZeq 16.0',
    '1/3 LZeq 20.0', '1/3 LZeq 25.0', '1/3 LZeq 31.5', '1/3 LZeq 40.0', '1/3 LZeq 50.0',
    '1/3 LZeq 63.0', '1/3 LZeq 80.0', '1/3 LZeq 100', '1/3 LZeq 125', '1/3 LZeq 160',
    '1/3 LZeq 200', '1/3 LZeq 250', '1/3 LZeq 315', '1/3 LZeq 400', '1/3 LZeq 500',
    '1/3 LZeq 630', '1/3 LZeq 800', '1/3 LZeq 1000', '1/3 LZeq 1250', '1/3 LZeq 1600',
    '1/3 LZeq 2000', '1/3 LZeq 2500', '1/3 LZeq 3150', '1/3 LZeq 4000', '1/3 LZeq 5000',
    '1/3 LZeq 6300', '1/3 LZeq 8000', '1/3 LZeq 10000', '1/3 LZeq 12500', '1/3 LZeq 16000', '1/3 LZeq 20000'
]

output_csv_folder = os.getcwd() + '/05_sonometer_process_test/temp_csvs/'

def read_first_row_excel(path):
    """
    Reads the first row of an Excel file to determine the total number of rows.
    """
    try:
        try:
            df_first_row = pd.read_excel(path, sheet_name='Time History',skiprows=1,nrows=1)
        except:
            df_first_row = pd.read_excel(path, sheet_name='Measurement History')
    
        return df_first_row
    except Exception as e:
        logger.error(f"Error reading first row of Excel file {path}: {e}")
        return 0

def get_length_excel(path,sheet_name):
    wb = openpyxl.load_workbook(path,read_only=True,data_only=True)
    ws = wb[f'{sheet_name}'] if sheet_name else wb.active

    last_row_idx = ws.max_row
    
    return last_row_idx

def handle_not_finished_minute(datetime):

    if not datetime.minute == 00:
        datetime = datetime.replace(second=00)
        return datetime 

def get_row_indices_by_column(path, sheet_name, column_name, row_content_list, header_row=1):
    
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active

    excel_length = ws.max_row

    results = np.arange(start = 1,stop=excel_length ,step = 60)
    return results


def get_days_in_df(result_df):

    days_list = []

    for day in result_df['Timestamp']:

        if day.day not in days_list:
            days_list.append(day.day)
    
    return days_list

def process_one_third_octave_xlsx(xlsx_path, output_folder,count):
    
      
    """
    Reads and processes a 1/3 octave band data XLSX file from a LxT sonometer.
    Saves the processed statistics to the specified output folder.
    """
    
    try:
        df_first_row = pd.read_excel(xlsx_path, sheet_name='Measurement History',header= None,skiprows=1,nrows=1)
        try:
        # ------------------------------------
        # 1-Getting first and last row info
        # ------------------------------------
            logger.info("[XSLX Processing] Getting info from first and last rows of XLSX file")

            initial_date = handle_not_finished_minute(pd.to_datetime(df_first_row.iloc[0,1],format='%Y-%M-%D'))            
            excel_length = get_length_excel(xlsx_path,'Measurement History')
            
            df_last_row = pd.read_excel(xlsx_path, sheet_name='Measurement History',header= None,skiprows=excel_length-1,nrows=1)
            final_date = pd.to_datetime(df_last_row.iloc[0,1],format='%Y-%M-%D')
            
            df_last_row = df_last_row.drop(np.r_[0:43,79:151],axis=1)
            df_last_row.dropna(axis=1,inplace=True)
            df_last_row.columns = third_octaves
        
            df_first_row = df_first_row.drop(np.r_[0:43,79:151],axis=1)
            df_first_row.dropna(axis=1,inplace=True)
            df_first_row.columns = third_octaves
        
        except Exception as e:
            logger.error(f"Error reading first or last rows of excel file: {xlsx_path}")

        # ------------------------------------
        # 2-Getting content and index info from the desired hourly range
        # ------------------------------------
        try:
            logger.info("[XSLX Processing] Getting content and index info from the desired hourly range")

            row_content_list = pd.Series(pd.date_range(initial_date,final_date,freq = 'H')
                                            .strftime('%Y-%m-%d %H:%M:%S'))
            
            row_indexs = get_row_indices_by_column(xlsx_path,sheet_name='Measurement History',column_name = 'Time',row_content_list = row_content_list)

        except Exception as e:
            logger.error(f"Error getting index and context info from hourly range from file: {xlsx_path}")


        try:
            # ------------------------------------
            # 3-Reading excel file and naming columns
            # ------------------------------------
            logger.info("[XSLX Processing] Reading excel file and naming columns")

            df_result = pd.read_excel(xlsx_path,sheet_name = 'Measurement History',usecols = "AR:CA",header = 0,skiprows = lambda x: x not in row_indexs )
            df_result.columns = third_octaves
        except Exception as e:
            logger.error(f"Error reading excel file and naming columns from file: {xlsx_path}")

        try:

            # ------------------------------------
            # 4-Implementing data into columns from previously retrieved data
            # ------------------------------------
            logger.info("[XSLX Processing] Implementing data into columns from previously retrieved data")
            
            df_last_row['Timestamp'] = final_date
            
            df_result = pd.concat([df_result,df_last_row],ignore_index = True)

            df_result['Timestamp'] = row_content_list
            df_result['Timestamp'] = pd.to_datetime(df_result['Timestamp'], errors='coerce')
            df_result['Filename'] = os.path.basename(xlsx_path)
            df_result['Filename'].fillna(os.path.basename(xlsx_path)) 
            df_result['Unixtimestamp'] = (df_result['Timestamp'].view('int64') // 10**9).astype('Int64')

            df_result.sort_values(by='Timestamp')            

        except Exception as e:
            logger.error(f"Error implementing data into columns of xlsx file: {xlsx_path}")

        try:
            # ------------------------------------
            # 4-Saving per day CSV files from whole DF, and whole DF
            # ------------------------------------

            logger.info("[XSLX Processing] Saving per day CSV files from whole DF, and whole DF")

            days = get_days_in_df(df_result)
            output_folder = output_folder + f'/{point}'

            os.makedirs(output_folder, exist_ok=True)
            
            for day in days:
                
                day_df = df_result.loc[df_result['Timestamp'].dt.day == int(day)].copy()
                filename = os.path.join(output_folder, f"day{day}_{point}_Processed_{count}.csv") 
                day_df.to_csv(filename, index=False)

            df_result.to_csv(output_folder + f"/{point}_Processed_{count}.csv")

        except Exception as e:
            logger.error(f"Error splitting whole DF into per day DFs:{e}")






    except Exception as e:
        logger.error(f"Error reading Measurement History sheet from {path} ,  trying Time History sheet")


if __name__ == "__main__":

    start_time = time.time()
    logger = setup_logging("sonometer_process")

    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT XLSX files]")

    path = SANDISK_PATH_LINUX
    for point_folder in os.listdir(path):
        if point_folder == "P2_CONTENEDORES":
            
            output_point_folder = os.path.join(path, point_folder,'sonometer_acoustics')
            points_folders = os.path.join(path, point_folder, "sonometer_files_test")
            
            for point in tqdm.tqdm(os.listdir(points_folders), desc="Processing Points"):
                    if point == 'Boluda':
                        count = 0
                        lxt_files = [f for f in os.listdir(os.path.join(points_folders,point)) if f.endswith('.xlsx')]
                        for file in tqdm.tqdm(lxt_files, desc=f"Processing files in {point}"):
                            
                            file_path = os.path.join(points_folders,point, file)
                            logger.info(f"[SONOMETER] -> Processing file: {file_path}")                            
                            process_one_third_octave_xlsx(file_path, output_point_folder,count)
                            count += 1
                            logger.info(f"[SONOMETER] -> Processed data saved at: {output_point_folder}")

    logger.info("---------------------------")
    logger.info("--- Execution Time:%s  ---" % round(time.time() - start_time,2))
    logger.info("---------------------------")

    print("--- %s seconds ---" % round(time.time() - start_time,2))
