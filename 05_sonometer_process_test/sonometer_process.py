import os
import pandas as pd
from logging_config import setup_logging
from config import *
import tqdm

from datetime import  timedelta

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


def process_one_third_octave_xlsx(xlsx_path, output_folder,count,excel_rows):
    
      
    """
    Reads and processes a 1/3 octave band data XLSX file from a LxT sonometer.
    Saves the processed statistics to the specified output folder.
    """

    total = excel_rows
    chunksize = 1000  # Number of rows to read per chunk



    df_final = pd.DataFrame(columns= ['Filename', 'sensor_id', 'Timestamp', 'Unixtimestamp'] + third_octaves)
    for skip in tqdm.tqdm(range(0,total,chunksize),desc=f"Processing {chunksize} rows/it of {os.path.basename(xlsx_path)}"):
        
        # ---------------------------------------------------
        # 1- Initializing DataFrames
        # ---------------------------------------------------

        df_summary = pd.DataFrame(columns=['Filename', 'sensor_id', 'Timestamp', 'Unixtimestamp'],index = [0])
        df_measurements = pd.DataFrame(columns=third_octaves)
        df_sum_pivot = pd.DataFrame(columns=['Filename', 'sensor_id', 'Timestamp', 'Unixtimestamp'],index = [0])

        # ---------------------------------------------------
        # 2- Reading CSVs from XLSX Sheets
        # ---------------------------------------------------
        try:
            df_sum_pivot = pd.read_excel(xlsx_path, sheet_name='Summary', header=None)

            try:
                
                if skip == 0:
                    
                        df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Time History',header = 0,usecols="H:AQ",skiprows = skip,nrows = chunksize)
                        if df_measurements_pivot.columns.to_list() != third_octaves:
                            logger.warning(f"Error reading Time History using columns H:AQ {xlsx_path}, trying I:AR")
                            df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Time History',header = 0,usecols="I:AR",skiprows = skip,nrows = chunksize)

                else:
                    
                    df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Time History', header=None,names = third_octaves,usecols="H:AQ",skiprows = 1+ skip,nrows = chunksize)
                    if df_measurements_pivot.columns.tolist() != third_octaves:

                        logger.warning(f"Error reading Time History using columns H:AQ {xlsx_path}, trying I:AR")
                        df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Time History', header=None,names = third_octaves,usecols="I:AR",skiprows = 1+ skip,nrows = chunksize)
            except Exception as e:

                logger.warning(f"Time History sheet not found in {xlsx_path}, trying Measurement History sheet: {e}")
                
                if skip == 0:
                    df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Measurement History',header = 0,usecols="H:AQ",skiprows = skip,nrows = chunksize)
                else:
                    df_measurements_pivot = pd.read_excel(xlsx_path, sheet_name='Measurement History', header=None,names = third_octaves,usecols="I:AR",skiprows = 1+ skip,nrows = chunksize)

            except Exception as e:
                
                logger.error(f"Neither Measurement History nor Time History sheets found in {xlsx_path}: {e}")
                
                return
        except Exception as e:
            
            logger.error(f"Error processing XLSX to CSV for {xlsx_path}: {e}")
            
            return
        
        # ---------------------------------------------------
        # 3- Processing Summary Sheet
        # ---------------------------------------------------
        try:

            
            df_summary['Filename'] = df_sum_pivot[1][2]
            df_summary['sensor_id'] = df_sum_pivot[1][3]
            df_summary['Timestamp'] = pd.to_datetime(df_sum_pivot[1][13], format="%Y-%m-%d  %H:%M:%S")
            df_summary['Unixtimestamp'] = df_summary['Timestamp'].apply(lambda x: int(x.timestamp()))

        except Exception as e:
            
            logger.error(f"Error processing Summary sheet in {xlsx_path}: {e}")
            
            return
        # ---------------------------------------------------
        # 3- Processing Measurements Sheet
        # ---------------------------------------------------

        try:
            
            df_measurements = df_measurements_pivot[third_octaves].iloc[skip:]   
            if skip == 0: df_measurements = df_measurements.dropna()         
        
        except Exception as e:
            
            logger.error(f"Error processing Time/Measurement History sheet in {xlsx_path}: {e}")
            
            return

        # ---------------------------------------------------
        # 3- Concatenating Summary and Measurements DataFrames, extending Filename and sensor_id columns,
        #    adapting timestamp and unixtimestamp to a minute frequency
        # ---------------------------------------------------

        try:
            
            start_len = len(df_final)
            df_final = pd.concat([df_final, df_measurements_pivot], ignore_index=True)

            end_len = len(df_final)
            df_final.loc[start_len:end_len-1, 'Filename'] = df_summary['Filename'].iloc[0]
            df_final.loc[start_len:end_len-1, 'sensor_id'] = df_summary['sensor_id'].iloc[0]
            df_final.loc[start_len:end_len-1, 'Timestamp'] = df_summary['Timestamp'].iloc[0]
            df_final.loc[start_len:end_len-1, 'Unixtimestamp'] = df_summary['Unixtimestamp'].iloc[0]

        except Exception as e:

            logger.error(f"Error concatenating dataframes for {xlsx_path}: {e}")
            
            return
    
    first_idx = df_final['Timestamp'].first_valid_index()
    first_ts  = df_final.loc[first_idx, 'Timestamp']  

    n_rest = len(df_final) - first_idx - 1
    
    if n_rest > 0 : 
        df_final['Timestamp']=pd.date_range(
            start=first_ts,
            end = first_ts + timedelta(seconds = int(n_rest + 1)),
            freq='s')


    df_final['Unixtimestamp'] = (
    df_final['Timestamp']
    .dt.tz_localize('Europe/Madrid', nonexistent='shift_forward', ambiguous='NaT')
    .dt.tz_convert('UTC')
    .view('int64') // 10**9
    )
    point = file_path.split('/')[-2]
    name = point + '_processed.csv' if count == 0 else point + f'_processed_{count}.csv'
    
    df_final.dropna(inplace=True)
    df_final.to_csv(os.path.join(output_folder, name), index=False)

if __name__ == "__main__":

    logger = setup_logging("sonometer_process")

    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT CSV files]")

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
                            process_one_third_octave_xlsx(file_path, output_point_folder,count,600000)
                            count += 1
                            logger.info(f"[SONOMETER] -> Processed data saved at {output_point_folder}")


