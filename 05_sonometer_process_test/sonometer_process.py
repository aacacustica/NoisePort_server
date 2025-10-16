import os
import pandas as pd
from logging_config import setup_logging
from config import *
import subprocess
import sys
from pathlib import Path
import tqdm
sys.path.append(str(Path("/home/aac_s3_test/noisePort_server/05_sonometer_process_test")))
from xlsx_converter import *  

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


def process_one_third_octave_xlsx(xlsx_path, output_folder,count):
    
      
    """
    Reads and processes a 1/3 octave band data XLSX file from a LxT sonometer.
    Saves the processed statistics to the specified output folder.
    """


    # ---------------------------------------------------
    # 1- Reading CSVs
    # ---------------------------------------------------
    try:
        #Read of CSVs 
        None
    except Exception as e:
        logger.error(f"Error processing XLSX to CSV for {xlsx_path}: {e}")
        return
    
    # ---------------------------------------------------
    # 2- Processing Summary Sheet
    # ---------------------------------------------------
    try:

        df_summary = pd.DataFrame(columns=['Filename', 'sensor_id', 'Timestamp', 'Unixtimestamp'],index = [0])
        df = pd.read_csv(os.getcwd() + '/05_sonometer_process_test/temp_csvs/Summary.csv')
        df_summary['Filename'] = df['Unnamed: 1'][1]
        df_summary['sensor_id'] = df['Unnamed: 1'][2]
        df_summary['Timestamp'] = pd.to_datetime(df['Unnamed: 1'][11], format="%Y-%m-%d  %H:%M:%S")
        df_summary['Unixtimestamp'] = df_summary['Timestamp'].apply(lambda x: int(x.timestamp()))

    except Exception as e:
        logger.error(f"Error processing Summary sheet in {xlsx_path}: {e}")
        return
    # ---------------------------------------------------
    # 3- Processing Measurements Sheet
    # ---------------------------------------------------

    try:
        df_measurements = pd.DataFrame(columns=third_octaves)
        df = pd.read_csv(os.getcwd() + '/05_sonometer_process_test/temp_csvs/Measurement History.csv')
        df_measurements = df[third_octaves].iloc[0:]
        
    except Exception as e:
        logger.error(f"Error processing Measurement History sheet in {xlsx_path}, trying Time History Sheet: {e}")
        df_measurements = df_measurements = pd.read_csv(os.getcwd() + '/05_sonometer_process_test/temp_csvs/Time History.csv')

    except Exception as e:
        
        logger.error(f"Error processing Time History sheet in {xlsx_path}: {e}")
        
        return

    # ---------------------------------------------------
    # 3- Concatenating Summary and Measurements DataFrames, extending Filename and sensor_id columns,
    #    adapting timestamp and unixtimestamp to a minute frequency
    # ---------------------------------------------------

    try:

        df_final = pd.DataFrame(columns=df_summary.columns.tolist() + df_measurements.columns.tolist())
        df_final = pd.concat([df_summary, df_measurements], axis=1)
        df_final['Filename'] = df_summary['Filename'].fillna(df_summary['Filename'].dropna().iloc[0])
        df_final['sensor_id'] = df_summary['sensor_id'].fillna(df_summary['sensor_id'].dropna().iloc[0])

        first_idx = df_final['Timestamp'].first_valid_index()
        first_ts  = df_final.loc[first_idx, 'Timestamp']        
        aligned_start = first_ts.ceil('min')                # 15:33:49 -> 15:34:00
        n_rest = len(df_final) - first_idx - 1

        if n_rest > 0:
            df_final.loc[first_idx+1:, 'Timestamp'] = pd.date_range(
                start=aligned_start, periods=n_rest, freq='min'
            )

        df_final['Unixtimestamp'] = (
        df_final['Timestamp']
        .dt.tz_localize('Europe/Madrid', nonexistent='shift_forward', ambiguous='NaT')
        .dt.tz_convert('UTC')
        .view('int64') // 10**9
        )

    except Exception as e:

        logger.error(f"Error concatenating dataframes for {xlsx_path}: {e}")
        
        return
    # ---------------------------------------------------
    # 4- Saving final dataframe to CSV
    # ---------------------------------------------------
    try:

        point = file_path.split('/')[-2]
        name = point + '_processed.csv' if count == 0 else point + f'_processed_{count}.csv'

        df_final.to_csv(os.path.join(output_folder, name), index=False)

    except Exception as e:  
        logger.error(f"Error saving processed data for {xlsx_path}: {e}")
        return
    
    # ---------------------------------------------------
    # 5- Deleting temporary CSV files
    # ---------------------------------------------------
    try:

        subprocess.run(['rm', '-rf', os.getcwd() + '/05_sonometer_process_test/temp_csvs/'])

    except Exception as e:

        logger.error(f"Error deleting temporary CSV files: {e}")

        return



if __name__ == "__main__":
    logger = setup_logging("sonometer_process")

    logger.info("[SONOMETER] -> [Starting 1/3 Octave Band Data Processing from LxT CSV files]")

    path = SANDISK_PATH_LINUX

    for point_folder in os.listdir(path):
        if point_folder == "P2_CONTENEDORES":
            
            output_point_folder = os.path.join(path, point_folder,'sonometer_acoustics')
            points_folders = os.path.join(path, point_folder, "sonometer_files_test")
            
            for point in tqdm.tqdm(os.listdir(points_folders), desc="Processing Points"):
                    if point == 'Ribera 2_3':
                        count = 0
                        lxt_files = [f for f in os.listdir(os.path.join(points_folders,point)) if f.endswith('.xlsx')]
                        for file in tqdm.tqdm(lxt_files, desc=f"Processing files in {point}"):
                            
                            file_path = os.path.join(points_folders,point, file)
                            logger.info(f"[SONOMETER] -> Processing file: {file_path}")
                            process_one_third_octave_xlsx(file_path, output_point_folder,count)
                            count += 1
                            logger.info(f"[SONOMETER] -> Processed data saved at {output_point_folder}")


