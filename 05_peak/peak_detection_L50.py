import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import os
import argparse
import logging
import sys
import time
sys.path.insert(0, "/home/aac_s3_test/noisePort_server/05_peak")

from logging_config import *
from config_peak import *
from scipy.signal import find_peaks
from collections import defaultdict
from tqdm import tqdm
from config import *
from config_peak import *


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    filename='peak_detection.log', 
                    filemode='w')



def leq(levels):
    levels = levels[~np.isnan(levels)]
    l = np.array(levels)
    return 10 * np.log10(np.mean(np.power(10, l / 10)))



def get_hourly_folders():

    hour_path_acoustics = []
    hour_path_predictions = []
    hour_path_peaks = []
    
    for point in os.listdir(RESULTADOS_FOLDER):
        if point == 'P5_TEST':
            
            spl_folder = os.path.join(RESULTADOS_FOLDER,point,'SPL')
            ai_folder = os.path.join(RESULTADOS_FOLDER,point,'AI_MODEL')
            
            predictions_params_query = os.path.join(ai_folder,PREDICTIONS_QUERY)
            peaks_params_query = os.path.join(spl_folder,PEAKS_QUERY)
            acoustic_params_query = os.path.join(spl_folder,ACOUSTICS_QUERY)

            if not os.path.exists(peaks_params_query): os.makedirs(peaks_params_query)

            for file in os.listdir(acoustic_params_query):
                if file.endswith('.csv') and 'fixed' in file:
                    hour_path_acoustics.append(os.path.join(acoustic_params_query,file))        
            for file in os.listdir(predictions_params_query):
                if file.endswith('.csv') and 'fixed' in file:
                    hour_path_predictions.append(os.path.join(predictions_params_query,file))
            for file in os.listdir(peaks_params_query):
                if file.endswith('.csv'):
                    hour_path_peaks.append(os.path.join(peaks_params_query,file))
                    
    return hour_path_acoustics,hour_path_predictions,hour_path_peaks




def merge_peaks(df_pk: pd.DataFrame, df_final: pd.DataFrame) -> pd.DataFrame:
    # Ordenar por tiempo
    df_final = df_final.sort_values("Timestamp").reset_index(drop=True)
    df_pk = df_pk.sort_values("start_time").reset_index(drop=True)

    # Crear IntervalIndex
    intervals = pd.IntervalIndex.from_arrays(df_pk["start_time"], df_pk["end_time"], closed="both")
    
    intervals = pd.IntervalIndex.from_arrays(
    intervals.left.tz_localize(None),
    intervals.right.tz_localize(None),
    closed=intervals.closed
    )
    # Inicializar DataFrame con NaNs para los picos
    df_picos_matched = pd.DataFrame(pd.NA, index=df_final.index, columns=df_pk.columns)

    # Iterar sobre cada timestamp y asignar pico correspondiente

    for i, ts in enumerate(df_final["Timestamp"]):
        ts = ts.tz_localize(None)
        matches = df_pk[intervals.contains(ts)]
        if not matches.empty:
            # Tomamos solo la primera coincidencia
            df_picos_matched.iloc[i] = matches.iloc[0]
            df_final.at[i,'is_peak'] = True

    # Concatenar resultados con prefijo
    df_final = pd.concat([df_final, df_picos_matched.add_prefix("peak_")], axis=1)

    return df_final



def assign_folder_paths(csv_file):
    title = csv_file.split("/")[-1]
    point = csv_file.split("/")[-4]

    output_folder = csv_file.replace(ACOUSTICS_QUERY,PEAKS_QUERY)

    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    
    return title,point,output_folder



def merge_acoustics_predictions_and_peaks(point,acoustics_paths,predictions_paths,peaks_paths,logger):
    base_path = RESULTADOS_FOLDER
    point_path = os.path.join(base_path,point)
    spl_path = os.path.join(point_path,'SPL')
    output_path = os.path.join(spl_path,MERGED_QUERY)

    peaks_paths = [f for f in peaks_paths if 'fixed' in f]
    predictions_paths = [f for f in predictions_paths if 'fixed' in f]
    acoustics_paths = [f for f in acoustics_paths if 'fixed' in f]
    peaks_paths = sorted(peaks_paths)
    predictions_paths = sorted(predictions_paths)
    acoustics_paths = sorted(acoustics_paths)
   
    for acoustic_path,pred_path,peak_path in zip(acoustics_paths,predictions_paths,peaks_paths):
            df_pk = pd.read_csv(peak_path)
            df_pr = pd.read_csv(pred_path)
            df_ac = pd.read_csv(acoustic_path)

            logger.info("Merged individual csv files into single dataframes for acoustics, predictions and peaks")

            #1 -  Comprobamos que la columna timestamp tiene el mismo formato en los 2 dataframes que la contienen para nombre y contenido
            df_ac['Timestamp'] = pd.to_datetime(df_ac['Timestamp'])
            df_pr['Timestamp'] = pd.to_datetime(df_pr['Timestamp'])

            #2 - Normalizar peaks
            df_pk.rename(columns={'start time': 'start_time', 'end time': 'end_time'}, inplace=True)
            df_pk['start_time'] = pd.to_datetime(df_pk['start_time'])
            df_pk['end_time'] = pd.to_datetime(df_pk['end_time'])
            
            #2 - Hacemos merge de acoustics y predictions ya que ambos tienen 1 registro por segundo y es mergeable a pelo
            df_pr['Timestamp'] = df_pr['Timestamp'].dt.tz_convert(None)
            df_ac['Timestamp'] = df_ac['Timestamp'].dt.tz_localize(None)
            df_merged = pd.merge(df_ac,df_pr,on='Timestamp',how='inner',suffixes=('_acoustic','_prediction'))

            #3 - Agregamos las columnas de peaks segun intervalo de tiempo
            df_final = df_merged.copy()
            df_final['is_peak'] = False
            

            #4 - Iteramos sobre peaks y agregamos dato en caso de que entre en el rango del pico
            df_merged = merge_peaks(df_pk,df_final)
            merged_filename = os.path.join(output_path, f"merged_{os.path.basename(acoustic_path)}")
            if not os.path.exists(output_path): os.makedirs(output_path)
            df_merged.to_csv(merged_filename, index=False)           
            
            logger.info(f"Merged acoustics, predictions and peaks saved at {output_path}")




def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", type=str, required=False, help="Path to the csv file")
    return parser.parse_args()



def main():
    # python -m 05_peak.peak_detection_L50
    
    logger = setup_logging('peak_detection')
    args = argument_parser()
    logging.info(f"Inizializing")


    if not args.path:
        base_path = SANDISK_PATH_LINUX
    else:
        base_path = args.path
    

    ################
    # inizializating
    ################
    hourly_acoustics_folders,hourly_predictions_folders,_ = list(get_hourly_folders())
    for csv_file in tqdm(hourly_acoustics_folders, desc='Processing csv files'):
        df = pd.read_csv(csv_file)


        #folder paths
        title, point, output_folder = assign_folder_paths(csv_file)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])


        ################
        # PEAK ANALYSIS
        ################
        try:
            # dynamic median for the LA values with a window of 30 seconds
            df['LA_median'] = df['LA'].rolling(window=WINDOW_SIZE, min_periods=1).quantile(0.5) + ADDING_THRESHOLD
            above_threshold = df[df['LA'] > df['LA_median']]
        

            if not above_threshold.empty:
                #########
                # find peaks
                peaks, properties = find_peaks(above_threshold['LA'], prominence=PROMINENCE, width=WIDTH)
                df_peaks = above_threshold.iloc[peaks]
                logging.info(f"Detected {len(df_peaks)} peaks")

                # duration
                start_points = properties['left_ips'].astype(int)
                end_points = properties['right_ips'].astype(int)
                durations = end_points - start_points
            
                #########
                # CREATING CSV
                peak_data = []
                for start, end in zip(start_points, end_points):
                    peak_LA_values = above_threshold['LA'].iloc[start:end+1].values
                    leq_value = leq(peak_LA_values)
                    peak_data.append({
                        'filename': above_threshold['Filename'].iloc[start],
                        'start_time': above_threshold['Timestamp'].iloc[start],
                        'end_time': above_threshold['Timestamp'].iloc[end],
                        'duration': int(end - start),
                        'leq': round(leq_value, 1),
                        'LA_values': peak_LA_values.tolist()
                    })

                #########
                # SAVING
                peaks_df = pd.DataFrame(peak_data)
                output_file_name = os.path.join(output_folder, f"peaks_detection_{title}.csv") 
                peaks_df.to_csv((output_file_name), index=False)
                logging.info(f"Peaks saved at {output_file_name}")
            
            else:
                logging.info("No peaks detected!")
            
        except Exception as e:
            logger.error(f"Error saving peak csv: {e}")
        

    ################
    # CONCAT AND SAVE
    ################
    try:
        hourly_acoustics_folders,hourly_predictions_folders,hourly_peaks_folders = list(get_hourly_folders())
        merge_acoustics_predictions_and_peaks(point,hourly_acoustics_folders,hourly_predictions_folders,hourly_peaks_folders,logger)
    
    except Exception as e:
        logger.error(f"Error concatenating acoustics predictions and peaks: {e}")


if __name__ == "__main__":
    main()