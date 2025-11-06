import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import os
import argparse
import logging
import sys

sys.path.insert(0, "/home/aac_s3_test/noisePort_server/05_peak")

from logging_config import *
from config_peak import *
from scipy.signal import find_peaks
from tqdm import tqdm
from config import *


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    filename='peak_detection.log', 
                    filemode='w')



def leq(levels):
    levels = levels[~np.isnan(levels)]
    l = np.array(levels)
    return 10 * np.log10(np.mean(np.power(10, l / 10)))


def find_acoustics_folder(base_path):

    csv_files = []
    for point in os.listdir(base_path):
        if point == 'P5_TEST':
            for acoustic_folder in os.listdir(os.path.join(base_path,point)):
                if acoustic_folder == 'acoustic_params':
                    for day in os.listdir(os.path.join(base_path,point,acoustic_folder)):
                        
                        if not os.path.isdir(os.path.join(base_path,point,acoustic_folder,day)):
                            continue
                        csv_files_day = [f for f in os.listdir(os.path.join(base_path,point,acoustic_folder,day)) if f.endswith('.csv')]
                        csv_files = csv_files + [os.path.join(base_path,point,acoustic_folder,day,f) for f in csv_files_day]
                    
    
    return csv_files



def assign_folder_paths(csv_file):

    title = csv_file.split("/")[-1]
    point = csv_file.split("/")[-4]

    output_folder = csv_file.replace("3-Medidas/" + point +"/acoustic_params","5-Resultados/" + point +"/SPL/PEAKS")
    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    
    return title,point,output_folder


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", type=str, required=False, help="Path to the csv file")
    return parser.parse_args()



def main():

    #----------------------------------------------------------------------------------------------------
    #
    # python .\peak_detection_L50.py -p "\\192.168.205.117\AAC_Server\PUERTOS\NOISEPORT\20231211_SANTUR"
    # 
    #----------------------------------------------------------------------------------------------------

    logger = setup_logging('query_automatize')
    args = argument_parser()

    if not args.path:
        base_path = SANDISK_PATH_LINUX
    else:
        base_path = args.path
    
    """
                                        temporary debugging pathing
               ->   TODO assign point folder iterations in find_audiomoth_folders function  <-
    """
    csv_files = list(find_acoustics_folder(base_path))

    for csv_file in tqdm(csv_files, desc='Processing csv files'):
        
        df = pd.read_csv(csv_file)


                #-------------------------------
                # 1- Getting folder paths
                #-------------------------------
        
        title, point, output_folder = assign_folder_paths(csv_file)

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

                #-------------------------------
                # 2- Asigning full path to each filename item in filename row
                #------------------------------- 
        
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        #df['Filename'] = df['Filename'].apply(lambda x: os.path.join(acoustic_folder)
        try:
            
                #-------------------------------
                # 3- Rolling median for the LA values with a window of 30 seconds
                #-------------------------------
            
            df['LA_median'] = df['LA'].rolling(window=WINDOW_SIZE, min_periods=1).quantile(0.5) + ADDING_THRESHOLD
            above_threshold = df[df['LA'] > df['LA_median']]
        
        except Exception as e:
            logger.error(f"Error rolling median for LA values:{e}")
        
        
        if not above_threshold.empty:
            
            
            try:

                #-------------------------------
                # 4- Finding peaks which surpass the LA median above threshold
                #-------------------------------

                clip_info = []
                peaks, properties = find_peaks(above_threshold['LA'], prominence=PROMINENCE, width=WIDTH)
                df_peaks = above_threshold.iloc[peaks]
                
                logging.info(f"Detected {len(df_peaks)} peaks")
            
            
            except Exception as e:
                logger.error(f"Error finding peaks:{e}")
        
            try:

                #-------------------------------
                # 5- Getting durations
                #-------------------------------

                start_points = properties['left_ips'].astype(int)
                end_points = properties['right_ips'].astype(int)
                durations = end_points - start_points

            except:
                
                logger.error(f"Error calculating durations: {e}")

            try:

                #-------------------------------
                # 6-  Saving Peaks Dataframe to CSV
                #-------------------------------

                peak_data = []
                
                for start, end in zip(start_points, end_points):
                    peak_LA_values = above_threshold['LA'].iloc[start:end+1].values
                    leq_value = leq(peak_LA_values)
                    peak_data.append({
                        'filename': above_threshold['filename'].iloc[start],
                        'start_time': above_threshold['date'].iloc[start],
                        'end_time': above_threshold['date'].iloc[end],
                        'duration': end - start,
                        'leq': leq_value.round(2),
                        'LA_values': peak_LA_values.tolist()
                    })

                peaks_df = pd.DataFrame(peak_data)
                peaks_df.to_csv(os.path.join(output_folder, f"peaks_detection_{title}.csv"), index=False)
                logging.info(f"Peaks saved at {output_folder} as peaks_detection_{title}.csv")
            
            
            except Exception as e:
                logger.error(f"Error saving peak csv: {e}")


            try:
                              
                #-------------------------------
                # 7-  Getting mean and mean rounded from durations and finishing
                #-------------------------------

                mean = np.mean(durations)
                mean_rounded = np.round(mean, 2)
                logging.info("")
                logging.info(f"Average duration: {mean_rounded} seconds")
                logging.info(f"Max duration: {np.max(durations)} seconds")
                logging.info(f"Min duration: {np.min(durations)} seconds")

            except Exception as e:
                logger.error(f"Error getting mean and mean rounded from durations: {e}")
        else:
            logging.error("No peaks detected")


if __name__ == "__main__":
    main()