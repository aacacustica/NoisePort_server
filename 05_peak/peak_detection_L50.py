import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import os
import argparse
import logging
import sys
import time
import re

sys.path.insert(0, "/home/martin/NoisePort_server/05_peak")

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


def _extract_key_from_filename(path: str):
    """
    Extrae la clave YYYYMMDD_HH del nombre de fichero.
    """
    name = os.path.basename(path)
    m = re.search(r'(\d{8}_\d{2})', name)
    return m.group(1) if m else None

def _to_datetime_no_tz(series: pd.Series):
    """
    Convierte a datetime y elimina timezone si existe.
    """
    series = pd.to_datetime(series, errors='coerce')
    # si es tz-aware, convertir a naive
    if pd.api.types.is_datetime64tz_dtype(series.dtype):
        series = series.dt.tz_convert(None)
    return series

def leq(levels):
    levels = levels[~np.isnan(levels)]
    l = np.array(levels)
    return 10 * np.log10(np.mean(np.power(10, l / 10)))



def get_hourly_folders(point):
    hour_path_acoustics = []
    hour_path_predictions = []
    hour_path_peaks = []
    

            
    spl_folder = os.path.join(RESULTADOS_FOLDER_NEW,point,'SPL')
    ai_folder = os.path.join(RESULTADOS_FOLDER_NEW,point,'AI_MODEL')
    
    predictions_params_query = os.path.join(ai_folder,PREDICTIONS_QUERY)
    peaks_params_query = os.path.join(spl_folder,'queries',PEAKS_QUERY)
    acoustic_params_query = os.path.join(spl_folder,'queries',ACOUSTICS_QUERY)

    if not os.path.exists(peaks_params_query): os.makedirs(peaks_params_query)

    for file in os.listdir(acoustic_params_query):
        if file.endswith('.csv') and 'fixed' in file:
            hour_path_acoustics.append(os.path.join(acoustic_params_query,file))        
    for file in os.listdir(predictions_params_query):
        if file.endswith('.csv') and 'fixed':
            hour_path_predictions.append(os.path.join(predictions_params_query,file))
    for file in os.listdir(peaks_params_query):
        if file.endswith('.csv') and 'fixed' in file:
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
    point = csv_file.split("/")[-5]

    output_folder = csv_file.replace(ACOUSTICS_QUERY,PEAKS_QUERY)

    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    output_folder = output_folder.replace(output_folder.split("/")[-1],"")
    
    return title,point,output_folder



def merge_acoustics_predictions_and_peaks(point,
                                          acoustics_paths,
                                          predictions_paths,
                                          peaks_paths,
                                          logger):
    """
    Refactor de la función para emparejar archivos por clave horaria (YYYYMMDD_HH),
    procesar todas las horas donde existan ACÚSTICA y PREDICCIÓN, y aplicar peaks
    cuando existan para esa hora.

    Devuelve la lista de archivos generados (paths).
    """
    base_path = RESULTADOS_FOLDER_NEW
    point_path = os.path.join(base_path, point)
    output_path = os.path.join(point_path, 'SPL', 'peaks', MERGED_QUERY)

    # Filtrado inicial (el tag de fixed se propaga en toda la cadena y llega hasta peaks también)
    peaks_paths = [f for f in peaks_paths if 'fixed' in f]
    predictions_paths = [f for f in predictions_paths if 'fixed' in f ]
    acoustics_paths = [f for f in acoustics_paths if 'fixed' in f]

    # Indexar por clave YYYYMMDD_HH
    ac_dict = {}
    for f in acoustics_paths:
        k = _extract_key_from_filename(f)
        if k:
            ac_dict[k] = f
        else:
            logger.warning(f"Unable to extract key from acoustic file: {f}")

    pr_dict = {}
    for f in predictions_paths:
        k = _extract_key_from_filename(f)
        if k:
            pr_dict[k] = f
        else:
            logger.warning(f"Unable to extract key from prediction file: {f}")

    pk_dict = {}
    for f in peaks_paths:
        k = _extract_key_from_filename(f)
        if k:
            pk_dict.setdefault(k, []).append(f)
        else:
            logger.warning(f"Unable to extract key from peaks file: {f}")

    # Aseguramos salida
    os.makedirs(output_path, exist_ok=True)

    # Iterar sobre las horas donde existan acoustics Y predictions
    all_keys = sorted(set(ac_dict.keys()) & set(pr_dict.keys()))
    logger.info(f"Processing {len(all_keys)} hours (acoustic+prediction pairs). "
                f"{len(pk_dict)} keys with peaks available.")

    generated_files = []

    for key in all_keys:
        acoustic_path = ac_dict[key]
        pred_path = pr_dict[key]
        peak_files_for_key = pk_dict.get(key, [])  # lista (posiblemente vacía)

        try:
            # Lectura
            df_ac = pd.read_csv(acoustic_path)
            df_pr = pd.read_csv(pred_path)
        except Exception as e:
            logger.exception(f"Failed reading acoustic/prediction files for key {key}: {e}")
            continue

        if '20251212' in acoustic_path:
            logger.debug(f"Read files for {key}: acoustics={acoustic_path}, predictions={pred_path}, peaks={peak_files_for_key or 'NONE'}")
        
        # Normalizar Timestamp en ambos DF
        if 'Timestamp' not in df_ac.columns or 'Timestamp' not in df_pr.columns:
            logger.error(f"Missing 'Timestamp' column for key {key}. Skipping.")
            continue

        df_ac['Timestamp'] = _to_datetime_no_tz(df_ac['Timestamp'])
        df_pr['Timestamp'] = _to_datetime_no_tz(df_pr['Timestamp'])

        # Drop NaT timestamps
        n_ac_nat = df_ac['Timestamp'].isna().sum()
        n_pr_nat = df_pr['Timestamp'].isna().sum()
        if n_ac_nat > 0 or n_pr_nat > 0:
            logger.warning(f"{key}: Dropping {n_ac_nat} NaT rows from acoustics and {n_pr_nat} from predictions.")
            df_ac = df_ac.dropna(subset=['Timestamp'])
            df_pr = df_pr.dropna(subset=['Timestamp'])

        # Merge acústica + predicción por Timestamp (inner)
        try:
            df_merged = pd.merge(df_ac, df_pr, on='Timestamp', how='inner', suffixes=('_acoustic', '_prediction'))
        except Exception as e:
            logger.exception(f"Merge failed for key {key}: {e}")
            continue

        if df_merged.empty:
            logger.info(f"{key}: merged acoustics+predictions is empty. Will still write file (empty) to keep traceability.")
        else:
            logger.debug(f"{key}: merged shape {df_merged.shape}")

        # Preparar columna is_peak
        df_final = df_merged.copy()
        df_final['is_peak'] = False

        # Si hay ficheros de peaks para esta clave, concatenarlos y normalizarlos
        if peak_files_for_key:
            try:
                # leer y concatenar todos los peaks del mismo key (si hay varios)
                list_pk = []
                for pk_file in peak_files_for_key:
                    df_pk_tmp = pd.read_csv(pk_file)
                    # estandarizar nombres de columnas y parseo de tiempos
                    df_pk_tmp.rename(columns={'start time': 'start_time', 'end time': 'end_time'}, inplace=True)
                    if 'start_time' not in df_pk_tmp.columns or 'end_time' not in df_pk_tmp.columns:
                        logger.warning(f"{pk_file} missing start_time/end_time columns. Skipping this peaks file.")
                        continue
                    df_pk_tmp['start_time'] = pd.to_datetime(df_pk_tmp['start_time'], errors='coerce')
                    df_pk_tmp['end_time'] = pd.to_datetime(df_pk_tmp['end_time'], errors='coerce')
                    # descartamos filas inválidas
                    df_pk_tmp = df_pk_tmp.dropna(subset=['start_time', 'end_time'])
                    list_pk.append(df_pk_tmp)
                if list_pk:
                    df_pk = pd.concat(list_pk, ignore_index=True)
                    # Opcional: ordenar por start_time
                    df_pk = df_pk.sort_values('start_time').reset_index(drop=True)
                else:
                    df_pk = None
            except Exception as e:
                logger.exception(f"{key}: failed reading/concat peaks: {e}")
                df_pk = None
        else:
            df_pk = None

        # Aplicar merge_peaks sólo si tenemos df_pk
        if df_pk is not None and not df_pk.empty:
            try:
                df_with_peaks = merge_peaks(df_pk, df_final)
            except Exception as e:
                logger.exception(f"{key}: merge_peaks failed: {e}. Continuing with is_peak=False.")
                df_with_peaks = df_final
        else:
            df_with_peaks = df_final

        # Guardar CSV 
        merged_filename = os.path.join(output_path, f"merged_{key}.csv")
        try:
            df_with_peaks.to_csv(merged_filename, index=False)
            generated_files.append(merged_filename)
            logger.info(f"Saved merged file for {key} -> {merged_filename}")
        except Exception as e:
            logger.exception(f"Failed saving merged file for {key}: {e}")

    logger.info(f"Processing finished. Generated {len(generated_files)} files in {output_path}")
    return generated_files




def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--point", type=str, required=False, help="Point to process")
    return parser.parse_args()



def main():
    # python -m 05_peak.peak_detection_L50
    
    logger = setup_logging('peak_detection')
    args = argument_parser()

    point_to_process = args.point
    logging.info(f"Inizializing")

    """
    if not args.path:
        base_path = SANDISK_PATH_LINUX
    else:
        base_path = args.path
    """

    ################
    # inizializating
    ################
    hourly_acoustics_folders,hourly_predictions_folders,_ = list(get_hourly_folders(point_to_process))
    
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
        hourly_acoustics_folders,hourly_predictions_folders,hourly_peaks_folders = list(get_hourly_folders(point_to_process))
        merge_acoustics_predictions_and_peaks(point,hourly_acoustics_folders,hourly_predictions_folders,hourly_peaks_folders,logger)
    
    except Exception as e:
        logger.error(f"Error concatenating acoustics predictions and peaks: {e}")


if __name__ == "__main__":
    main()