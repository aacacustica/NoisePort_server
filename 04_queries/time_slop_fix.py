import os
import shutil
import tqdm

import pandas as pd

from config import * 
from logging_config import * 

logger = setup_logging('query_automatize')

def safe_read_timestamp_series(csv_path, nrows=10):
    """Read up to nrows timestamps from csv_path and return pd.Series."""
    try:
        df = pd.read_csv(csv_path, nrows=nrows)
    except Exception as e:
        logger.debug(f"safe_read_timestamp_series: error reading {csv_path}: {e}")
        return None
    if df.empty or 'Timestamp' not in df.columns:
        logger.debug(f"safe_read_timestamp_series: {csv_path} empty or missing 'Timestamp'.")
        return None
    ts = pd.to_datetime(df['Timestamp'], errors='coerce')
    if ts.isna().all():
        logger.debug(f"safe_read_timestamp_series: {csv_path} all Timestamps NaT after parsing.")
        return None
    return ts

def get_csv_first_valid_timestamp(csv_path):
    ts_series = safe_read_timestamp_series(csv_path, nrows=10)
    if ts_series is None:
        return None
    return ts_series.dropna().iloc[0]

def get_csv_last_valid_timestamp(csv_path):
    try:
        df = pd.read_csv(csv_path, usecols=['Timestamp'])
    except Exception as e:
        logger.debug(f"get_csv_last_valid_timestamp: error reading {csv_path}: {e}")
        return None
    if df.empty or 'Timestamp' not in df.columns:
        return None
    ts = pd.to_datetime(df['Timestamp'], errors='coerce').dropna()
    if ts.empty:
        return None
    return ts.iloc[-1]

def sort_csvs_by_content_timestamp(folder):
    csvs = [f for f in os.listdir(folder) if f.lower().endswith('.csv')]
    def _key(fname):
        ts = get_csv_first_valid_timestamp(os.path.join(folder, fname))
        return ts.timestamp() if ts is not None else float("inf")
    return sorted(csvs, key=_key)

def detect_minute_jump_by_content(prev_path, curr_path, threshold_seconds=70):
    tprev = get_csv_last_valid_timestamp(prev_path)
    tcurr = get_csv_first_valid_timestamp(curr_path)
    if tprev is None or tcurr is None:
        return False
    return (tcurr - tprev).total_seconds() > threshold_seconds

def get_extra_seconds_indices(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"get_extra_seconds_indices: error reading {csv_path}: {e}")
        return 0, []

    if df.empty or 'Timestamp' not in df.columns:
        return 0, []

    # Parsear a datetime (NaT donde no sea posible)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # Eliminar timezone sin cambiar la hora
    df['Timestamp'] = df['Timestamp'].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)

    # Evitar que un NaT rompa .dt
    timestamps_valid = df['Timestamp'].dropna()
    if timestamps_valid.empty:
        return 0, []

    official_minute = timestamps_valid.iloc[0].minute
    idxs = df[timestamps_valid.dt.minute != official_minute].index.tolist()
    return len(idxs), idxs

def append_extra_seconds(fixed_folder_path, prev_name, curr_name, row_indices):
    if not row_indices:
        return
    prev_path = os.path.join(fixed_folder_path, prev_name)
    curr_path = os.path.join(fixed_folder_path, curr_name)
    df_prev = pd.read_csv(prev_path)
    df_curr = pd.read_csv(curr_path)
    rows_to_move = df_prev.loc[row_indices].copy()
    df_prev = df_prev.drop(index=row_indices).reset_index(drop=True)
    df_curr = pd.concat([df_curr, rows_to_move], ignore_index=True).sort_values('Timestamp')
    df_prev.to_csv(prev_path, index=False)
    df_curr.to_csv(curr_path, index=False)
    logger.info(f"append_extra_seconds: moved {len(rows_to_move)} rows from {prev_name} -> {curr_name}")

def last_file_trim_overflow(last_csv_path):
    try:
        df = pd.read_csv(last_csv_path)
    except Exception as e:
        logger.error(f"last_file_trim_overflow: error reading {last_csv_path}: {e}")
        return pd.DataFrame()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    first_hour = df['Timestamp'].iloc[0].hour
    overflow_mask = df['Timestamp'].dt.hour != first_hour
    extra_rows = df[overflow_mask].copy()
    df[~overflow_mask].to_csv(last_csv_path, index=False)
    return extra_rows

def build_bucket_key_from_df_rows(rows_df):
    if rows_df.empty or 'Timestamp' not in rows_df.columns:
        return None, None
    ts0 = rows_df['Timestamp'].dropna().iloc[0]
    bucket_day = ts0.strftime('%Y%m%d')
    bucket_hour = ts0.hour
    return f"{bucket_day}_{bucket_hour:02d}", ts0

def append_leftover_rows_to_next_bucket(leftover_df, next_fixed_folder_path):
    """
    Prepend leftover_df to first CSV in next_fixed_folder_path, filtered by next hour.
    """
    if leftover_df.empty:
        return
    os.makedirs(next_fixed_folder_path, exist_ok=True)
    next_hour = int(os.path.basename(next_fixed_folder_path).split('_')[-1])
    
    # Convert leftover timestamps
    leftover_df['Timestamp'] = pd.to_datetime(leftover_df['Timestamp'], errors='coerce')
    rows_for_hour = leftover_df[leftover_df['Timestamp'].dt.hour == next_hour].copy()
    if rows_for_hour.empty:
        return

    csvs = sort_csvs_by_content_timestamp(next_fixed_folder_path)
    if not csvs:
        fname = f"generated_{rows_for_hour.iloc[0]['Timestamp'].strftime('%Y%m%d_%H%M%S')}_tflt_w_1.0.csv"
        rows_for_hour.to_csv(os.path.join(next_fixed_folder_path, fname), index=False)
        logger.info(f"append_leftover_rows_to_next_bucket: created {fname} with {len(rows_for_hour)} rows")
        return

    first_csv_path = os.path.join(next_fixed_folder_path, csvs[0])
    df_next = pd.read_csv(first_csv_path)

    # Ensure Timestamp is converted in df_next too!
    df_next['Timestamp'] = pd.to_datetime(df_next['Timestamp'], errors='coerce')
    rows_for_hour['Timestamp'] = pd.to_datetime(rows_for_hour['Timestamp'], errors='coerce')
    
    merged = pd.concat([rows_for_hour, df_next], ignore_index=True).sort_values('Timestamp').reset_index(drop=True)
    merged.to_csv(first_csv_path, index=False)
    logger.info(f"append_leftover_rows_to_next_bucket: prepended {len(rows_for_hour)} rows to {first_csv_path}")

def get_next_hour_bucket(bucket):
    day, hour = bucket.split('_')
    hour = int(hour)
    if hour < 23:
        return f"{day}_{hour+1:02d}"
    # rollover
    next_day = (pd.to_datetime(day) + pd.Timedelta(days=1)).strftime('%Y%m%d')
    return f"{next_day}_00"

def get_last_minute_leftovers(df):
    if df.empty or 'Timestamp' not in df.columns:
        return pd.DataFrame()

    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    # si todo es NaT → no se puede sacar el último minuto
    if df['Timestamp'].dropna().empty:
        return pd.DataFrame()

    last_minute = df['Timestamp'].dropna().iloc[-1].minute
    return df[df['Timestamp'].dt.minute == last_minute].copy()


def get_measurement_folders(point):
    point = point.replace('3-Medidas','5-Resultados')
    point_SPL = os.path.join(point,'SPL')
    point_AI = os.path.join(point,'AI_MODEL')

    acoustic_path  = os.path.join(point,'acoustic_params')
    prediction_path = os.path.join(point_AI,'predictions_litle')

    return acoustic_path,prediction_path


def get_bucket_list(measurement_path):
    return sorted([b for b in os.listdir(measurement_path)
                   if os.path.isdir(os.path.join(measurement_path, b))
                   and 'fixed' not in b and not b.endswith('.txt')])


def copy_original_csvs(bucket_path, fixed_folder, measurement_folder):
    
    if measurement_folder == 'predictions_litle':
        csv_files = [f for f in os.listdir(bucket_path) if f.endswith('w_1.0.csv')]
    else:
        csv_files = [f for f in os.listdir(bucket_path) if f.lower().endswith('.csv')]

    os.makedirs(fixed_folder, exist_ok=True)

    for fname in csv_files:
        dst = os.path.join(fixed_folder, fname)
        if not os.path.exists(dst):
            shutil.copy(os.path.join(bucket_path, fname), dst)

    return csv_files

def handle_minute_jumps(prev_path, curr_path, leftover_buckets, measurement_folder):
    if not detect_minute_jump_by_content(prev_path, curr_path):
        return

    df_prev = pd.read_csv(prev_path)
    df_prev['Timestamp'] = pd.to_datetime(df_prev['Timestamp'])

    prev_hour = df_prev['Timestamp'].iloc[0].hour
    leftover_rows = df_prev[df_prev['Timestamp'].dt.hour != prev_hour]

    if leftover_rows.empty:
        return

    # Eliminar leftovers del archivo anterior
    df_prev.drop(index=leftover_rows.index).to_csv(prev_path, index=False)

    # Agregar leftovers al dict con el bucket al que pertenecen
    bucket_key, _ = build_bucket_key_from_df_rows(leftover_rows)

    key = (bucket_key, measurement_folder)
    leftover_buckets.setdefault(key, pd.DataFrame())
    leftover_buckets[key] = pd.concat(
        [leftover_buckets[key], leftover_rows],
        ignore_index=True
    ).sort_values('Timestamp')

def handle_last_csv_leftovers(fixed_folder, bucket, leftover_buckets, measurement_folder):
    fixed_csvs = sort_csvs_by_content_timestamp(fixed_folder)
    if not fixed_csvs:
        return

    last_path = os.path.join(fixed_folder, fixed_csvs[-1])
    df_last = pd.read_csv(last_path)
    df_last['Timestamp'] = pd.to_datetime(df_last['Timestamp'], errors='coerce')

    minute_leftovers = get_last_minute_leftovers(df_last)
    if minute_leftovers.empty:
        return

    df_last.drop(index=minute_leftovers.index).to_csv(last_path, index=False)

    next_bucket = get_next_hour_bucket(bucket)
    if not next_bucket:
        return

    key = (next_bucket, measurement_folder)

    leftover_buckets.setdefault(key, pd.DataFrame())
    leftover_buckets[key] = pd.concat(
        [leftover_buckets[key], minute_leftovers],
        ignore_index=True
    ).sort_values('Timestamp')

def handle_already_fixed_pairs(processed_folder_txt,day_csv):
    if not os.path.exists(processed_folder_txt):
        open(processed_folder_txt, 'w').close()

    with open(processed_folder_txt, "r+") as myfile:
        content = myfile.read()

        if day_csv in content:
            return False
        
        myfile.seek(0, os.SEEK_END)
        myfile.write(day_csv + "\n")

        return True

def time_slop_fix(point,acoustic_folder,pred_litle_folder):

    measurement_folders = [acoustic_folder,pred_litle_folder]

    for measurement_folder in measurement_folders:
        if 'acoustic_params' in measurement_folder:
            processed_folders_txt_path = os.path.join(measurement_folder,'processed_acoustic.txt')
        elif 'predictions_litle' in measurement_folder:
            processed_folders_txt_path = os.path.join(measurement_folder,'processed_acoustic.txt')

        measurement_path = os.path.join(point, measurement_folder)
        leftover_buckets = {}

        buckets = get_bucket_list(measurement_path)

        for bucket in tqdm.tqdm(buckets, desc=f'Fixing time slops {measurement_folder}'):
            bucket_path = os.path.join(measurement_path, bucket)
            fixed_folder = os.path.join(measurement_path, f'fixed_{bucket}')

            csv_files = copy_original_csvs(bucket_path, fixed_folder, measurement_folder)
            if not csv_files:
                continue

            # Ordenar CSVs por contenido temporal
            fixed_csvs = sort_csvs_by_content_timestamp(fixed_folder)

            # Procesar pares consecutivos
            for prev_name, curr_name in zip(fixed_csvs, fixed_csvs[1:]):
                prev_path = os.path.join(fixed_folder, prev_name)
                curr_path = os.path.join(fixed_folder, curr_name)

                if 'acoustic_params' in measurement_folder:
                    processed_folders_txt_path = os.path.join(measurement_folder,'processed_acoustic.txt')
                elif 'predictions_litle' in measurement_folder:
                    processed_folders_txt_path = os.path.join(measurement_folder,'processed_acoustic.txt')

                if not handle_already_fixed_pairs(processed_folders_txt_path,prev_name):
                    continue
                
                handle_minute_jumps(prev_path, curr_path, leftover_buckets, measurement_folder)

                _, extra_idx = get_extra_seconds_indices(prev_path)
                if extra_idx:
                    append_extra_seconds(fixed_folder, prev_name, curr_name, extra_idx)

            # Último archivo del bucket
            handle_last_csv_leftovers(fixed_folder, bucket, leftover_buckets, measurement_folder)

        # Distribuir TODOS los leftovers acumulados
        for (bucket_key, m_folder), leftover_df in leftover_buckets.items():
            next_folder = os.path.join(point, m_folder, f"fixed_{bucket_key}")
            os.makedirs(next_folder, exist_ok=True)
            append_leftover_rows_to_next_bucket(leftover_df, next_folder)

