import numpy as np
import pandas as pd


def calculate_duration(start_time, end_time):
    duration = end_time - start_time
    return duration.total_seconds()


def evaluation_period_str(hour_column):
    period = ''
    if hour_column >= 7 and hour_column < 19:
        period = 'Ld'
    elif hour_column >= 19 and hour_column < 23:
        period = 'Le'
    else:
        period = 'Ln'
    return period


def evaluation_period_str_valencia(hour_column):
    period = ''
    if hour_column >= 8 and hour_column < 22:
        period = 'Ld_valencia'
    else:
        period = 'Ln_valencia'
    return period


def add_night_column(hour_column, day_col):
    night_list=["Lunes-Martes","Martes-Miércoles","Miércoles-Jueves","Jueves-Viernes","Viernes-Sábado","Sábado-Domingo","Domingo-Lunes"]
    night = ''
    if hour_column >= 23:
        night=night_list[day_col]
    elif hour_column < 7:
        night=night_list[day_col-1]
    return night


def add_datetime_columns(df,logging, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    #df['day_hour'] = df.apply(lambda x: str(x[date_col].day) + '-' + str(x[date_col].hour),axis=1)
    if df[date_col].dtype == 'datetime64[ns]':
        df['date'] = df[date_col].dt.date
        df['day'] = df[date_col].dt.day
        df['hour'] = df[date_col].dt.hour
        df['weekday'] = df[date_col].dt.weekday
        df['day_name'] = df[date_col].dt.day_name()
    else:
        logging.error(f"Failed to convert {date_col} to datetime in some rows.")
    #df['min_sec_str'] = df.apply(lambda x: datetime.datetime.strftime(x[date_col],'%M:%S'),axis=1)
    #df['min_sec_15_str'] = df.apply(lambda x: str(x[date_col].minute % 15) + '-'+str(x[date_col].second),axis=1)
    return df


def add_datetime_columns_pred(df,logging, date_col):
    logging.info(f"Adding datetime columns to {date_col}...")
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    if df[date_col].dtype == 'datetime64[ns]':
        df['datetime'] = df[date_col].dt.date
        df['day'] = df[date_col].dt.day
        df['hour'] = df[date_col].dt.hour
        df['weekday'] = df[date_col].dt.weekday
        df['day_name'] = df[date_col].dt.day_name()
    else:
        logging.error(f"Failed to convert {date_col} to datetime in some rows.")

    return df


def db_limit(hour_column,ld_limit,le_limit,ln_limit):
    limit = 0
    if hour_column >= 7 and hour_column < 19:
        limit = ld_limit
    elif hour_column >= 19 and hour_column < 23:
        limit = le_limit
    else:
        limit = ln_limit
    return limit


def categorize_time_of_day(hour):
    if 7 <= hour < 19:
        return 'Ld'
    elif 19 <= hour < 23:
        return 'Le'
    else:
        return 'Ln'
    

def categorize_time_of_day_4(hour):
    if 7 <= hour < 11:
        return 'Ld_1'
    elif 11 <= hour < 15:
        return 'Ld_2'
    elif 15 <= hour < 19:
        return 'Ld_3'
    elif 19 <= hour < 23:
        return 'Le'
    elif 23 <= hour or hour < 3:
        return 'Ln_1'
    elif 3 <= hour < 7:
        return 'Ln_2'


def leq(levels):
    levels = levels[~np.isnan(levels)]
    l = np.array(levels)
    return 10*np.log10(np.mean(np.power(10,l/10)))


def get_day_levels(df,laeq_column):
    df['indicador_str'] = df.apply(lambda x: evaluation_period_str(x['hour']),axis=1)
    indicadores = df.groupby('indicador_str').agg({laeq_column:[leq]}).round(1)
    return indicadores
    

def get_day_levels_valencia(df,laeq_column):
    df['indicador_valencia'] = df.apply(lambda x: evaluation_period_str_valencia(x['hour']),axis=1)
    indicadores = df.groupby('indicador_valencia').agg({laeq_column:[leq]}).round(1)
    return indicadores




def remove_unnamed_columns(df_preds):
    df_preds = df_preds.loc[:, ~df_preds.columns.str.contains('^Unnamed')]
    df_preds = df_preds.drop(columns=['Brown_Level_1'])
    df_preds = df_preds.drop(columns=['index'])
    return df_preds


def yamnet_class_map_csv():
    yammnet_class_map = "yamnet_class_AAC_v3_0.csv" 
    df_audioset = pd.read_csv(yammnet_class_map,sep=';')
    df_audioset = remove_unnamed_columns(df_audioset)
    return df_audioset


def taxonomy_json():
    urban_taxonomy_map = pd.read_json("urban_taxonomy_map_v1_0.json", typ='series').to_dict()
    port_taxonomy_map = pd.read_json("port_taxonomy_map_v1.0.json", typ='series').to_dict()
    return urban_taxonomy_map, port_taxonomy_map


def prediction_csv(path_input):
    df_prediction = pd.read_csv(path_input, converters={'class': eval, 'probability': eval})
    columns_to_check = ["classes_custom", "probabilities_custom", "sum_probs_custom", "sum_probs_original"]
    
    for col in columns_to_check:
        if col in df_prediction.columns:
            df_prediction = df_prediction.drop(col, axis=1)
            
    # columns to rename
    columns_to_rename = ["classes_original", "probabilities_original"]
    new_columns = ["classes", "probabilities"]
    
    for i in range(len(columns_to_rename)):
        if columns_to_rename[i] in df_prediction.columns:
            df_prediction = df_prediction.rename(columns={columns_to_rename[i]: new_columns[i]})

    return df_prediction


def insert_dates(df):
    df["year"] = df.index.year
    df["month"] = df.index.month
    df["day"] = df.index.day
    df["hour"] = df.index.hour
    df["minute"] = df.index.minute
    df["second"] = df.index.second
    df["weekday"] = df.index.day_name()

    weekday_translation = {
        "Monday": " Lunes",
        "Tuesday": " Martes",
        "Wednesday": " Miércoles",
        "Thursday": " Jueves",
        "Friday": " Viernes",
        "Saturday": " Sábado",
        "Sunday": " Domingo"
    }
    df["weekday"] = df["weekday"].replace(weekday_translation)
    df["weekday"] = df["weekday"].astype(str)
    df["day"] = df["day"].astype(str).str.zfill(2)
    df["fullday"] = df["day"] + df["weekday"]
    return df


def remove_row_out_timespan(df_LAeq, df_Pred):
    df_LAeq.index = pd.to_datetime(df_LAeq.index)
    df_Pred['datetime'] = pd.to_datetime(df_Pred['datetime'])
    start_date = df_LAeq.index.min()
    end_date = df_LAeq.index.max()
    df_Pred_filtered = df_Pred[(df_Pred['datetime'] >= start_date) & (df_Pred['datetime'] <= end_date)]
    
    return df_Pred_filtered



def apply_db_correction(df, coefficient):
    if 'LA' in df.columns:
        df['LA_corrected'] = df['LA'] - coefficient
        df['LAmax_corrected'] = df['LAmax'] - coefficient
        df['LAmin_corrected'] = df['LAmin'] - coefficient
    
    elif 'LAeq' in df.columns:
        df['LA_corrected'] = df['LAeq'] - coefficient
        df['LAmax_corrected'] = df['LAFmax'] - coefficient
        df['LAmin_corrected'] = df['LAFmin'] - coefficient
    elif 'Value' in df.columns:
        df['LA_corrected'] = df['Value'] - coefficient
    else:
        print('No column found to apply the correction')
    return df