from datetime import datetime
import os
import pandas as pd



def get_data_bilbo(filename: str):
    df = pd.read_csv(filename)
    try:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='raise')
    except KeyError:
        print("No 'datetime' column found in CSV.")
    except pd.errors.OutOfBoundsDatetime:
        print("Error converting 'datetime' column.")
    return df



def get_data_814(filename: str):
    try:
        df = pd.read_csv(filename, header=16, encoding='latin1')
    except UnicodeDecodeError:
        df = pd.read_csv(filename, header=16)
   
    if "Leq" not in df.columns:
        df = pd.read_csv(filename, header=19, sep=';', encoding='latin1')
    df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    return df



def get_data_lx_ES(filename: str):
    df = pd.read_excel(filename, sheet_name='Historia del tiempo')
    df['datetime'] = pd.to_datetime(df['Fecha'])
    # add a day beacuse there is a bug in the data
    # df['datetime'] = df['datetime'] + pd.DateOffset(days=1)
    return df



def get_data_lx_EN(filename: str):
    df = pd.read_excel(filename,sheet_name=4)
    df['datetime'] = pd.to_datetime(df['Date'])
    return df



def get_data_824(filename: str):
    df = pd.read_csv(filename, sep=',', encoding='latin1', header=15)
    df = df.dropna(axis=1)
    
    if "Leq" not in df.columns:
        df = pd.read_csv(filename,header=15, sep=',')
    
    df['datetime'] = pd.to_datetime(df['Date'] + ' '+ df['Time'])
    return df



def get_data_SV307(filename: str):
    try:
        df = pd.read_csv(filename,header=14,sep=';',skipfooter=8,usecols=[0,1,2,3,4,5,6,7,8], engine='python')
    except Exception as e:
        df = pd.read_csv(filename,header=18,skipfooter=8,usecols=[0,1,2,3,4,5,6,7,8], engine='python')
    
    if not 'LAeq (Ch1, P1) [dB]' in df.columns:
        df = pd.read_csv(filename,header=18,skipfooter=8,usecols=[0,1,2,3,4,5,6,7,8], engine='python', sep=';')

    df = df[pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M:%S', errors='coerce').notnull()]
    df['datetime'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M:%S')
    
    df.rename(columns={'LAeq (Ch1, P1) [dB]': 'LAeq',
                       'LAFmax (Ch1, P1) [dB]': 'LAFmax',
                       'LAFmin (Ch1, P1) [dB]': 'LAFmin',
                       'LCpeak (Ch1, P1) [dB]': 'LCeq'}, inplace=True)
    
    # create LCeq-LAeq column
    df['LCeq-LAeq'] = df['LCeq'] - df['LAeq']

    return df



def get_data_audiomoth(filename: str):
    df = pd.read_csv(filename)
    if 'Time' in df.columns:
        df['datetime'] = pd.to_datetime(df['Time'], format='%Y-%m-%d_%H:%M:%S')
    else:
        df['datetime'] = pd.to_datetime(df['date'])
    return df 



def get_data_cesva(measurement_folder: str):
    if os.path.isfile(measurement_folder):
        cesva_index = measurement_folder.find('CESVA')
        if cesva_index != -1:
            measurement_folder = measurement_folder[:cesva_index] + 'CESVA'
        else:
            raise ValueError("CESVA folder not found in the file path.")

    elif 'CESVA' not in measurement_folder:
        raise ValueError("The directory does not contain 'CESVA'.")
    
    cesva_files = []
    cols_to_use = ['Date hour','Elapsed t','LA1s','LAFmax1s','LAFmin1s']
    
    for root, dirs, files in os.walk(measurement_folder, topdown=False):
        for name in files:
            if name.endswith('.csv'):
                cesva_files.append(os.path.join(root, name))
    
    df_all = pd.DataFrame()

    for file_path in cesva_files:
        try:
            df = pd.read_csv(file_path,sep=';',header=11,decimal=',', usecols=cols_to_use)
            df.dropna(subset=['Elapsed t'],inplace=True) 
        except Exception as e: 
            pass
        try:
            df = pd.read_csv(file_path,sep=';',header=12,decimal=',',usecols=cols_to_use)
            df.dropna(subset=['Elapsed t'],inplace=True)   
        except Exception as e: 
            pass
        
        #df = df[['Date hour','Elapsed t','LA1s','LAFmax1s','LAFmin1s']]
        df_all = pd.concat([df_all,df])
    
    df = df_all.copy()
    del df_all
    for col in df.columns:
        if col not in  ["Date hour", "Elapsed t"]:
            df[col] = pd.to_numeric(df[col])
    
    df['datetime'] = df.apply(lambda x: datetime.strptime(x['Date hour'], '%d/%m/%Y %H:%M:%S'),axis=1)
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df