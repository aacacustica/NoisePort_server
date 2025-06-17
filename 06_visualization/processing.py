import pandas as pd
import matplotlib.pyplot as plt
import os
plt.style.use("bmh")
from visualization import *
from reading import *
from utils_vi import *
from config_vi import *
from tqdm import tqdm
import glob
import json
from scipy.signal import find_peaks
import ast



def load_data(file_path, logger, new_date=None, new_time=None, new_threshold_date=None, new_threshold_time=None, selected_folder=None):
    logger.info("")
    slm_type_function_mapping = {
        'tenerife_TCT': (get_data_tenerife_TCT, tenerife_tct_dict),
        # "SV307": (get_data_SV307, sv307_dict),
        "SV307": (get_data_SV307_new, sv307_dict),
        "audiomoth": (get_data_audiomoth, audiopost_dict),
        "814": (get_data_814, larson814_dict),
        "824": (get_data_824, larson824_dict),
        "lx_ES": (get_data_lx_ES, larsonlx_dict),
        "lx_EN": (get_data_lx_EN, larsonlx_dict),
        "cesva": (get_data_cesva, cesva_dict),
        "sono-bilbo": (get_data_bilbo, sonometer_bilbo_dict),
        "bruel&kjaer": (get_data_bruel_kjaer, bruel_kjaer_dict),
    } # SLM stands for Sound Level Meter
    # load the data for each SLM type until one works |  for each slm_type, (func, slm_dict) in slm_type_function_mapping.items(): means that for each key and value in the dictionary, the key is slm_type and the value is a tuple with the function and the dictionary | the function is the function to load the data and the dictionary is the dictionary with the column names for the SLM type


    for slm_type, (func, slm_dict) in slm_type_function_mapping.items():
        try:
            logger.info(f"Loading file {file_path} for SLM type {slm_type}")

            # this is the actual invocation of the function
            df = func(file_path, logger, new_date=new_date, new_time=new_time, new_threshold_date=new_threshold_date, new_threshold_time=new_threshold_time, selected_folder=selected_folder)
            

            logger.info("")
            logger.info(f"Data loaded for SLM type {slm_type}")
            return df, slm_type, slm_dict
        


        except Exception as e:
            clean_message = str(e).replace('\n', ' ')
            logger.warning(f"Failed to load data for SLM type {slm_type}: {clean_message}. Trying next SLM type")
            continue
    
    raise ValueError("SLM type not found or file could not be loaded")



def process_folder(folder_path, folder_date_time, folder_threshold, logger, selected_folder):
    logger.info("")

    # folder contains a CESVA folder
    cesva_path = os.path.join(folder_path, 'CESVA')
    if os.path.isdir(cesva_path):
        # load the data from the CESVA folder
        subfolders = [f for f in os.listdir(cesva_path) if os.path.isdir(os.path.join(cesva_path, f))]
        new_date, new_time = folder_date_time.get(folder_path, (None, None))
        new_threshold_date, new_threshold_time  = folder_threshold.get(folder_path, (None, None))
        
        # CESVA folder contains subfolders
        for subfolder in subfolders:
            #load the data from the first subfolder
            subfolder_path = os.path.join(cesva_path, subfolder)
            
            # subfolder contains measurement files
            files = [os.path.join(subfolder_path, f) for f in os.listdir(subfolder_path) if f.endswith(('.csv', '.xlsx', '.CSV', 'XLSX'))]
            if files:
                # logger.info(f"Files found: {files}")
                return load_data(files[0], logger, new_date=new_date, new_time=new_time, new_threshold_date=new_threshold_date, new_threshold_time=new_threshold_time)
            else:
                logger.warning(f"No measurement files found in {subfolder_path}")


    else:
        new_date, new_time = folder_date_time.get(folder_path, (None, None))
        new_threshold_date, new_threshold_time  = folder_threshold.get(folder_path, (None, None))

        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(('.csv', '.xlsx', '.CSV'))]
        len_files = len(files)
        logger.info(f"Files found: {len_files} at {folder_path}")
        logger.info(f"Processing folder: {folder_path}")



        if len_files > 1:
            logger.info("Processing more than one file, so concatenating them")
            first_file = files[0]
            return load_data(first_file, logger, new_date=new_date, new_time=new_time, new_threshold_date=new_threshold_date, new_threshold_time=new_threshold_time,selected_folder=selected_folder)
        
        elif len_files == 1:
            logger.info("Processing only one file, so loading it directly")
            # this is the case where there is only one file
            return load_data(files[0], logger, new_date=new_date, new_time=new_time, new_threshold_date=new_threshold_date, new_threshold_time=new_threshold_time,selected_folder=selected_folder)
        

        if not files:
            logger.info(f"No measurement files found in {folder_path}")
            try:
                logger.info(f"Trying to load data")
                # load_data regular files
                return load_data(folder_path, logger, new_date=new_date, new_time=new_time, new_threshold_date=new_threshold_date, new_threshold_time=new_threshold_time, selected_folder=selected_folder)
            except Exception as e:
                logger.error(f"Error loading data: {e}")
            # if no files found, return None
            logger.error(f"No measurement files found in {folder_path}")
            return None, None, None
        
    return None, None, None 



def process_all_folders(input_folder, folders, PERIODO_AGREGACION, PERCENTILES, taxonomy, yamnet_csv, sufix_string, folder_coefficients, folder_date_time, folder_threshold, oca_limits, oca_type, logger):
    print()
    stable_version = get_stable_version(logger)
    
    for folder in tqdm(folders, desc="Processing folders"): # \\192.168.205.117\AAC_Server\OCIO\24052_ZARAUTZ\CAMPAÑA_1\3-Medidas\ZARAUTZ_C1_P1\AUDIOMOTH
        logger.info("")
        logger.info(f"Suffix string: {sufix_string}")


        #####################################
        # SETTING UP THE FOLDER PATHS
        #####################################
        try:
            reg_folder = os.path.join(input_folder, folder) # \\192.168.205.117\AAC_Server\INDUSTRIA\23132-IRUÑA_OCA_CANTERA\5-Resultados\FAA205-P1_CAMPAÑA1\SPL
            folder = folder.split("\\")[:-1]
            folder = os.path.join('\\\\', *folder)
            actual_folder_name = folder.split("\\")[-1]
            logger.info(f"Processing folder: {actual_folder_name}")
            logger.info(f"Entering folder: {folder}")


            spl_string = "SPL"
            graphics_string = f"Graphics_{sufix_string}"
            result_dir_name = "5-Resultados"


            # 5-Resultados directory
            resultados_dir = reg_folder.split("\\")[:-3]
            resultados_dir = os.path.join('\\\\', *resultados_dir, result_dir_name)
            logger.info(f"Resultados directory: {resultados_dir}")
            if not os.path.exists(resultados_dir):
                os.makedirs(resultados_dir)
                logger.info(f"Created output folder: {resultados_dir}")
            


            # CREATING THE FOLDER OUTPUT DIR --> /5-RESULTAODS/P1/SPL/GRAPHICS_SUFFIX/
            folder_output_dir = os.path.join(resultados_dir, folder, spl_string, graphics_string)
            logger.info(f"folder_output_dir: {folder_output_dir}")
            if '3-Medidas' in folder_output_dir:
                folder_output_dir = folder_output_dir.replace('3-Medidas', '5-Resultados')
            if not os.path.exists(folder_output_dir):
                os.makedirs(folder_output_dir)
                logger.info(f"Created output folder: {folder_output_dir}")
            

            # CREATIONG THE FOLDER OUTPUT DIR FOR WEEK 
            folder_output_dir_week = os.path.join(folder_output_dir, f"Graphics_week")
            logger.info(f"folder_output_dir_week: {folder_output_dir_week}")
            if not os.path.exists(folder_output_dir_week):
                os.makedirs(folder_output_dir_week)
                logger.info(f"Created output folder: {folder_output_dir_week}")



            # PREDICTION FOLDERS
            # CREATING THE PREDICTION FOLDER --> /5-RESULTAODS/P1/AI_MODEL/Predictions/
            ai_prediction_folder = os.path.join(folder.replace('3-Medidas', '5-Resultados'), "AI_MODEL", "Predictions")
            os.makedirs(ai_prediction_folder, exist_ok=True)
            logger.info(f"Created AI prediction folder: {ai_prediction_folder}")
            
            
            # CREATING THE PREDICTION VISUALIZATION FOLDER--> /5-RESULTAODS/P1/AI_MODEL/Visualizations/
            ia_visualization_folder = ai_prediction_folder.replace("Predictions", "Visualizations")
            os.makedirs(ia_visualization_folder, exist_ok=True)
            logger.info(f"Created AI visualization folder: {ia_visualization_folder}")
            


        except Exception as e:
            logger.error(f"An error occurred while setting up the folder paths: {e}")
            continue
        ########################################################


        #############################
        ## GETTING THE DATAFRAME ###
        #############################
        try:
            logger.info("")
            logger.info(f"Processing folder {folder}") 
            logger.info(f"Getting the data from the dataframes")
            
            
            if sufix_string == "SONOMETRO":
                logger.info(f"Processing SONOMETRO data")
                reg_folder = reg_folder.replace(ACOUSTIC_PARAMS_FOLDER, SONOMETER_FOLDER)
                df, slm_type, slm_dict = process_folder(reg_folder, folder_date_time, folder_threshold, logger,selected_folder=SONOMETER_FOLDER)
                if df is None:
                    logger.warning(f"df is None")
                    continue
            
            else:
                logger.info(f"Getting the acoustic data from the dataframes")
                df, slm_type, slm_dict = process_folder(reg_folder, folder_date_time, folder_threshold, logger, ACOUSTIC_PARAMS_FOLDER)
                if df is None:
                    logger.warning(f"df is None")
                    continue
            
            # taking just 1 day, which are the first 86400 rows
            # df = df.iloc[:86400] # 1 day of data, 24 hours * 60 minutes * 60 seconds = 86400 seconds


            #############################
            ### GETTING PREDICTION DF ###
            #############################
            logger.info("")
            logger.info(f"Getting the prediction data from the dataframes")
            reg_folder_prediction = reg_folder.replace(ACOUSTIC_PARAMS_FOLDER, PREDICTION_LITTLE_FOLDER)
            logger.info(f"Prediction folder: {reg_folder_prediction}")
            
            
            df_prediction, slm_type, slm_dict = process_folder(reg_folder_prediction, folder_date_time, folder_threshold, logger, PREDICTION_LITTLE_FOLDER)
            if df_prediction is None:
                logger.warning(f"df prediction is None")
                continue
            ###################################################################


            logger.info("")
            if TENERIFE_TIMEZONE:
                df['datetime'] = pd.to_datetime(df['datetime']) - pd.Timedelta(hours=1)
                logger.info(f"Time zone was set to Tenerife")

            #take data from 06/05/2025  16:00:00
            # df = df.loc[df['datetime'] >= '2025-05-06 16:00:00']

            try:
                if df is not None:
                    logger.info("")
                    # add datetime columns, sort by datetime and set datetime as index
                    logger.info(f"FOR SPL FILE: Adding datetime columns, sorting by datetime and setting datetime as index")
                    df = add_datetime_columns(df,logger, date_col='datetime')
                    df = df.sort_values('datetime')
                    df.set_index('datetime', inplace=True, drop=False)
                    start_date = df.index[0]
                    end_date = df.index[-1]

                    logger.info(f"Start date {start_date} and end date {end_date}")
                    logger.info(f"df was sorted by datetime and datetime was set as index")
                else:
                    logger.warning(f"df is None")
                    continue
                    

                # the same for the prediction file
                if df_prediction is not None:
                    logger.info("")
                    logger.info(f"FOR PREDICTION FILE: Adding datetime columns, sorting by datetime and setting datetime as index")
                    
                    df_prediction = add_datetime_columns(df_prediction, logger, date_col='datetime')
                    df_prediction = df_prediction.sort_values('datetime')
                    df_prediction.set_index('datetime', inplace=True, drop=False)
                    pred_start_date = df_prediction.index[0]
                    pred_end_date = df_prediction.index[-1]

                    logger.info(f"Start date {pred_start_date} and end date {pred_end_date}")
                    logger.info(f"df was sorted by datetime and datetime was set as index")
                else:
                    logger.warning(f"df_prediction is None")
            
            except Exception as e:
                logger.error(f"An error occurred while adding datetime columns: {e}")


            try:
                logger.info("")
                if df is not None:
                    # drop the beginning and ending of the measurement (15min)
                    df = df.loc[start_date + pd.Timedelta(REMOVE_START_TIME, unit='seconds'):end_date - pd.Timedelta(REMOVE_END_TIME, unit='seconds')]
                    logger.info(f"SPL df was trimmed, {REMOVE_START_TIME} secs from the beggining and {REMOVE_END_TIME} secs from the end")
                
                if df_prediction is not None:
                    df_prediction = df_prediction.loc[pred_start_date + pd.Timedelta(REMOVE_START_TIME, unit='seconds'):pred_end_date - pd.Timedelta(REMOVE_END_TIME, unit='seconds')]
                    logger.info(f"Prediction df was trimmed, {REMOVE_START_TIME} secs from the beggining and {REMOVE_END_TIME} secs from the end")


                # add indicators column
                if df is not None:
                    logger.info(f"Adding indicators column")
                    df['indicador_str'] = df.apply(lambda x: evaluation_period_str(x['hour']), axis=1)
                if df_prediction is not None:
                    df_prediction['indicador_str'] = df_prediction.apply(lambda x: evaluation_period_str(x['hour']), axis=1)

                
                # add nights column
                if df is not None:
                    logger.info(f"Adding nights column")
                    df['night_str'] = df.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)
                if df_prediction is not None:
                    df_prediction['night_str'] = df_prediction.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)

            except Exception as e:
                logger.error(f"An error occurred while adding indicators and nights columns: {e}")



            try:
                # add oca column
                logger.info(f"Adding oca column")
                logger.info(f"OCA Limits --> {oca_limits}")

                if df is not None:
                    df['oca'] = df['hour'].apply(
                            lambda h: db_limit(h, **oca_limits)
                    )
                if df.isnull().values.any():
                # check if there is nan values
                    logger.warning(f"There are nan values in the dataframe")
                    # removing nan values
                    df = df.dropna()
                    logger.info(f"Removing nan values from the dataframe")

                # removing nan values
                if df_prediction is not None:
                    df_prediction = df_prediction.dropna()
                    logger.info(f"Removing nan values")

            except Exception as e:
                logger.error(f"An error occurred while adding oca column: {e}")
                


            try:
                #####################################################
                ########## APPLYING DB CORRECTION TO THE DATA #########
                #####################################################
                logger.info("")
                tuple_folder_coeff = []
                logger.info(f"Applying db correction")
                for key, value in folder_coefficients.items():
                    key = key.split("\\")[:-1]
                    folder_name = key[-1]

                    # save tupples folder name, coefficient value
                    folder_name_coeff_value = (folder_name, value)
                    tuple_folder_coeff.append(folder_name_coeff_value)
                    key = os.path.join('\\\\', *key)

                    # assign the value to the folder
                    if folder == key:
                        df = apply_db_correction(df, value, sufix_string, logger)
                        logger.info(f"Apply {value} correction coefficient to the folder {folder}")
            
            except Exception as e:
                logger.error(f"An error occurred while processing folder {folder}: {e}")
                

            try:
                logger.info("")    
                logger.info(f"Creating slm_dict")    
                folder = folder.split("\\")[-1]
                
                # add slm_dict column LAEQ_COLUMN_COEFF: with the value of LA_corrected
                slm_dict["LAEQ_COLUMN_COEFF"] = 'LA_corrected'
                slm_dict["LAMAX_COLUMN_COEFF"] = 'LAmax_corrected'
                slm_dict["LAMIN_COLUMN_COEFF"] = 'LAmin_corrected'
                #### NEW ONES ####
                slm_dict["LC-LA_COLUMN_COEFF"] = 'LCeq-LAeq_corrected'
                # slm_dict["L90_COLUMN_COEFF"] = '90percentile'


                # SAVE THE INFO IN A JSON FILE
                info_dict = {
                    "PERIODO_AGREGACION": PERIODO_AGREGACION,
                    "PERCENTILES": PERCENTILES,
                    "folder_coeff": tuple_folder_coeff,
                    "stable_version": stable_version,
                    "slm_type": slm_type,
                    "oca_limits": oca_limits,
                    "oca_type": oca_type,
                }

                # save the info in a json file
                with open(os.path.join(folder_output_dir, "processing_parameters.json"), 'w') as f:
                    json.dump(info_dict, f)
                logger.info(f"Saved processing_parameters.json in {folder_output_dir}")
            except Exception as e:
                logger.error(f"An error occurred while creating slm_dict: {e}")
                continue



            # try:
            #     ################################################################
            #     # TRANSFORMING 1 SECOND DATA TO 1 HOUR DATA
            #     ##################################################################
            #     logger.info("")
            #     logger.info(f"Transforming 1 second data to 1 hour data")
                
            #     df_1h = transform_1h(df, slm_dict, logger, agg_period=3600)
            #     logger.info(f"Transformed 1 second data to 1 hour data")
            #     df_1h = df_1h.round(2)


            #     logger.info(f"MAKING FOLDER FOR 1H ANALYSIS TO SAVE THE DATA")
            #     # remove the last part of the folder_output_dir
            #     folder_output_dir_for_alarms = os.path.dirname(folder_output_dir)
            #     folder_output_dir_1h = os.path.join(folder_output_dir_for_alarms, 'Graphics_ALARMS')
            #     os.makedirs(folder_output_dir_1h, exist_ok=True)


            #     logger.info("")
            #     logger.info("")
            #     logger.info(f"TRANSFORMATION SECTION")
            #     logger.info(f"Adding oca column")
            #     logger.info(f"OCA Limits --> {oca_limits}")
            #     # set index (which is datetime) as column
            #     df_1h.reset_index(inplace=True)
                
            #     df_1h = transformation(df_1h, logger, oca_limits)

            #     #####
            #     #ssave
            #     df_1h_csv_path = os.path.join(folder_output_dir_1h, f"{actual_folder_name}_1h.csv")
            #     df_1h.to_csv(df_1h_csv_path, index=False)
            #     logger.info(f"Saved 1 hour dataframe to {df_1h_csv_path}")
            # except Exception as e:
            #     logger.error(f"An error occurred while transforming 1 second data to 1 hour data: {e}")
            #     continue



            # try:
            #     ##################################
            #     # PEAK + ROLLING ANALYSIS
            #     ##################################
            #     logger.info("")
            #     logger.info(f"Applying find_peaks function to the whole dataframe")
            #     peaks, properties=find_peaks(df['LA_corrected'], prominence=PROMINENCE, width=WIDTH)
            #     df_peaks = df.iloc[peaks].copy()

            #     logger.info("")
            #     logger.info(f"Rolling the data with a window of {WINDOW_SIZE} seconds")
            #     # rolling median for the LA values with a window of 30 seconds
            #     df_peaks['LA_cor_median'] = df_peaks['LA_corrected'].rolling(window=WINDOW_SIZE, min_periods=1).quantile(0.5) + ADDING_THRESHOLD
            #     df_peaks['Peak'] = 1

            #     #above threshold
            #     logger.info(f"Calculating peaks above threshold")
            #     df_peaks = df_peaks[df_peaks['LA_corrected'] > df_peaks['LA_cor_median']]
            #     logger.info(f"There are {len(df_peaks)} peaks in the dataframe after filtering")
            #     # 

            #     df_peaks_csv_path = os.path.join(folder_output_dir, f"{actual_folder_name}_peaks_filtered.csv")
            #     df_peaks.to_csv(df_peaks_csv_path, index=False)
            #     logger.info(f"Saved peaks filtered dataframe to {df_peaks_csv_path}, with a lenght of {len(df_peaks)}")

            #     #Timestamp to datetime
            #     df_peaks['Timestamp'] = pd.to_datetime(df_peaks['Timestamp'])
            # except Exception as e:
            #     logger.error(f"An error occurred while trimming the dataframe {e}")
                # continue



            ###################################
            ###################################
            ###################################
            # MERGING SECTION
            ####################################
            # logger.info("")
            # logger.info(f"MERGING SECTION")
            
            # try:
            #     ################################### MERGING ACOUSTIC WITH PREDICTION DATAFRAME ###################################
            #     acoustic_pred_csv_path = os.path.join(ai_prediction_folder, f"{actual_folder_name}_acoustic_pred.csv")
            #     if os.path.exists(acoustic_pred_csv_path):
            #         logger.info(f"[1] File {acoustic_pred_csv_path} already exists, skipping merge")
            #         df_acustic_pred = pd.read_csv(acoustic_pred_csv_path)
                
            #     else:
            #         logger.info("[1] Merging the prediction dataframe with the acoustic dataframe")
            #         df_acustic_pred = df.merge(
            #             df_prediction[['class', 'probability']],
            #             left_index=True,
            #             right_index=True,
            #             how='left'
            #         )
            #         logger.info("Merge successful for the acoustic and prediction dataframes")


            #         logger.info("Saving the merged dataframe to the prediction folder")
            #         df_acustic_pred.to_csv(acoustic_pred_csv_path, index=False)
            #         logger.info(f"Saved merged dataframe to {acoustic_pred_csv_path}")

            # except Exception as e:
            #     logger.error(f"An error occurred while merging the ACOUSTIC PREDICT dataframE: {e}")
            #     ###################################



            # try:
            #         ############# MERGING PEAKS WITH ACOUSTIC DATAFRAME ##################
            #     acoustic_pred_peak_csv_path = os.path.join(ai_prediction_folder, f"{actual_folder_name}_acoustic_pred_peak.csv")
            #     if os.path.exists(acoustic_pred_peak_csv_path):
            #         logger.info(f"[2] File {acoustic_pred_peak_csv_path} already exists, skipping merge")
            #         df_all = pd.read_csv(acoustic_pred_peak_csv_path)

            #     else:
            #         logger.info("")
            #         logger.info("[2] Merging the peaks dataframe with the acoustic dataframe")
            #         df_all = df_acustic_pred.merge(
            #             df_peaks[['Peak']],
            #             left_index=True,
            #             right_index=True,
            #             how='left'
            #         )
            #         logger.info("Merge successful for the peaks and acoustic dataframes")

            #         logger.info(f"Saving the merged dataframe to the prediction folder")
            #         df_all.to_csv(acoustic_pred_peak_csv_path, index=False)
            #         logger.info(f"Saved merged dataframe to {acoustic_pred_peak_csv_path}")

            # except Exception as e:
            #     logger.error(f"An error occurred while merging the ACOUSTIC PREDICT PEAKS dataframE: {e}")



            # try:
            #     ################################### MERGING ALL WITH YAMNET DATAFRAME ###################################
            #     yamnet_all_csv_path = os.path.join(ai_prediction_folder, f"{actual_folder_name}_all_yamnet.csv")              
            #     if os.path.exists(yamnet_all_csv_path):
            #         logger.info(f"[3] File {yamnet_all_csv_path} already exists, skipping merget")
            #         df_all_yamnet = pd.read_csv(yamnet_all_csv_path)


            #     else:
            #         logger.info("")
            #         logger.info("[3] Merging the peaks dataframe with the yamnet dataframe")
            #         # [1] convert the string‐representations into reallists
            #         df_all_cp = df_all.copy()

            #         # remove nan values for all column
            #         df_all_cp.dropna(subset=['class', 'probability'], inplace=True)
            #         df_all_cp['class'] = df_all_cp['class'].apply(ast.literal_eval)
            #         df_all_cp['probability'] = df_all_cp['probability'].apply(ast.literal_eval)

            #         # [2]exploding both columns at once
            #         df_exploded = (df_all_cp.explode(['class', 'probability']).reset_index(drop=True))
            #         # print(df_exploded)

            #         # true is to avoid the index being added as a column and false is to keep the index
            #         ####################################################################

            #         df_all_yamnet = df_exploded.merge(
            #             yamnet_csv,
            #             how="left",
            #             left_on="class",
            #             right_on="display_name"
            #         )
            #         df_all_yamnet.drop(columns=['display_name'], inplace=True, errors='ignore')
            #         logger.info("Merge successful for the peaks and acoustic dataframes")


            #         logger.info(f"Saving the merged dataframe to the prediction folder")  
            #         df_all_yamnet.to_csv(yamnet_all_csv_path, index=False)
            #         logger.info(f"Saved merged dataframe to {yamnet_all_csv_path}")

            # except Exception as e:
            #     logger.error(f"An error occurred while merging the ACOUSTIC dataframE: {e}")
            ###################################
            ###################################
            ###################################


            ###################################
            ###################################
            logger.info("")
            logger.info(f"PLOTTING SECTION")

            # Plotting night evolution
            # if PLOT_NIGHT_EVOLUTION:
            #     logger.info(f"[1] Plotting night evolution for folder {folder}")
            #     plot_night_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")
            
            if PLOT_NIGHT_EVOLUTION_WEEK:
                logger.info(f"[1.1] Plotting night evolution for folder {folder}")
                plot_night_evolution_week(df, folder_output_dir_week, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")

            # Plotting night evolution 15 min
            # if PLOT_NIGHT_EVOLUTION_15_MIN:
            #     logger.info(f"[2] Plotting night evolution 15 min for folder {folder}")
            #     plot_night_evolution_15_min(df, folder_output_dir, logger, name_extension="15_min", laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")

            # Plotting night evolution 15 min
            if PLOT_NIGHT_EVOLUTION_15_MIN_WEEK:
                logger.info(f"[2.2] Plotting night evolution 15 min for folder {folder}")
                plot_night_evolution_15_min_week(df, folder_output_dir_week, logger, name_extension="15_min", laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")


            ############################ PREDICTION PLOTTING SECTION ####################################################################################
            # Plotting LEq power average with predictions
            """
            if PLOT_PREDIC_LAEQ_MEAN:
                logger.info(f"[3] Plotting PLOT_PREDIC_LAEQ_MEAN for folder {folder}")
                plot_predic_laeq_mean(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)
            

            # Plotting LEq power average with predictions
            if PLOT_PREDIC_LAEQ_MEAN_WEEK:
                logger.info(f"[3] Plotting PLOT_PREDIC_LAEQ_MEAN for folder {folder}")
                plot_predic_laeq_mean_week(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)
            """

            # TODO
            # if PLOT_PREDIC_LAEQ_15_MIN_PERIOD:
            #     logger.info(f"[4] Plotting PLOT_PREDIC_LAEQ_15_MIN_PERIOD for folder {folder}")
            #     plot_predic_laeq_15_min_period(df, yamnet_csv, taxonomy, ia_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)


            # if PLOT_PREDIC_LAEQ_15_MIN_4H:
            #     logger.info(f"[5] Plotting PLOT_PREDIC_LAEQ_4H for folder {folder}")
            #     plot_predic_laeq_15_min_4h(df, yamnet_csv,taxonomy, df_prediction, ia_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)


            # # Plotting stack bar with predictions class
            # if PLOT_PREDICTION_STACK_BAR:
            #     logger.info(f"[6] Plotting PLOT_PREDICTION_STACK_BAR for folder {folder}")
            #     plot_prediction_stack_bar(df_prediction, yamnet_csv, taxonomy, ia_visualization_folder, logger, plotname=folder)
            

            # Plotting prediction map
            # if PLOT_PREDICTION_MAP:
            #     logger.info(f"[7] Plotting PLOT_PREDICTION_MAP for folder {folder}")
            #     plot_prediction_map(df_prediction, taxonomy, ia_visualization_folder, logger, plotname=folder)
            
            """
            if PLOT_PREDICTION_MAP:
                logger.info(f"[7] Plotting PLOT_PREDICTION_MAP for folder {folder}")
                plot_prediction_map_new(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)
            """

            # # Plotting tree map
            # if PLOT_TREE_MAP:
            #     logger.info(f"[8] Plotting PLOT_TREE_MAP for folder {folder}")
            #     plot_tree_map(df_prediction,taxonomy,ia_visualization_folder, logger, plotname=folder)
            ##############################################################################################################################################


            
            # Plotting time plot
            # if PLOT_MAKE_TIME_PLOT:
            #     logger.info(f"[9] Plotting time plot for folder {folder}")
            #     make_time_plot(df, folder_output_dir, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder, percentiles=PERCENTILES)


            # Plotting time plot
            if PLOT_MAKE_TIME_PLOT_WEEK:
                logger.info(f"[9.1] Plotting time plot for folder {folder}")
                make_time_plot_week(df, folder_output_dir_week, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder, percentiles=PERCENTILES)


            # Plotting heatmap evolution hour
            # if PLOT_HEATMAP_EVOLUTION_HOUR:
            #     logger.info(f"[10] Plotting heatmap for folder {folder}")
            #     plot_heatmap_evolution_hour(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)


            # Plotting heatmap evolution hour
            if PLOT_HEATMAP_EVOLUTION_HOUR_WEEK:
                logger.info(f"[10.1] Plotting heatmap for folder {folder}")
                plot_heatmap_evolution_hour_week(df, folder_output_dir_week, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)
            
            
            # Plotting heatmap evolution 15 min
            # if PLOT_HEATMAP_EVOLUTION_15_MIN:
            #     logger.info(f"[11] Plotting heatmap 15 min for folder {folder}")
            #     plot_heatmap_evolution_15_min(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)

            
            # Plotting heatmap evolution 15 min
            if PLOT_HEATMAP_EVOLUTION_15_MIN_WEEK:
                logger.info(f"[11] Plotting heatmap 15 min for folder {folder}")
                plot_heatmap_evolution_15_min_week(df, folder_output_dir_week, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)
            

            # Plotting individual heatmap
            # if PLOT_INDICADORES_HEATMAP:
            #     logger.info(f"[12] Plotting indicadores heatmap for folder {folder}")
            #     plot_indicadores_heatmap(df, folder_output_dir, logger, plotname=folder, ind_column=slm_dict["LAEQ_COLUMN_COEFF"])

            
            # Plotting individual heatmap
            if PLOT_INDICADORES_HEATMAP_WEEK:
                logger.info(f"[12] Plotting indicadores heatmap for folder {folder}")
                plot_indicadores_heatmap_week(df, folder_output_dir_week, logger, plotname=folder, ind_column=slm_dict["LAEQ_COLUMN_COEFF"])


            # Plotting day evolution
            # if PLOT_DAY_EVOLUTION:
            #     logger.info(f"[13] Plotting day evolution for folder {folder}")
            #     plot_day_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)


            if PLOT_DAY_EVOLUTION_WEEK:
                logger.info(f"[13] Plotting day evolution for folder {folder}")
                plot_day_evolution_week(df, folder_output_dir_week, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)
            

            # Plotting period evolution
            # if PLOT_PERIOD_EVOLUTION:
            #     logger.info(f"[14] Plotting period evolution (1) Ld (2) Le for folder {folder}")
            #     plot_period_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)


            # Plotting period evolution
            if PLOT_PERIOD_EVOLUTION_WEEK:
                logger.info(f"[14] Plotting period evolution (1) Ld (2) Le for folder {folder}")
                plot_period_evolution_week(df, folder_output_dir_week, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)
            

            # I dont know why I commented this out
            # if PLOT_SPECTROGRAM_1_3:
            #     logger.info(f"[15] Plotting spectrogram for folder {folder}")
            #     # plt_spectrogram(df_oct, folder_output_dir, logger, plotname=folder)
            #     plt_spectrogram(df, folder_output_dir, sufix_string, logger, plotname=folder)




            #############################
            ######### PLOTING ALARMS
            ################################
            logger.info("")
            logger.info(f"PLOTTING ALARMS!!!")

            if OCA_ALARM:
                logger.info(f"[1] Plotting OCA alarm for folder {folder}")
                oca_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder)
            
            
            if LMAX_ALARM:
                logger.info(f"[2] Plotting LMAX alarm for folder {folder}")
                lmax_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold=95) # OCA +10

            
            if LC_LA_ALARM:
                logger.info(f"[3] Plotting LC-LA alarm for folder {folder}")
                LC_LA_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_norma=10, threshold_dB=3)


            if L90_ALARM:
                logger.info(f"[4] Plotting L90 alarm for folder {folder}")
                l90_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_dB=5)


            if L90_ALARM_DYNAMIC:
                # TODO
                logger.info(f"[5] Plotting L90 alarm dynamic for folder {folder}")
                l90_alarm_dynamic(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_dB=5)


            if FREQUENCY_COMPOSITION:
                logger.info(f"[6] Plotting frequency composition for folder {folder}")            
                frequency_composition(df, folder_output_dir_1h, logger, plotname=folder, threshold_comp=5)


            if TONAL_FREQUENCY:
                logger.info(f"[7] Plotting tonal frequency for folder {folder}")
                tonal_frequency(df, folder_output_dir_1h, logger, plotname=folder)





            ################################################################
            # PEAK ANALYSIS
            ##################################################################
            logger.info("")
            logger.info(f"PEAKS PLOTTING!!!")
            

            if PLOT_PEAK_DISTRIBUTION_HEATMAP:
                logger.info(f"[8] Plotting peak heatmap for folder {folder}")
                plot_peak_distribution_heatmap(df_peaks, folder_output_dir_1h, logger, plotname=folder)


            if PLOT_PEAK_DISTRIBUTION:
                logger.info(f"[9] Plotting peak distribution for folder {folder}")
                plot_peak_distribution(df_peaks, folder_output_dir_1h, logger, plotname=folder)


            if PLOT_PEAK_DENSITY_DISTRIBUTION:
                logger.info(f"[10] Plotting density distribution for folder {folder}")
                plot_density_distribution_peaks(df_peaks, folder_output_dir_1h, logger, plotname=folder)


            # #####################################################
            # PLOTTING PREDICTION SECTION
            # #####################################################
            if PLOT_PEAK_PREDIC_LAEQ_MEAN:
                logger.info(f"[11] Plotting PLOT_PREDIC_LAEQ for folder {folder}")
                plot_predic_peak_laeq_mean(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)

            
            if PLOT_PEAK_BOX_PLOT_PREDICTION:
                logger.info(f"[12] Plotting box plot prediction for folder {folder}")
                plot_box_plot_prediction(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)


            if PLOT_PEAK_HEATMAT_PREDICTION:
                logger.info(f"[13] Plotting heatmap prediction for folder {folder}")
                plot_heat_map_prediction(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)



        except Exception as e:
            logger.error(f"An error occurred while processing folder {folder}: {e}")