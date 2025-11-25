import pandas as pd
import matplotlib.pyplot as plt
import os
plt.style.use("bmh")
from visualization import *
from reading import *
from utils import *
from config import *
from tqdm import tqdm
import glob
import json


def load_data(file_path, logger):
    slm_type_function_mapping = {
        "814": (get_data_814, larson814_dict),
        "824": (get_data_824, larson824_dict),
        "lx_ES": (get_data_lx_ES, larsonlx_dict),
        "lx_EN": (get_data_lx_EN, larsonlx_dict),
        "SV307": (get_data_SV307, sv307_dict),
        "cesva": (get_data_cesva, cesva_dict),
        "audiomoth": (get_data_audiomoth, audiopost_dict),
        "sono-bilbo": (get_data_bilbo, sonometer_bilbo_dict),
    } # SLM stands for Sound Level Meter
    # load the data for each SLM type until one works |  for each slm_type, (func, slm_dict) in slm_type_function_mapping.items(): means that for each key and value in the dictionary, the key is slm_type and the value is a tuple with the function and the dictionary | the function is the function to load the data and the dictionary is the dictionary with the column names for the SLM type
    for slm_type, (func, slm_dict) in slm_type_function_mapping.items():
        try:
            logger.info(f"Loading data for SLM type {slm_type}")
            df = func(file_path)
            logger.info("\n")
            logger.info(f"Data loaded for SLM type {slm_type}")
            return df, slm_type, slm_dict
        
        except Exception as e:
            clean_message = str(e).replace('\n', ' ')
            logger.warning(f"Failed to load data for SLM type {slm_type}: {clean_message}. Trying next SLM type")
            continue
    raise ValueError("SLM type not found or file could not be loaded")



def process_folder(folder_path, logger):
    # folder contains a CESVA folder
    cesva_path = os.path.join(folder_path, 'CESVA')
    if os.path.isdir(cesva_path):
        # load the data from the CESVA folder
        subfolders = [f for f in os.listdir(cesva_path) if os.path.isdir(os.path.join(cesva_path, f))]
        
        # CESVA folder contains subfolders
        for subfolder in subfolders:
            #load the data from the first subfolder
            subfolder_path = os.path.join(cesva_path, subfolder)
            
            # subfolder contains measurement files
            files = [os.path.join(subfolder_path, f) for f in os.listdir(subfolder_path) if f.endswith(('.csv', '.xlsx', '.CSV', 'XLSX'))]
            if files:
                logger.info(f"Files found: {files}")
                return load_data(files[0], logger)  
            else:
                logger.warning(f"No measurement files found in {subfolder_path}")

    else:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(('.csv', '.xlsx', '.CSV'))]
        logger.info(f"Files found: {files}")
        
        if not files:
            logger.warning(f"No measurement files found in {folder_path}")
            return None, None, None
        
        return load_data(files[0], logger) 
    return None, None, None 



def process_all_folders(input_folder, folders, PERIODO_AGREGACION, PERCENTILES, taxonomy, yamnet_csv, sufix_string, folder_coefficients, logger):
    print()
    stable_version = get_stable_version(logger)

    for folder in tqdm(folders, desc="Processing folders"): # \\192.168.205.117\AAC_Server\OCIO\24052_ZARAUTZ\CAMPAÑA_1\3-Medidas\ZARAUTZ_C1_P1\AUDIOMOTH
        reg_folder = os.path.join(input_folder, folder) # \\192.168.205.117\AAC_Server\INDUSTRIA\23132-IRUÑA_OCA_CANTERA\5-Resultados\FAA205-P1_CAMPAÑA1\SPL
        folder = folder.split("\\")[:-1]
        folder = os.path.join('\\\\', *folder)
        logger.info(f"Entering folder: {folder}")
        spl_string = "SPL"
        graphics_string = f"Graphics_{sufix_string}"
        result_dir_name = "5-Resultados"
        resultados_dir = reg_folder.split("\\")[:-3]
        resultados_dir = os.path.join('\\\\', *resultados_dir, result_dir_name)

        # CREATING THE OUTPUTFOLDER IF IT DOESN'T EXIST
        if not os.path.exists(resultados_dir):
            os.makedirs(resultados_dir)
            logger.info(f"Created output folder: {resultados_dir}")
        
        folder_output_dir = os.path.join(resultados_dir, folder, spl_string, graphics_string)
        if '3-Medidas' in folder_output_dir:
            folder_output_dir = folder_output_dir.replace('3-Medidas', '5-Resultados')

        if not os.path.exists(folder_output_dir):
            os.makedirs(folder_output_dir)
            logger.info(f"Created output folder: {folder_output_dir}")

        logger.info(f"folder_output_dir: {folder_output_dir}")
            


        ##############################################################
        ########## GETTING 1/3 OCTAVE FILE FOR EACH FOLDER ###########
        oct_file = glob.glob(os.path.join(reg_folder, "leq_oct_*"))
        try:
            df_oct = pd.read_csv(oct_file[0])
        except (IndexError, FileNotFoundError):
            logger.warning(f"No oct file found or failed to load in folder: {reg_folder}")
            df_oct = None
        except Exception as e:
            logger.warning(f"An error occurred while loading the oct file: {e}")
            df_oct = None
        ##############################################################

        ##############################################################
        ########## GETTING PREDICTION FILE FOR EACH FOLDER ###########
        predictions_folder = os.path.join(folder.replace('3-Medidas', '5-Resultados'), "AI_MODEL", "Predictions")
        prediction_csv_file = None
        if not os.path.exists(predictions_folder):
            logger.warning(f"Predictions folder not found: {predictions_folder}")
        if os.path.exists(predictions_folder):
            # list csv files in the directory
            predictions_files = glob.glob(os.path.join(predictions_folder, "*.csv"))
            if predictions_files:
                prediction_file = predictions_files[0]
                prediction_csv_file = prediction_csv(prediction_file)
            else:
                logger.warning("No CSV files found in the predictions folder.")
        
        predictions_visualization_folder = predictions_folder.replace("Predictions", "Visualizations")
        if not os.path.exists(predictions_visualization_folder):
            os.makedirs(predictions_visualization_folder)
            logger.info(f"Created output folder: {predictions_visualization_folder}")
        

        #creating the peaks folder for predictions
        predictions_peak_folder = os.path.join(predictions_visualization_folder, "Peaks")
        if not os.path.exists(predictions_peak_folder):
            os.makedirs(predictions_peak_folder)
            logger.info(f"Created ouput folder: {predictions_peak_folder}")


        # create the folder for the graphic analysis in SPL
        # remove the last element of the folder_output_dir
        folder_output_dir_graphic_analysis = folder_output_dir.split("\\")[:-1]
        
        # join the pathj
        folder_output_dir_graphic_analysis = os.path.join('\\\\', *folder_output_dir_graphic_analysis)
        
        folder_output_dir_graphic_analysis = os.path.join(folder_output_dir_graphic_analysis, "Peaks", "Graphic_analysis")
        if not os.path.exists(folder_output_dir_graphic_analysis):
            os.makedirs(folder_output_dir_graphic_analysis)
            logger.info(f"Created output folder: {folder_output_dir_graphic_analysis}")
        logger.info(f"folder_output_dir_graphic_analysis: {folder_output_dir_graphic_analysis}")
        ##############################################################

        ###################################################################
        ########## GETTING PEAK PREDICTION FILE FOR EACH FOLDER ###########
        peak_predictions_folder = os.path.join(folder.replace('3-Medidas', '5-Resultados'), "SPL", "Peaks")
        peak_prediction_csv_file = None
        if not os.path.exists(peak_predictions_folder):
            logger.warning(f"Peaks folder not found: {peak_predictions_folder}")
        if os.path.exists(peak_predictions_folder):
            # list csv files in the directory
            peak_predictions_files = glob.glob(os.path.join(peak_predictions_folder, "*.csv"))
            if peak_predictions_files:
                # take the file which contains 'peak_prediction' in the name
                peak_prediction_file = [f for f in peak_predictions_files if 'peak_prediction' in f][0]
                peak_prediction_csv_file = pd.read_csv(peak_prediction_file)
            else:
                logger.warning("No CSV files found in the peaks folder.")
        ###################################################################



        try:
            logger.info("\n")
            logger.info(f"Processing folder {folder}") 
            df, slm_type, slm_dict = process_folder(reg_folder, logger)
            if df is None:
                logger.info(f"df is None")
                continue
        
            # add one hour to the datetime column
            # if 'datetime' in df.columns:
            #     df['datetime'] = pd.to_datetime(df['datetime']) + pd.Timedelta(hours=1)

            # add datetime columns, sort by datetime and set datetime as index
            logger.info(f"FOR SPL FILE: Adding datetime columns, sorting by datetime and setting datetime as index")
            df = add_datetime_columns(df,logger, date_col='datetime') 
            df = df.sort_values('datetime')
            df.set_index('datetime', inplace=True, drop=False)
            start_date = df.index[0]
            end_date = df.index[-1]
            logger.info(f"Start date {start_date} and end date {end_date}")
            logger.info(f"df was sorted by datetime and datetime was set as index")


            # the same for the prediction file
            if prediction_csv_file is not None:
                logger.info(f"FOR PREDICTION FILE: Adding datetime columns, sorting by datetime and setting datetime as index")
                prediction_csv_file = add_datetime_columns_pred(prediction_csv_file, logger, date_col='date')
                prediction_csv_file = prediction_csv_file.sort_values('date')
                prediction_csv_file.set_index('date', inplace=True, drop=False)
                pred_start_date = prediction_csv_file.index[0]
                pred_end_date = prediction_csv_file.index[-1]
                logger.info(f"Start date {pred_start_date} and end date {pred_end_date}")
                logger.info(f"df was sorted by datetime and datetime was set as index")
            else:
                logger.warning(f"prediction_csv_file is None")

            # the same for the peak prediction file
            if peak_prediction_csv_file is not None:
                logger.info(f"FOR PEAK PREDICTION FILE: Adding datetime columns, sorting by datetime and setting datetime as index")
                peak_prediction_csv_file = add_datetime_columns_pred(peak_prediction_csv_file, logger, date_col='start_time')
                peak_prediction_csv_file = peak_prediction_csv_file.sort_values('start_time')
                peak_prediction_csv_file.set_index('start_time', inplace=True, drop=False)
                peak_pred_start_date = peak_prediction_csv_file.index[0]
                peak_pred_end_date = peak_prediction_csv_file.index[-1]
                logger.info(f"Start date {peak_pred_start_date} and end date {peak_pred_end_date}")
                logger.info(f"df was sorted by datetime and datetime was set as index")
            else:
                logger.warning(f"peak_prediction_csv_file is None")

            
            try:
                # drop the beginning and ending of the measurement (15min)
                df = df.loc[start_date + pd.Timedelta(REMOVE_START_TIME, unit='seconds'):end_date - pd.Timedelta(REMOVE_END_TIME, unit='seconds')]
                logger.info(f"SPL df was trimmed, {REMOVE_START_TIME} secs from the beggining and {REMOVE_END_TIME} secs from the end")
                if prediction_csv_file is not None:
                    prediction_csv_file = prediction_csv_file.loc[pred_start_date + pd.Timedelta(REMOVE_START_TIME, unit='seconds'):pred_end_date - pd.Timedelta(REMOVE_END_TIME, unit='seconds')]
                    logger.info(f"Prediction df was trimmed, {REMOVE_START_TIME} secs from the beggining and {REMOVE_END_TIME} secs from the end")

                # add indicators column
                logger.info(f"Adding indicators column")
                df['indicador_str'] = df.apply(lambda x: evaluation_period_str(x['hour']), axis=1)
                if prediction_csv_file is not None:
                    prediction_csv_file['indicador_str'] = prediction_csv_file.apply(lambda x: evaluation_period_str(x['hour']), axis=1)
                if peak_prediction_csv_file is not None:
                    peak_prediction_csv_file['indicador_str'] = peak_prediction_csv_file.apply(lambda x: evaluation_period_str(x['hour']), axis=1)

                # add nights column
                logger.info(f"Adding nights column")
                df['night_str'] = df.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)
                if prediction_csv_file is not None:
                    prediction_csv_file['night_str'] = prediction_csv_file.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)
                if peak_prediction_csv_file is not None:
                    peak_prediction_csv_file['night_str'] = peak_prediction_csv_file.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)

                # add oca column
                logger.info(f"Adding oca column")
                df['oca'] = df.apply(lambda x: db_limit(x['hour'],ld_limit= LIMITE_DIA , le_limit= LIMITE_TARDE ,ln_limit= LIMITE_NOCHE) , axis=1)

                # removing nan values
                if prediction_csv_file is not None:
                    prediction_csv_file = prediction_csv_file.dropna()
                    logger.info(f"Removing nan values")
                # check if there is nan values
                if df.isnull().values.any():
                    logger.warning(f"There are nan values in the dataframe")
                #df['oca'] = df.apply(lambda x: db_limit(x['hour'],ld_limit= LIMITE_DIA , le_limit= LIMITE_TARDE ,ln_limit= LIMITE_NOCHE) , axis=1)

                
                #####################################################
                ########## APPLYING DB CORRECTION TO THE DATA #########
                #####################################################
                tuple_folder_coeff = []
                logger.info(f"Applying db correction")
                for key, value in folder_coefficients.items():
                    key = key.split("\\")[:-1]
                    folder_name = key[-1]

                    # save tupples folder name, coefficient value
                    folder_name_coeff_value = (folder_name, value)
                    tuple_folder_coeff.append(folder_name_coeff_value)

                    # assign the value to the folder
                    key = os.path.join('\\\\', *key)
                    if folder == key:
                        df = apply_db_correction(df, value, sufix_string, logger)
                        logger.info(f"Apply {value} correction coefficient to the folder {folder}")


            except Exception as e:
                logger.error(f"An error occurred while trimming the dataframe {e}")
                continue


            folder = folder.split("\\")[-1]

            # add slm_dict column LAEQ_COLUMN_COEFF: with the value of LA_corrected
            slm_dict["LAEQ_COLUMN_COEFF"] = 'LA_corrected'
            slm_dict["LAMAX_COLUMN_COEFF"] = 'LAmax_corrected'
            slm_dict["LAMIN_COLUMN_COEFF"] = 'LAmin_corrected'
            slm_dict["LC-LA_COLUMN_COEFF"] = 'LCeq-LAeq_corrected'
            slm_dict["L90_COLUMN_COEFF"] = '90percentile'
            
            # SAVE THE INFO IN A JSON FILE
            info_dict = {
                "PERIODO_AGREGACION": PERIODO_AGREGACION,
                "PERCENTILES": PERCENTILES,
                "folder_coeff": tuple_folder_coeff,
                "stable_version": stable_version
            }

            # save the info in a json file
            with open(os.path.join(folder_output_dir, "processing_parameters.json"), 'w') as f:
                json.dump(info_dict, f)
            logger.info(f"Saved processing_parameters.json in {folder_output_dir}")
            #########################################################################
            #########################################################################




            # TRANSFORMING 1 SECOND DATA TO 1 HOUR DATA
            logger.info(f"Transforming 1 second data to 1 hour data")
            folder_output_dir_1h = os.path.join(folder_output_dir, '1h_analysis')
            os.makedirs(folder_output_dir_1h, exist_ok=True)

            # nivel de cada segundo en una hora MAKE SURE
            df_1h = transform_1h(df, slm_dict, logger, agg_period=3600)
            df_1h = df_1h.round(2)
            # convert index to a column
            df_1h.reset_index(inplace=True)
            
            # add random data to the dataframe
            df_1h_fake = add_random_data(df_1h)
            df_1h_fake = df_1h_fake.round(2)
            logger.info(f"Data transformed to 1 hour data")
            
            folder_output_dir_1h_folder = os.path.join(folder_output_dir_1h, 'ALARMS')
            os.makedirs(folder_output_dir_1h_folder, exist_ok=True)


            # transformation
            logger.info(f"TRANSFORMATION SECTION")
            df_1h = transformation(df_1h, logger, LIMITE_DIA, LIMITE_TARDE, LIMITE_NOCHE)
            df_1h_fake = transformation_fake(df_1h_fake, logger, LIMITE_DIA, LIMITE_TARDE, LIMITE_NOCHE)

            # remove the 'date_time' column
            df_1h = df_1h.drop(columns=['date_time'])

            # save processed csvs file
            df_1h.to_csv(os.path.join(folder_output_dir_1h, f"{folder}_1h_processed.csv"), index=True)
            logger.info(f"Data saved to {os.path.join(folder_output_dir_1h, f'{folder}_1h_processed.csv')}")

            df_1h_fake.to_csv(os.path.join(folder_output_dir_1h, f"{folder}_1h_fake_processed.csv"), index=True)
            logger.info(f"Data saved to {os.path.join(folder_output_dir_1h, f'{folder}_1h_fake_processed.csv')}")


            ############# 
            # PREPARING THE MERGE DATA FOR THE PEAK PREDICTION
            #############
            # print(type(peak_prediction_csv_file['classes'].iloc[0]))
            start_date = peak_prediction_csv_file.index[0]
            end_date = peak_prediction_csv_file.index[-1]

            # save the peak prediction file
            peak_prediction_csv_file.to_csv(os.path.join(folder_output_dir_graphic_analysis, f"{folder}_peak_predictions.csv"), index=True)
            logger.info(f"Data saved to {os.path.join(folder_output_dir_graphic_analysis, f'{folder}_peak_predictions.csv')}")

            peak_prediction_csv_file['classes'] = peak_prediction_csv_file['classes'].apply(ast.literal_eval)
            df_exploded = peak_prediction_csv_file.explode('classes')
            
            df_merged = pd.merge(df_exploded, yamnet_csv, left_on='classes', right_on='display_name', how='left')
            # rename columns
            df_merged = df_merged.rename(columns={'classes':'class', 'leq':'LAeq'})

            # save
            df_merged.to_csv(os.path.join(folder_output_dir_graphic_analysis, f"{folder}_merged_peak_predictions.csv"), index=True)
            logger.info(f"Data saved to {os.path.join(folder_output_dir_graphic_analysis, f'{folder}_merged_peak_predictions.csv')}")
            ################


            logger.info("")
            logger.info(f"PLOTTING SECTION")

            if OCA_ALARM:
                logger.info(f"[1] Plotting OCA alarm for folder {folder}")
                oca_alarm(df_1h, folder_output_dir_1h_folder, logger, plotname=folder)
            
            
            if LMAX_ALARM:
                logger.info(f"[2] Plotting LMAX alarm for folder {folder}")
                lmax_alarm(df_1h, folder_output_dir_1h_folder, logger, plotname=folder, threshold=95) # OCA +10

            
            if LC_LA_ALARM:
                logger.info(f"[3] Plotting LC-LA alarm for folder {folder}")
                LC_LA_alarm(df_1h, folder_output_dir_1h_folder, logger, plotname=folder, threshold_norma=10, threshold_dB=3)


            if L90_ALARM:
                logger.info(f"[4] Plotting L90 alarm for folder {folder}")
                l90_alarm(df_1h, folder_output_dir_1h_folder, logger, plotname=folder, threshold_dB=5)


            if L90_ALARM_DYNAMIC:
                logger.info(f"[4] Plotting L90 alarm for folder {folder}")
                l90_alarm_dynamic(df_1h, folder_output_dir_1h_folder, logger, plotname=folder, threshold_dB=5)


            if FREQUENCY_COMPOSITION:
                logger.info(f"[5] Plotting frequency composition for folder {folder}")            
                frequency_composition(df_oct, folder_output_dir_1h_folder, logger, plotname=folder, threshold_comp=5)


            if TONAL_FREQUENCY:
                logger.info(f"[6] Plotting tonal frequency for folder {folder}")
                tonal_frequency(df_oct, folder_output_dir_1h_folder, logger, plotname=folder)


            if PLOT_PEAK_PREDICTION:
                logger.info(f"[7] Plotting peak prediction for folder {folder}")
                plot_peak_predictions(df_merged, predictions_peak_folder, start_date, end_date, logger, plotname=folder)


            if PLOT_PEAK_DISTRI_HEATMAP:
                logger.info(f"[8] Plotting peak heatmap for folder {folder}")
                plot_peak_distribution_heatmap(df_merged, folder_output_dir_graphic_analysis, logger, plotname=folder)


            if PLOT_PEAK_DISTRI:
                logger.info(f"[9] Plotting peak distribution for folder {folder}")
                plot_peak_distribution(df_merged, folder_output_dir_graphic_analysis, logger, plotname=folder)


            if PLOT_DENSITY_DISTRI:
                logger.info(f"[10] Plotting density distribution for folder {folder}")
                plot_density_distribution_peaks(df_merged, folder_output_dir_graphic_analysis, logger, plotname=folder)


            if PLOT_BOX_PLOT_PREDICTION:
                logger.info(f"[11] Plotting box plot prediction for folder {folder}")
                plot_box_plot_prediction(df_merged, folder_output_dir_graphic_analysis, logger, plotname=folder)

            
            if PLOT_SPECTROGRAM_1_3:
                logger.info(f"[12] Plotting spectrogram for folder {folder}")
                plt_spectrogram(df, folder_output_dir, logger, plotname=folder)

                


        #     if PLOT_NIGHT_EVOLUTION:
        #         logger.info(f"[1] Plotting night evolution for folder {folder}")
        #         plot_night_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")
            
        #     # Plotting night evolution 15 min
        #     if PLOT_NIGHT_EVOLUTION_15_MIN:
        #         logger.info(f"[2] Plotting night evolution 15 min for folder {folder}")
        #         plot_night_evolution_15_min(df, folder_output_dir, logger, name_extension="15_min", laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")


        #     # Plotting LEq power average with predictions
        #     if PLOT_PREDIC_LAEQ_15_MIN:
        #         logger.info(f"[3] Plotting PLOT_PREDIC_LAEQ for folder {folder}")
        #         plot_predic_laeq_15_min(df, yamnet_csv, taxonomy, prediction_csv_file, predictions_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)

            
        #     if PLOT_PREDIC_LAEQ_15_MIN_PERIOD:
        #         logger.info(f"[4] Plotting PLOT_PREDIC_LAEQ_PERIOD for folder {folder}")
        #         plot_predic_laeq_15_min_period(df, yamnet_csv, taxonomy, prediction_csv_file, predictions_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)


        #     if PLOT_PREDIC_LAEQ_15_MIN_4H:
        #         logger.info(f"[5] Plotting PLOT_PREDIC_LAEQ_4H for folder {folder}")
        #         plot_predic_laeq_15_min_4h(df, yamnet_csv,taxonomy, prediction_csv_file, predictions_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)


        #     # Plotting stack bar with predictions class
        #     if PLOT_PREDICTION_STACK_BAR:
        #         logger.info(f"[6] Plotting PLOT_PREDICTION_STACK_BAR for folder {folder}")
        #         plot_prediction_stack_bar(prediction_csv_file, yamnet_csv, taxonomy, predictions_visualization_folder, logger, plotname=folder)
            

        #     # Plotting prediction map
        #     if PLOT_PREDICTION_MAP:
        #         logger.info(f"[7] Plotting PLOT_PREDICTION_MAP for folder {folder}")
        #         plot_prediction_map(prediction_csv_file, taxonomy, predictions_visualization_folder, logger, plotname=folder)

            
        #     # Plotting tree map
        #     if PLOT_TREE_MAP:
        #         logger.info(f"[8] Plotting PLOT_TREE_MAP for folder {folder}")
        #         plot_tree_map(prediction_csv_file,taxonomy,predictions_visualization_folder, logger, plotname=folder)

            
        #     # Plotting time plot
        #     if PLOT_MAKE_TIME_PLOT:
        #         logger.info(f"[9] Plotting time plot for folder {folder}")
        #         make_time_plot(df, folder_output_dir, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder, percentiles=PERCENTILES)
            

        #     # Plotting heatmap evolution hour
        #     if PLOT_HEATMAP_EVOLUTION_HOUR:
        #         logger.info(f"[10] Plotting heatmap for folder {folder}")
        #         plot_heatmap_evolution_hour(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)
            
            
        #     # Plotting heatmap evolution 15 min
        #     if PLOT_HEATMAP_EVOLUTION_15_MIN:
        #         logger.info(f"[11] Plotting heatmap 15 min for folder {folder}")
        #         plot_heatmap_evolution_15_min(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)
            

        #     # Plotting individual heatmap
        #     if PLOT_INDICADORES_HEATMAP:
        #         logger.info(f"[12] Plotting indicadores heatmap for folder {folder}")
        #         plot_indicadores_heatmap(df, folder_output_dir, logger, plotname=folder, ind_column=slm_dict["LAEQ_COLUMN_COEFF"])


        #     # Plotting day evolution
        #     if PLOT_DAY_EVOLUTION:
        #         logger.info(f"[13] Plotting day evolution for folder {folder}")
        #         plot_day_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)
            

        #     # Plotting period evolution
        #     if PLOT_PERIOD_EVOLUTION:
        #         logger.info(f"[14] Plotting period evolution (1) Ld (2) Le for folder {folder}")
        #         plot_period_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)


        except Exception as e:
            logger.error(f"An error occurred while processing folder {folder}: {e}")