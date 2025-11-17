import pandas as pd
import matplotlib.pyplot as plt
import os
plt.style.use("bmh")
from tqdm import tqdm
import glob
import json
from scipy.signal import find_peaks
import ast

from .visualization import *
from .reading import *
from .utils_vi import *
from config_vi import *



def process_all_folders(input_folder, folders, PERIODO_AGREGACION, PERCENTILES, taxonomy, yamnet_csv, sufix_string, oca_limits, oca_type, logger):
    print()
    stable_version = get_stable_version(logger)
    home_dir = os.path.expanduser('~')



    for folder in tqdm(folders, desc="Processing folders"): # \\192.168.205.117\AAC_Server\OCIO\24052_ZARAUTZ\CAMPAÑA_1\3-Medidas\ZARAUTZ_C1_P1\AUDIOMOTH
        logger.info("")
        logger.info(f"Suffix string: {sufix_string}")


        # making the processed files txt to avoind repeting processing 
        processed_list_path = os.path.join(folder,f"processed_csv_{sufix_string}_{stable_version}.txt")
        if os.path.exists(processed_list_path):
            with open(processed_list_path, "r", encoding="utf-8") as f:
                processed_csvs = {line.strip() for line in f if line.strip()}
        else:
            processed_csvs = set()


        yamnet_df = yamnet_csv[[
            # "mid",
            "display_name",
            # "iso_taxonomy",
            # "Brown_Level_2",
            # "Brown_Level_3",
            "NoisePort_Level_1",
            # "NoisePort_Level_2",
        ]]


        #############################
        ## GETTING THE DATAFRAME ###
        #############################
        try:
            logger.info("")
            logger.info(f"Processing folder {folder}") 
            logger.info(f"Getting the data from the dataframes")
            
            
            if sufix_string == "raspberry":
                logger.info("Processing RASPBERRY data")
                csv_files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
                csv_paths = [os.path.join(folder, f) for f in csv_files]
                logger.info(f"Found {len(csv_paths)} CSV files in {folder}")

                for csv_path in csv_paths:
                    csv_path_abs = os.path.abspath(csv_path)

                    #skip processed files
                    if csv_path_abs in processed_csvs:
                        logger.info(f"Skipping already processed file: {csv_path_abs}")
                        continue

                    logger.info(f"Processing CSV: {csv_path_abs}")
                    df=pd.read_csv(csv_path_abs)

                    # ─────────────
                    # CLEAN / TIDY
                    # ─────────────
                    # [0] datetimecolumn
                    df["datetime"] = pd.to_datetime(df["Timestamp"])
                    df["datetime"] = df["datetime"].dt.tz_localize(None)

                    # [1] rename
                    if "Filename_acoustic" in df.columns:
                        df = df.rename(columns={"Filename_acoustic": "Filename"})


                    if "Prediction_1" in df.columns:
                        df = df.merge(
                            yamnet_df,
                            how="left",
                            left_on="Prediction_1",
                            right_on="display_name",
                        )
                    else:
                        logger.warning("Prediction_1 column not found in df; cannot merge YAMNet taxonomy")


                    # drop other filenames columns
                    cols_to_drop = []
                    for col in ["Filename_prediction", "peak_filename", "Prediction_2", "Prediction_3", "Prob_2", "Prob_3", "display_name"]:
                        if col in df.columns:
                            cols_to_drop.append(col)
                    if cols_to_drop:
                        df = df.drop(columns=cols_to_drop)
 

                    # [2] desired order
                    base_cols = [
                        "id_micro",
                        "Filename",
                        "datetime",
                        "Timestamp",
                        "Unixtimestamp",
                        "LA", "LC", "LZ", "LAmax", "LAmin",
                    ]

                    # [3] 1/3 octave bands
                    band_cols = [c for c in df.columns if c.endswith("Hz")]

                    # [4 ] prediction columns
                    pred_cols = [c for c in df.columns
                                if c.startswith("Prediction_") or c.startswith("Prob_")]

                    taxonomy_cols = []
                    if "NoisePort_Level_1" in df.columns:
                        taxonomy_cols = ["NoisePort_Level_1"]

                    # [5] peak columns
                    peak_cols = [
                        "is_peak",
                        "peak_start_time",
                        "peak_end_time",
                        "peak_duration",
                        "peak_leq",
                        "peak_LA_values",
                    ]
                    peak_cols = [c for c in peak_cols if c in df.columns]

                    # 6] rerange
                    ordered_cols = [
                        c for c in base_cols + band_cols + pred_cols+taxonomy_cols + peak_cols if c in df.columns
                    ]

                    df = df[ordered_cols]
                    df = df.sort_values("datetime").reset_index(drop=True)

                    #save
                    # base, ext = os.path.splitext(csv_path_abs)
                    # corrected_path = base + "_corrected" + ext
                    # df.to_csv(corrected_path, index=False)
                    # logger.info(f"Saved corrected CSV to: {corrected_path}")


            elif sufix_string == "SONOMETRO":
                logger.info(f"Processing SONOMETRO data")
                pass
            
            else:
                logger.error(f"suffix is wrong {sufix_string}")                
            ###################################################################
            ###################################################################
            


            logger.info("")
            if TENERIFE_TIMEZONE:
                df['datetime'] = pd.to_datetime(df['datetime']) - pd.Timedelta(hours=1)
                logger.info(f"Time zone was set to Tenerife")


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
            except Exception as e:
                logger.error(f"An error occurred while adding datetime columns: {e}")



            try:
                logger.info("")
                # if df is not None:
                    # drop the beginning and ending of the measurement (15min)
                    # df = df.loc[start_date + pd.Timedelta(REMOVE_START_TIME, unit='seconds'):end_date - pd.Timedelta(REMOVE_END_TIME, unit='seconds')]
                    # logger.info(f"SPL df was trimmed, {REMOVE_START_TIME} secs from the beggining and {REMOVE_END_TIME} secs from the end")
                    # print(df)
                
                # add indicators column
                if df is not None:
                    logger.info(f"Adding indicators column")
                    df['indicador_str'] = df.apply(lambda x: evaluation_period_str(x['hour']), axis=1)
                
                # add nights column
                if df is not None:
                    logger.info(f"Adding nights column")
                    df['night_str'] = df.apply(lambda x: add_night_column(x['hour'], x['weekday']), axis=1)
            except Exception as e:
                logger.error(f"An error occurred while adding indicators and nights columns: {e}")


            try:
                # add oca column
                logger.info(f"Adding oca column")
                logger.info(f"OCA Limits --> {oca_limits}")
                if df is not None:
                    df['oca'] = df['hour'].apply(lambda h: db_limit(h, **oca_limits))
            except Exception as e:
                logger.error(f"An error occurred while adding oca column: {e}")
                

            try:
                logger.info("")
                logger.info("FILTERING PREDICTIONS")
                if "Prediction_1" in df.columns and "Prob_1" in df.columns:
                    mask = df["Prob_1"] >= PROBABILITY_THRESHOLD
                    cols_to_clear = ["Prediction_1", "Prob_1"]
                    if "NoisePort_Level_1" in df.columns:
                        cols_to_clear.append("NoisePort_Level_1")
                    # keep row
                    df.loc[~mask, cols_to_clear] = pd.NA

                else:
                    logger.warning("Prediction_1 or Prob_1 column not found in df")

            except Exception as e:
                logger.error(f"An error occurred while processing predictions in folder {folder}: {e}")


            print(df)
            print(df.columns)
            exit()


            ################################################################
            ################################################################
            try:
                ################################################################
                # TRANSFORMING 1 SECOND DATA TO 1 HOUR DATA
                ##################################################################
                logger.info("")
                logger.info(f"Transforming 1 second data to 1 hour data")
                
                logger.info(f"MAKING FOLDER FOR 1H ANALYSIS TO SAVE THE DATA")
                # remove the last part of the folder_output_dir
                folder_output_dir_for_alarms = os.path.dirname(folder_output_dir)
                folder_output_dir_1h = os.path.join(folder_output_dir_for_alarms, 'Graphics_ALARMS')
                os.makedirs(folder_output_dir_1h, exist_ok=True)

                # check if the file exists
                df_1h_csv_path = os.path.join(folder_output_dir_1h, f"{actual_folder_name}_1h.csv")
                # if os.path.exists(df_1h_csv_path):
                #     logger.info(f"File {df_1h_csv_path} already exists, skipping transformation")
                #     df_1h = pd.read_csv(df_1h_csv_path)
                #     logger.info(f"Loaded 1 hour dataframe from {df_1h_csv_path}")
                #     # continue
                
                # else:
                # df_1h = transform_1h(df, slm_dict, logger, agg_period=3600)

                # print(len(df_1h))
                df_1h = transform_1h_pred(df, logger, agg_period=3600)
                df_1h = df_1h.round(2)
                logger.info(f"Transformed 1 second data to 1 hour data")
                print(df_1h)
                exit()



                logger.info("")
                logger.info("")
                logger.info(f"TRANSFORMATION SECTION")
                logger.info(f"Adding oca column")
                logger.info(f"OCA Limits --> {oca_limits}")
                # set index (which is datetime) as column
                df_1h.reset_index(inplace=True)
                
                df_1h = transformation(df_1h, logger, oca_limits)

                #####
                #ssave
                #removeing datetime column and rename datetime_y to datetime
                # df.drop(columns=['datetime'], inplace=True, errors='ignore')
                # df.rename(columns={'datetime_y': 'datetime'}, inplace=True, errors='ignore')
                df_1h.to_csv(df_1h_csv_path, index=False)
                logger.info(f"Saved 1 hour dataframe to {df_1h_csv_path}")

            except Exception as e:
                logger.error(f"An error occurred while transforming 1 second data to 1 hour data: {e}")
                continue




            ###################################
            ###################################
            logger.info("")
            logger.info(f"PLOTTING SECTION")

            # Plotting night evolution
            if PLOT_NIGHT_EVOLUTION:
                logger.info(f"[1.1] Plotting night evolution for folder {folder}")
                plot_night_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")
            

            # Plotting night evolution 15 min
            if PLOT_NIGHT_EVOLUTION_15_MIN:
                logger.info(f"[2.1] Plotting night evolution 15 min for folder {folder}")
                plot_night_evolution_15_min(df, folder_output_dir, logger, name_extension="15_min", laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")




            ############################ PREDICTION PLOTTING SECTION ####################################################################################
            # Plotting LEq power average with predictions
            if PLOT_PREDIC_LAEQ_MEAN:
                logger.info(f"[3.1] Plotting PLOT_PREDIC_LAEQ_MEAN for folder {folder}")
                plot_predic_laeq_mean(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)

            # TODO
            # if PLOT_PREDIC_LAEQ_15_MIN_PERIOD:
            #     logger.info(f"[4.1] Plotting PLOT_PREDIC_LAEQ_15_MIN_PERIOD for folder {folder}")
            #     plot_predic_laeq_15_min_period(df, yamnet_csv, taxonomy, ia_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)

            if PLOT_PREDIC_LAEQ_MEAN_4H:
                logger.info(f"[4.1] Plotting PLOT_PREDIC_LAEQ_MEAN_4H for folder {folder}")
                plot_predic_laeq_mean_4h(df_all_yamnet, df_ship_dock, taxonomy, ia_visualization_folder, logger, plotname=folder)    
                exit()
            
            if PLOT_PREDIC_LAEQ_DAY:
                logger.info(f"[4.1] Plotting PLOT_PREDIC_LAEQ_MEAN_4H for folder {folder}")
                plot_predic_laeq_mean_day(df_all_yamnet, df_ship_dock, taxonomy, ia_visualization_folder, logger, plotname=folder)
                exit()


            # TODO
            # if PLOT_PREDIC_LAEQ_15_MIN_4H:
            #     logger.info(f"[5] Plotting PLOT_PREDIC_LAEQ_4H for folder {folder}")
            #     plot_predic_laeq_15_min_4h(df, yamnet_csv,taxonomy, df_prediction, ia_visualization_folder, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder)


            # TODO
            # # Plotting stack bar with predictions class
            # if PLOT_PREDICTION_STACK_BAR:
            #     logger.info(f"[6] Plotting PLOT_PREDICTION_STACK_BAR for folder {folder}")
            #     plot_prediction_stack_bar(df_prediction, yamnet_csv, taxonomy, ia_visualization_folder, logger, plotname=folder)
            



            if PLOT_PREDICTION_MAP:
                logger.info(f"[7.1] Plotting PLOT_PREDICTION_MAP for folder {folder}")
                df_all_yamnet_1h = plot_prediction_map_new(df_all_yamnet, df_ship_dock, ia_visualization_folder, logger, plotname=folder)
                print(df_all_yamnet_1h)
                print(df_all_yamnet_1h.columns)
                # print(df_all_yamnet_1h['NoisePort_Level_1'].value_counts())
                # exit()

            
            ##############################################################################################################################################

 
            
            # Plotting time plot
            if PLOT_MAKE_TIME_PLOT:
                logger.info(f"[9.1] Plotting time plot for folder {folder}")
                make_time_plot(df, folder_output_dir, logger, columns_dict=slm_dict, agg_period=PERIODO_AGREGACION, plotname=folder, percentiles=PERCENTILES)


            # Plotting heatmap evolution hour
            if PLOT_HEATMAP_EVOLUTION_HOUR:
                logger.info(f"[10.1] Plotting heatmap for folder {folder}")
                plot_heatmap_evolution_hour(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)


            # Plotting heatmap evolution 15 min
            if PLOT_HEATMAP_EVOLUTION_15_MIN:
                logger.info(f"[11] Plotting heatmap 15 min for folder {folder}")
                plot_heatmap_evolution_15_min(df, folder_output_dir, logger, values_column=slm_dict['LAEQ_COLUMN_COEFF'], agg_func=leq,plotname=folder)

            
            # Plotting individual heatmap
            if PLOT_INDICADORES_HEATMAP:
                logger.info(f"[12] Plotting indicadores heatmap for folder {folder}")
                plot_indicadores_heatmap(df, folder_output_dir, logger, plotname=folder, ind_column=slm_dict["LAEQ_COLUMN_COEFF"])

            
            # Plotting day evolution
            if PLOT_DAY_EVOLUTION:
                logger.info(f"[13] Plotting day evolution for folder {folder}")
                plot_day_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)


            # Plotting period evolution
            if PLOT_PERIOD_EVOLUTION:
                logger.info(f"[14] Plotting period evolution (1) Ld (2) Le for folder {folder}")
                plot_period_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder)


            #############################
            ######### PLOTING ALARMS
            ################################
            logger.info("")
            logger.info(f"PLOTTING ALARMS!!!")

            if OCA_ALARM:
                logger.info(f"[1.1] Plotting OCA alarm for folder {folder}")
                df_alarms_1h = oca_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder)
                print(df_alarms_1h)


            if LMAX_ALARM:
                logger.info(f"[2.1] Plotting LMAX alarm for folder {folder}")
                df_alarms_1h=lmax_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold=95) # OCA +10
                print(df_alarms_1h)


            if LC_LA_ALARM:
                logger.info(f"[3.1] Plotting LC-LA alarm for folder {folder}")
                df_alarms_1h=LC_LA_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_norma=10, threshold_dB=3)
                print(df_alarms_1h)


            if L90_ALARM:
                logger.info(f"[4.1] Plotting L90 alarm for folder {folder}")
                l90_alarm(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_dB=5)


            if L90_ALARM_DYNAMIC:
                logger.info(f"[5.1] Plotting L90 alarm dynamic for folder {folder}")
                df_alarms_1h=l90_alarm_dynamic(df_1h, folder_output_dir_1h, logger, plotname=folder, threshold_dB=5)
                print(df_alarms_1h)



            if FREQUENCY_COMPOSITION:
                logger.info(f"[6.1] Plotting frequency composition for folder {folder}")            
                df_alarms_1h =frequency_composition(df, df_alarms_1h, folder_output_dir_1h, logger, plotname=folder, threshold_comp=5)
                print(df_alarms_1h)
                # exit()


            if TONAL_FREQUENCY:
                logger.info(f"[7.1] Plotting tonal frequency for folder {folder}")
                df_alarms_1h = tonal_frequency(df, df_alarms_1h, folder_output_dir_1h, logger, plotname=folder)
                print(df_alarms_1h)
            


            ################################################################
            # PEAK ANALYSIS
            ##################################################################
            logger.info("")
            logger.info(f"PEAKS PLOTTING!!!")
            

            if PLOT_PEAK_DISTRIBUTION_HEATMAP:
                logger.info(f"[8.1] Plotting peak heatmap for folder {folder}")
                df_alarms_1h=plot_peak_distribution_heatmap(df_peaks, df_alarms_1h, folder_output_dir_1h, logger, plotname=folder)


            if PLOT_PEAK_DISTRIBUTION:
                logger.info(f"[9.1] Plotting peak distribution for folder {folder}")
                plot_peak_distribution(df_peaks, folder_output_dir_1h, logger, plotname=folder)


            if PLOT_PEAK_DENSITY_DISTRIBUTION:
                logger.info(f"[10.1] Plotting density distribution for folder {folder}")
                plot_density_distribution_peaks(df_peaks, folder_output_dir_1h, logger, plotname=folder)


            # #####################################################
            # PLOTTING PREDICTION SECTION
            # #####################################################
            if PLOT_PEAK_PREDIC_LAEQ_MEAN:
                logger.info(f"[11.1] Plotting PLOT_PREDIC_LAEQ for folder {folder}")
                plot_predic_peak_laeq_mean(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)


            if PLOT_PEAK_BOX_PLOT_PREDICTION:
                logger.info(f"[12] Plotting box plot prediction for folder {folder}")
                plot_box_plot_prediction(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)



            if PLOT_PEAK_HEATMAT_PREDICTION:
                logger.info(f"[13] Plotting heatmap prediction for folder {folder}")
                plot_heat_map_prediction(df_all_yamnet, taxonomy, ia_visualization_folder, logger, plotname=folder)



            ################################
            ################################
            ################################
            # # ADDING THE NEW ALARMS CALCULATED
            logger.info("")
            logger.info(f"Adding the yamnet taxonomy to the alarms dataframe")
            columns_to_merge = ["datetime", "mid", "iso_taxonomy", "Brown_Level_2", "Brown_Level_3", "NoisePort_Level_1", "NoisePort_Level_2"]
            df_subset = df_all_yamnet_1h[columns_to_merge]


            df_alarms_1h = df_alarms_1h.merge(
                df_subset,
                left_on="date_time",
                right_on="datetime",
                how="left"
            )
            df_alarms_1h.drop(columns=["datetime"], inplace=True)
            logger.info(f"Adding the yamnet taxonomy to the alarms dataframe successful")


            ################################
            ################################
            ################################
            # SAVING THE ALARMS CSV FILE
            logger.info("")
            logger.info(f"SAVING THE ALARMS CSV FILE")

            try:
                alarms_csv_path = os.path.join(folder_output_dir_1h, f"{actual_folder_name}_full_alarms.csv")
                df_alarms_1h.to_csv(alarms_csv_path, index=False)
                logger.info(f"Saved alarms dataframe to {alarms_csv_path}")

            except Exception as e:
                logger.error(f"An error occurred while saving the alarms dataframe: {e}")
                continue
            ################################
            ################################
            ################################



        except Exception as e:
            logger.error(f"An error occurred while processing folder {folder}: {e}")