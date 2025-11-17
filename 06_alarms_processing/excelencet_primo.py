def slm_dic():
    logger.info("")    
    logger.info(f"Creating slm_dict")    
    folder = folder.split("/")[-1]
    
    # add slm_dict column LAEQ_COLUMN_COEFF: with the value of LA_corrected
    slm_dict["LAEQ_COLUMN_COEFF"] = 'LA_corrected'
    slm_dict["LAMAX_COLUMN_COEFF"] = 'LAmax_corrected'
    slm_dict["LAMIN_COLUMN_COEFF"] = 'LAmin_corrected'
    slm_dict["LC-LA_COLUMN_COEFF"] = 'LCeq-LAeq_corrected'
    # slm_dict["L90_COLUMN_COEFF"] = '90percentile'


    # SAVE THE INFO IN A JSON FILE
    info_dict = {
        "PERIODO_AGREGACION": PERIODO_AGREGACION,
        "PERCENTILES": PERCENTILES,
        "stable_version": stable_version,
        "oca_limits": oca_limits,
        "oca_type": oca_type,
    }

    # save the info in a json file
    with open(os.path.join(folder_output_dir, "processing_parameters.json"), 'w') as f:
        json.dump(info_dict, f)
    logger.info(f"Saved processing_parameters.json in {folder_output_dir}")

    return



def ships_schedule():
    ######################
    # this is working
    logger.info("")
    logger.info(f"Adding the ships on dock to the general dataframe")


    logger.info(f"Ship dock 1h")
    ship_dock_1h_path = os.path.join(home_dir, RELATIVE_PATH_SHIPS_1H)
    #check if the file exists
    if not os.path.exists(ship_dock_1h_path):
        logger.error(f"File {ship_dock_1h_path} does not exist. Please check the path.")
    else:
        logger.info(f"File {ship_dock_1h_path} exist!")
    df_ship_dock_1h = pd.read_csv(ship_dock_1h_path, parse_dates=['date_time'])
    # this is working. up to here
    #####################
    


    logger.info(f"Ship dock 15min")
    ship_dock_15min_path = os.path.join(home_dir, RELATIVE_PATH_SHIPS_15MIN)
    #check if the file exists
    if not os.path.exists(ship_dock_15min_path):
        logger.error(f"File {ship_dock_15min_path} does not exist. Please check the path.")
    else:
        logger.info(f"File {ship_dock_15min_path} exist!")
    df_ship_dock_15min = pd.read_csv(ship_dock_15min_path, parse_dates=['datetime'])
    

    df_ship_1s_csv_path = os.path.join(folder_output_dir, f"{actual_folder_name}_ship_1s.csv")
    if os.path.exists(df_ship_1s_csv_path):
        logger.info(f"File {df_ship_1s_csv_path} already exists, skipping ship dock 1s analysis")
        df = pd.read_csv(df_ship_1s_csv_path)
        logger.info(f"Loaded ships on dock 1s dataframe from {df_ship_1s_csv_path}")
        # continue
    
    else:
        logger.info(f"Ship dock 1s")
        ship_dock_1s_path = os.path.join(home_dir, RELATIVE_PATH_SHIPS_1S)
        #check if the file exists
        if not os.path.exists(ship_dock_1s_path):
            logger.error(f"File {ship_dock_1s_path} does not exist. Please check the path.")
        else:
            logger.info(f"File {ship_dock_1s_path} exist!")
        df_ship_dock_1s = pd.read_csv(ship_dock_1s_path, parse_dates=['date_time'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

        # df = Timestamp
        # df_ship_dock_1s = date_time

        # merging
        logger.info(f"Merging the ships on dock dataframes with the main dataframe")
        df = df.merge(
            df_ship_dock_1s[['date_time', 'nships']],
            left_on='Timestamp',
            right_on='date_time',
            how='left'
        )
        # remove the date_time column from the df_ship_dock_1s
        df.drop(columns=['date_time'], inplace=True, errors='ignore')

        df.to_csv(df_ship_1s_csv_path, index=False)
        logger.info(f"Saved ships on dock 1s dataframe to {df_ship_1s_csv_path}")
        # if the values inside the nships is 1 or more, then 1, else 0
        df_alarms_1h['ships_alarm'] = df_alarms_1h['ships_alarm'].apply(lambda x: 1 if x > 0 else 0)
    
    return