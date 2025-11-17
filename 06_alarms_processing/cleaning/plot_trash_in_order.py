def plot_regular():
    # Plotting night evolution
    if PLOT_NIGHT_EVOLUTION:
        logger.info(f"[1.1] Plotting night evolution for folder {folder}")
        plot_night_evolution(df, folder_output_dir, logger, laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")


    # Plotting night evolution 15 min
    if PLOT_NIGHT_EVOLUTION_15_MIN:
        logger.info(f"[2.1] Plotting night evolution 15 min for folder {folder}")
        plot_night_evolution_15_min(df, folder_output_dir, logger, name_extension="15_min", laeq_column=slm_dict["LAEQ_COLUMN_COEFF"], plotname=folder, indicador_noche="Ln")



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






############################ PREDICTION PLOTTING SECTION ####################################################################################

def plot_prediction():
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





def plot_alarm():
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