################################################################
# PEAK ANALYSIS
##################################################################
logger.info("")
logger.info(f"PEAKS PLOTTING!!!")

logger.info(f"[8] Plotting peak heatmap for folder {folder}")
plot_peak_distribution_heatmap(df_alarms_1h, folder_output_dir_1h, logger, plotname=folder)


logger.info(f"[9] Plotting peak distribution for folder {folder}")
plot_peak_distribution(df_alarms_1h, folder_output_dir_1h, logger, plotname=folder)


logger.info(f"[10] Plotting density distribution for folder {folder}")
plot_density_distribution_peaks(df_alarms_1h, folder_output_dir_1h, logger, plotname=folder)



# #####################################################
# PLOTTING PREDICTION SECTION
# #####################################################
logger.info("")
logger.info(f"PREDICTION PLOTTING!!!")

logger.info(f"[11] Plotting PLOT_PREDIC_LAEQ for folder {folder}")
plot_predic_peak_laeq_mean(df_alarms_1h, ia_visualization_folder, logger, plotname=folder)


logger.info(f"[12] Plotting box plot prediction for folder {folder}")
plot_box_plot_prediction(df_alarms_1h, ia_visualization_folder, logger, plotname=folder)


logger.info(f"[13] Plotting heatmap prediction for folder {folder}")
plot_heat_map_prediction(df_alarms_1h, ia_visualization_folder, logger, plotname=folder)