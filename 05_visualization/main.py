import argparse
import os
from logging_config import setup_logging
import config
from config import *
from processing import *
import re


def arg_parser():
    parser = argparse.ArgumentParser(description='Plotting AudioMoth data')
    parser.add_argument('-f', '--path_general', type=str, required=True, 
                        help='Path to sonometers folder')
    parser.add_argument('-a', '--agg_period', type=int, required=False, default=900, 
                        help='Aggregation period in seconds')
    parser.add_argument('-o', '--output-dir', type=str, required=False, 
                        help='Output directory, if not provided, the output directory is the same as the input directory')
    parser.add_argument('-p', '--percentiles', type=float, nargs='+', required=False, default=[90, 10],
                        help='Percentiles to plot [1 5 10 50 90] (L90 and L10 as default)')
    parser.add_argument('-l', '--limit_oca', type=str, required=False, default='OCA_RESIDENTIAL',
                        help='Limit OCA to plot [OCA_RESIDENTIAL, OCA_LEISURE, OCA_OFFICE, OCA_INDUSTRIAL, OCA_CULTURE]')
    parser.add_argument('--audiomoth', action='store_true', 
                        help='Process audiomoth data')
    parser.add_argument('--sonometer', action='store_true', 
                        help='Process sonometer data')
    #urban or port taxonomy
    parser.add_argument('--urban', action='store_true', 
                        help='Urban taxonomy')
    parser.add_argument('--port', action='store_true', 
                        help='Port taxonomy')
    # ask the user to change the date/time
    parser.add_argument('--change-date', action='store_true',
                        help='Change the date and the time of the csv file')
    return parser.parse_args()


def main():
    """
    usage example:
    python main.py -f \\192.168.205.117\AAC_Server\OCIO\OCIO_BILBAO\FASE_3 --audiomoth --urban
    
    or

    python main.py -f \\192.168.205.117\AAC_Server\OCIO\OCIO_BILBAO\FASE_3 --audiomoth --urban -l OCA_LEISURE
    """
    logger = setup_logging()
    args = arg_parser()
    yamnet_csv = yamnet_class_map_csv()
    urban_taxonomy_map, port_taxonomy_map = taxonomy_json()
    
    
    if args.path_general:
        input_folder = args.path_general
    else:
        raise ValueError("Path not provided")
    if args.agg_period:
        PERIODO_AGREGACION = args.agg_period
    else:
        PERIODO_AGREGACION = config.PERIODO_AGREGACION
    if args.percentiles:
        PERCENTILES = args.percentiles

    # user to change the date and/or time
    if args.change_date:
        CHANGE_DATE_TIME = True
    else:
        CHANGE_DATE_TIME = False


    # CHOICE OCA TYPE
    oca_type = args.limit_oca
    if oca_type == 'OCA_RESIDENTIAL':
        oca_limits = config.OCA_RESIDENTIAL
    elif oca_type == 'OCA_LEISURE':
        oca_limits = config.OCA_LEISURE
    elif oca_type == 'OCA_OFFICE':
        oca_limits = config.OCA_OFFICE
    elif oca_type == 'OCA_INDUSTRIAL':
        oca_limits = config.OCA_INDUSTRIAL
    elif oca_type == 'OCA_CULTURE':
        oca_limits = config.OCA_CULTURE
    else:
        raise ValueError(f"Unknown OCA type {oca_type!r}, must be 'OCA_RESIDENTIAL', 'OCA_LEISURE', 'OCA_OFFICE', 'OCA_INDUSTRIAL', 'OCA_CULTURE'")


    try:
        folder_coefficients = {}
        folder_date_time = {}
        folder_threshold = {}
        

        # -------------------------------------
        # A U D I O M O T H   P R O C E S S I N G
        # -------------------------------------
        if args.audiomoth:
            logger.info("Processing audiomoth data")
            if args.urban:
                taxonomy = urban_taxonomy_map
            elif args.port:
                taxonomy = port_taxonomy_map
            else:
                taxonomy = urban_taxonomy_map
            
            spl_audiomoth_folders = []
            for root, dirs, files in os.walk(input_folder):
                if "acoustic_params_query" in dirs:

                    spl_audiomoth_folder = os.path.join(root, "acoustic_params_query")
                    if os.path.exists(spl_audiomoth_folder):
                        # ask user for the correction coefficient
                        spl_audiomoth_folder_name = spl_audiomoth_folder.split("\\")[-2]
                        coeff = float(input(f"Enter correction coefficient for {spl_audiomoth_folder_name}: "))

                        # default values
                        new_date = None
                        new_time = None
                        threshold_date = None
                        threshold_time = None

                        if CHANGE_DATE_TIME:
                            # DATE
                            date_to_change = input("Would you like to change the date of the csv file? (y/n): ").lower()
                            while date_to_change not in ['y', 'n']:
                                date_to_change = input("Would you like to change the date of the csv file? (y/n): ").lower()
                            
                            if date_to_change == 'y':
                                new_date = input("Enter the new date (yyyy-mm-dd): ")
                                while not re.match(r"\d{4}-\d{2}-\d{2}", new_date):
                                    new_date = input("Enter the new date (yyyy-mm-dd): ")
                            
                            # TIME
                            time_to_change = input("Would you like to change the time of the csv file? (y/n): ").lower()
                            while time_to_change not in ['y', 'n']:
                                time_to_change = input("Would you like to change the time of the csv file? (y/n): ").lower()

                            if time_to_change == 'y':
                                new_time = input("Enter the new time (hh:mm:ss): ")
                                while not re.match(r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]", new_time):
                                    new_time = input("Enter the new time (hh:mm:ss): ")

                            # LIMIT DATE
                            threshold_to_change = input("Would you like to set a limit on the date to process the data? (y/n): ").lower()
                            while threshold_to_change not in ['y', 'n']:
                                threshold_to_change = input("Would you like to set a limit on the date to process the data? (y/n): ").lower()
                            
                            if threshold_to_change == 'y':
                                threshold_date = input("Enter the threshold date (yyyy-mm-dd): ")
                                while not re.match(r"\d{4}-\d{2}-\d{2}", threshold_date):
                                    threshold_date = input("Enter the threshold date (yyyy-mm-dd): ")

                            #LIMIT TIME
                            threshold_time_to_change = input("Would you like to set a limit on the time to process the data? (y/n): ").lower()
                            while threshold_time_to_change not in ['y', 'n']:
                                threshold_time_to_change = input("Would you like to set a limit on the time to process the data? (y/n): ").lower()

                            if threshold_time_to_change == 'y':
                                threshold_time = input("Enter the threshold time (hh:mm:ss): ")
                                while not re.match(r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]", threshold_time):
                                    threshold_time = input("Enter the threshold time (hh:mm:ss): ")

                        
                        # ADD TO THE DICTIONARIES
                        folder_coefficients[spl_audiomoth_folder] = coeff
                        spl_audiomoth_folders.append(spl_audiomoth_folder)

                        folder_date_time[spl_audiomoth_folder] = (new_date, new_time)
                        folder_threshold[spl_audiomoth_folder] = (threshold_date, threshold_time)



            # logger the info from the process_all_folders function
            logger.info("")
            logger.info(f"Using percentiles {PERCENTILES}")
            logger.info(f"Using aggregation period {PERIODO_AGREGACION}")
            # logger.info(f"Using taxonomy {taxonomy}")
            logger.info(f"Using yamnet csv {yamnet_csv}")
            logger.info(f"Using folder coefficients {folder_coefficients}")
            logger.info(f"Using folder date time {folder_date_time}")
            logger.info(f"Using folder threshold {folder_threshold}")
            logger.info(f"Using OCA type {oca_type}")


            process_all_folders(
                input_folder,
                spl_audiomoth_folders,
                PERIODO_AGREGACION,
                PERCENTILES,
                taxonomy,
                yamnet_csv,
                'AUDIOMOTH',
                folder_coefficients,
                folder_date_time,
                folder_threshold,
                oca_limits,
                oca_type,
                logger
            )



        # -------------------------------------
        # S O N O M E T E R   P R O C E S S I N G
        # -------------------------------------
        if args.sonometer:
            logger.info("Processing sonometer data")
            
            if args.urban:
                taxonomy = urban_taxonomy_map
            elif args.port:
                taxonomy = port_taxonomy_map
            else:
                taxonomy = urban_taxonomy_map
            
            spl_sonometer_folders = []
            for root, dirs, files in os.walk(input_folder):
                if 'acoustic_params_query' in dirs:
                    spl_sonometer_folder = os.path.join(root, "acoustic_params_query")
                    if os.path.exists(spl_sonometer_folder):
                        
                        # correction coefficient
                        spl_sonometer_folder_name = spl_sonometer_folder.split("\\")[-2]
                        coeff = float(input(f"Enter correction coefficient for {spl_sonometer_folder_name}: "))
                        
                        new_date = None
                        new_time = None
                        threshold_date = None
                        threshold_time = None

                        if CHANGE_DATE_TIME:
                            # DATE
                            date_to_change = input("Would you like to change the date of the csv file? (y/n): ").lower()
                            while date_to_change not in ['y', 'n']:
                                date_to_change = input("Would you like to change the date of the csv file? (y/n): ").lower()

                            if date_to_change == 'y':
                                new_date = input("Enter the new date (yyyy-mm-dd): ")
                                while not re.match(r"\d{4}-\d{2}-\d{2}", new_date):
                                    new_date = input("Enter the new date (yyyy-mm-dd): ")
                            
                            # TIME
                            time_to_change = input("Would you like to change the time of the csv file? (y/n): ").lower()
                            while time_to_change not in ['y', 'n']:
                                time_to_change = input("Would you like to change the time of the csv file? (y/n): ").lower()

                            if time_to_change == 'y':
                                new_time = input("Enter the new time (hh:mm:ss): ")
                                while not re.match(r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]", new_time):
                                    new_time = input("Enter the new time (hh:mm:ss): ")
                            
                            # LIMIT DATE
                            threshold_to_change = input("Would you like to set a limit on the date to process the data? (y/n): ").lower()
                            while threshold_to_change not in ['y', 'n']:
                                threshold_to_change = input("Would you like to set a limit on the date to process the data? (y/n): ").lower()

                            if threshold_to_change == 'y':
                                threshold_date = input("Enter the threshold date (yyyy-mm-dd): ")
                                while not re.match(r"\d{4}-\d{2}-\d{2}", threshold_date):
                                    threshold_date = input("Enter the threshold date (yyyy-mm-dd): ")
                            
                            # LIMIT TIME
                            threshold_time_to_change = input("Would you like to set a limit on the time to process the data? (y/n): ").lower()
                            while threshold_time_to_change not in ['y', 'n']:
                                threshold_time_to_change = input("Would you like to set a limit on the time to process the data? (y/n): ").lower()

                            if threshold_time_to_change == 'y':
                                threshold_time = input("Enter the threshold time (hh:mm:ss): ")
                                while not re.match(r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]", threshold_time):
                                    threshold_time = input("Enter the threshold time (hh:mm:ss): ")

                        
                        
                        # ADD TO THE DICTIONARIES
                        folder_coefficients[spl_sonometer_folder] = coeff
                        spl_sonometer_folders.append(spl_sonometer_folder)

                        folder_date_time[spl_sonometer_folder] = (new_date, new_time)
                        folder_threshold[spl_sonometer_folder] = (threshold_date, threshold_time)


            # logger the info from the process_all_folders function
            logger.info("")
            logger.info(f"Using percentiles {PERCENTILES}")
            logger.info(f"Using aggregation period {PERIODO_AGREGACION}")
            # logger.info(f"Using taxonomy {taxonomy}")
            logger.info(f"Using yamnet csv {yamnet_csv}")
            logger.info(f"Using folder coefficients {folder_coefficients}")
            logger.info(f"Using folder date time {folder_date_time}")
            logger.info(f"Using folder threshold {folder_threshold}")
            logger.info(f"Using OCA type {oca_type}")
            logger.info(f"Input folder {input_folder}")


            process_all_folders(
                input_folder,
                spl_sonometer_folders,
                PERIODO_AGREGACION,
                PERCENTILES,
                taxonomy,
                yamnet_csv,
                'SONOMETRO',
                folder_coefficients,
                folder_date_time,
                folder_threshold,
                oca_limits,
                oca_type,
                logger
            )


        logger.info("Finished sonometer test script")


    except Exception as e:
        logger.exception(f"Error occurred: {e}")



if __name__ == "__main__":
    main()
