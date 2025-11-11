import argparse
import os
from logging_config import setup_logging
from config_vi import *
import config_vi
from processing import *
import re


def arg_parser():
    parser = argparse.ArgumentParser(description='Plotting AudioMoth data')
    parser.add_argument('-f', '--path_general', type=str, required=True, 
                        help='Path to sonometers folder')
    parser.add_argument('-o', '--output-dir', type=str, required=False, 
                        help='Output directory, if not provided, the output directory is the same as the input directory')
    
    parser.add_argument('-a', '--agg_period', type=int, required=False, default=900, 
                        help='Aggregation period in seconds')
    parser.add_argument('-p', '--percentiles', type=float, nargs='+', required=False, default=[90, 10],
                        help='Percentiles to plot [1 5 10 50 90] (L90 and L10 as default)')
    parser.add_argument('-l', '--limit_oca', type=str, required=False, default='OCA_RESIDENTIAL',
                        help='Limit OCA to plot [OCA_RESIDENTIAL, OCA_LEISURE, OCA_OFFICE, OCA_INDUSTRIAL, OCA_CULTURE]')
    
    parser.add_argument('--audiomoth', action='store_true', 
                        help='Process audiomoth data')
    parser.add_argument('--sonometer', action='store_true', 
                        help='Process sonometer data'),
    parser.add_argument('--raspbery', action='store_true',
                        help='Process Raspberry Pi like TCT Tenerife'),
    
    #urban or port taxonomy
    parser.add_argument('--urban', action='store_true', 
                        help='Urban taxonomy')
    parser.add_argument('--port', action='store_true', 
                        help='Port taxonomy')
    
    # ask the user to change the date/time
    parser.add_argument('--change-date', action='store_true',
                        help='Change the date and the time of the csv file')
    return parser.parse_args()



def get_taxonomy(args, urban_taxonomy_map, port_taxonomy_map):
    return port_taxonomy_map if args.port else urban_taxonomy_map



def ask_date_time_changes():
    def ask(prompt, pattern):
        ans = input(prompt).lower()
        while ans not in ['y', 'n']:
            ans = input(prompt).lower()
        if ans == 'y':
            val = input("Enter value: ")
            while not re.match(pattern, val):
                val = input("Enter value: ")
            return val
        return None

    return (
        ask("Change date? (y/n): ", r"\d{4}-\d{2}-\d{2}"),
        ask("Change time? (y/n): ", r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"),
        ask("Set threshold date? (y/n): ", r"\d{4}-\d{2}-\d{2}"),
        ask("Set threshold time? (y/n): ", r"([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]")
    )


def collect_folders(input_folder, change_time_flag,label_source_type, logger):
    folders, coefficients, date_time, threshold = [], {}, {}, {}

    if label_source_type == "raspberry":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if ACOUSTIC_PARAMS_FOLDER in dirs:
                path = os.path.join(root, ACOUSTIC_PARAMS_FOLDER)
                folder_name = path.split("\\")[-2]
                coeff = float(input(f"Correction coefficient for {folder_name}: "))
                new_date = new_time = t_date = t_time = None

                if change_time_flag:
                    new_date, new_time, t_date, t_time = ask_date_time_changes()

                folders.append(path)
                coefficients[path] = coeff
                date_time[path] = (new_date, new_time)
                threshold[path] = (t_date, t_time)


    if label_source_type == "audiomoth":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if "AUDIOMOTH" in dirs:
                path = os.path.join(root, "AUDIOMOTH")
                folder_name = path.split("\\")[-2]
                coeff = float(input(f"Correction coefficient for {folder_name}: "))
                new_date = new_time = t_date = t_time = None

                if change_time_flag:
                    new_date, new_time, t_date, t_time = ask_date_time_changes()

                folders.append(path)
                coefficients[path] = coeff
                date_time[path] = (new_date, new_time)
                threshold[path] = (t_date, t_time)


    if label_source_type == "sonometer":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if "SONOMETER" in dirs:
                path = os.path.join(root, "SONOMETER")
                folder_name = path.split("\\")[-2]
                coeff = float(input(f"Correction coefficient for {folder_name}: "))
                new_date = new_time = t_date = t_time = None

                if change_time_flag:
                    new_date, new_time, t_date, t_time = ask_date_time_changes()

                folders.append(path)
                coefficients[path] = coeff
                date_time[path] = (new_date, new_time)
                threshold[path] = (t_date, t_time)

    return folders, coefficients, date_time, threshold



def resolve_oca_type(oca_type):
    oca_map = {
        'OCA_RESIDENTIAL': config_vi.OCA_RESIDENTIAL,
        'OCA_LEISURE': config_vi.OCA_LEISURE,
        'OCA_OFFICE': config_vi.OCA_OFFICE,
        'OCA_INDUSTRIAL': config_vi.OCA_INDUSTRIAL,
        'OCA_CULTURE': config_vi.OCA_CULTURE,
    }
    if oca_type not in oca_map:
        raise ValueError(f"Invalid OCA type: {oca_type}")
    return oca_map[oca_type]




def main():
    try:
        logger = setup_logging()
        args = arg_parser()

        taxonomy = get_taxonomy(args, *taxonomy_json())
        oca_limits = resolve_oca_type(args.limit_oca)
        yamnet_csv = yamnet_class_map_csv()
        input_folder = args.path_general

        source_types = {
            "AUDIOMOTH": args.audiomoth,
            "SONOMETRO": args.sonometer,
            "RASPBERRY": args.raspbery,
        }

        for label, active in source_types.items():
            logger.info(f"Active: {active}")
            logger.info(f"Trying to get label: {label}")
            if not active:
                continue
            label_source_type =label.lower()
            logger.info(f"Processing {label_source_type} data")
            # exit()

            ############################
            folders, coeffs, date_map, thresh_map = collect_folders(input_folder, args.change_date, label_source_type,logger)

            logger.info(f"Using percentiles {args.percentiles}")
            logger.info(f"Aggregation period {args.agg_period}")
            logger.info(f"Taxonomy: {taxonomy}")
            logger.info(f"Input folder: {input_folder}")


            logger.info("Entering the process all folder function")
            
            process_all_folders(
                input_folder,
                folders,
                args.agg_period,
                args.percentiles,
                taxonomy,
                yamnet_csv,
                label_source_type,
                coeffs,
                date_map,
                thresh_map,
                oca_limits,
                args.limit_oca,
                logger
            )

        logger.info("Finished all processing.")

    except Exception as e:
        logger.error(f"Error during executing the main program: {e}")


if __name__ == "__main__":
    main()
