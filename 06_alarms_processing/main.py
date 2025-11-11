import argparse
import os
import re
from .logging_config import setup_logging
# from config_vi import *
from . import config_vi 
from .processing import *
from .utils_vi import *



def arg_parser():
    parser = argparse.ArgumentParser(description='Plotting AudioMoth data')
    parser.add_argument('-f', '--path_general', type=str, required=True, 
                        help='Path to sonometers folder')
    
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
    return parser.parse_args()



def collect_folders(input_folder,label_source_type, logger):
    folders = []

    if label_source_type == "raspberry":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if config_vi.MERGED_FOLDER in dirs:
                path = os.path.join(root, config_vi.MERGED_FOLDER)
                folders.append(path)


    if label_source_type == "audiomoth":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if "AUDIOMOTH" in dirs:
                path = os.path.join(root, "AUDIOMOTH")
                folders.append(path)


    if label_source_type == "sonometer":
        logger.info("Searching for RASPBERRY")
        for root, dirs, _ in os.walk(input_folder):
            if "SONOMETER" in dirs:
                path = os.path.join(root, "SONOMETER")
                folders.append(path)

    return folders



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
    """
    execution
        python3 -m 06_alarms_processing.main -f "\192.168.205.120\Contenedores\5-Resultados\P5_TEST" --raspbery --port
    """
    try:
        logger = setup_logging()
        args = arg_parser()
        logger.info(f"Starting alarm processing!!")
        yamnet_csv = yamnet_class_map_csv()
        urban_taxonomy_map, port_taxonomy_map = taxonomy_json()
        taxonomy,taxonomy = args.urban,args.port

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

            ############################
            folders = collect_folders(input_folder, label_source_type,logger)

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
                logger)

        logger.info("Finished all processing.")

    except Exception as e:
        logger.error(f"Error during executing the main program: {e}")


if __name__ == "__main__":
    main()
