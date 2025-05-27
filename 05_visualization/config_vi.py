# relative path to the yamnet class map, urban taxonomy and port taxonomy
RELATIVE_PATH_YAMNET_MAP = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\AAC_IA_Puerto\yamnet_class_AAC_v3_0.csv"
RELATIVE_PATH_TAXONOMY_URBAN = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\taxonomies_json\urban_taxonomy_map_v1_0.json"
RELATIVE_PATH_TAXONOMY_PORT = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\taxonomies_json\port_1_taxonomy_mapping_v2.0.json"


# Remove start time and end time
REMOVE_START_TIME = 900
REMOVE_END_TIME = 900


# TENERIFE TIME ZONE (-1 HOUR)
TENERIFE_TIMEZONE = False

# TIME PLOT dB limits
DB_UPPER_LIMIT = 105
DB_LOWER_LIMIT = 30
HOUR_INTERVAL = 5


# HOUR INTERVAL FOR DAY EVOLUTION PLOTS
DB_RANGE_TOP = 105
DB_RANGE_BOTTOM = 30
BD_RANGE_STEP = 5


#Font size
SMALL_SIZE = 10
MEDIUM_SIZE = 12
BIGGER_SIZE = 20
BIGGEST_SIZE = 22
BIGGEST_15_MIN_SIZE = 24
BIGGEST_PREDICT_MIN_SIZE = 50
BIGGEST_PREDICT_TITLE_SIZE = 60



# Time limits for evaluation indicadores
LD_SECONDS = 21600
LE_SECONDS = 7200
LN_SECONDS = 14400


# Plotting Flags there are 15 flags to plot the following plots
PLOT_NIGHT_EVOLUTION = False
PLOT_NIGHT_EVOLUTION_15_MIN = False


# prediction
PLOT_PREDIC_LAEQ_15_MIN = False
PLOT_PREDIC_LAEQ_15_MIN_PERIOD = False
PLOT_PREDIC_LAEQ_15_MIN_4H = False
PLOT_PREDICTION_STACK_BAR = False
PLOT_PREDICTION_MAP = False
PLOT_TREE_MAP = False


PLOT_MAKE_TIME_PLOT = False

PLOT_HEATMAP_EVOLUTION_HOUR = False 
PLOT_HEATMAP_EVOLUTION_15_MIN = False 
PLOT_INDICADORES_HEATMAP = False

PLOT_DAY_EVOLUTION = False
PLOT_PERIOD_EVOLUTION = False

PLOT_SPECTROGRAM_1_3 = False

#### USING OCA
SHOW_OCA = True



#############
OCA_RESIDENTIAL = {
    'ld_limit': 65,
    'le_limit': 65,
    'ln_limit': 55,
}

OCA_LEISURE = {
    'ld_limit': 73,
    'le_limit': 73,
    'ln_limit': 63,
}

OCA_OFFICE = {
    'ld_limit': 70,
    'le_limit': 70,
    'ln_limit': 65,
}

OCA_INDUSTRIAL = {
    'ld_limit': 75,
    'le_limit': 75,
    'ln_limit': 65,
}

OCA_CULTURE = {
    'ld_limit': 60,
    'le_limit': 60,
    'ln_limit': 50,
}


######################## SLM COLUMN MAPS #####################################
"""
Sonometer column maps for different SLMs. The column maps are used to
   standardize the column names of the different SLMs. The column maps are
        LAEQ_COLUMN: LAeq column name
        LAMAX_COLUMN: LAFmax column name
        LAMIN_COLUMN: LAFmin column name

   The column maps are used in the following functions:
        get_data_814
        get_data_824
        get_data_lx_ES
        get_data_lx_EN
        get_data_cesva
        get_data_SV307
        get_data_audio
"""
        
larsonlx_dict = {'LAEQ_COLUMN': 'LAeq',
                 'LAMAX_COLUMN': 'LAFmax',
                 'LAMIN_COLUMN': 'LAFmin'}


larson824_dict = {'LAEQ_COLUMN': 'Leq',
                  'LAMAX_COLUMN': 'Max',
                  'LAMIN_COLUMN': 'Min'}


larson814_dict = {'LAEQ_COLUMN': 'Leq',
                  'LAMAX_COLUMN': 'Max',
                  'LAMIN_COLUMN': 'Min'}


cesva_dict = {'LAEQ_COLUMN': 'LA1s',
              'LAMAX_COLUMN': 'LAFmax1s',
              'LAMIN_COLUMN': 'LAFmin1s'}


sv307_dict = {'LAEQ_COLUMN': 'LAeq (Ch1, P1) [dB]',
              'LAMAX_COLUMN': 'LAFmax (Ch1, P1) [dB]',
              'LAMIN_COLUMN': 'LAFmin (Ch1, P1) [dB]'} 


sonometer_bilbo_dict = {'LAEQ_COLUMN': 'Value'}


audiopost_dict = {'LAEQ_COLUMN': 'LA',
                  'LAMAX_COLUMN': 'LAmax',
                  'LAMIN_COLUMN': 'LAmin'}


bruel_kjaer_dict = {'LAEQ_COLUMN': 'LAeq',
                    'LAMAX_COLUMN': 'LAFmax',
                    'LAMIN_COLUMN': 'LAFmin'}


tenerife_tct_dict = {'LAEQ_COLUMN': 'LA',
                    'LAMAX_COLUMN': 'LAmax',
                    'LAMIN_COLUMN': 'LAmin'}


# plotting Colors
C_MAP_WEEKDAY = {
            'Lunes': '#cc0000', # RED
            'Martes': '#8e7cc3', # PURPLE
            'Miércoles': '#9b5f00', # BROWN
            'Jueves': '#2986cc', # BLUE
            'Viernes': '#ffa500', # ORANGE
            'Sábado': '#6aa84f', # GREEN
            'Domingo': '#d172a4', # PINK
        }


C_MAP_WEEKDAY_NIGHT = {
            'Lunes-Martes': '#cc0000', # RED
            'Martes-Miércoles': '#8e7cc3', # PURPLE
            'Miércoles-Jueves': '#9b5f00', # BROWN
            'Jueves-Viernes': '#2986cc', # BLUE
            'Viernes-Sábado': '#ffa500', # ORANGE
            'Sábado-Domingo': '#6aa84f', # GREEN
            'Domingo-Lunes': '#d172a4', # PINK
}


PERCENTIL_COLOUR = {
    1: '#B28DFF',
    5: '#6EB5FF',
    10: '#B2E2F2',
    50: '#FFABAB',
    90: '#9E9E9E'
}


COLOR_PALLET_URBAN = {
            'Other human': '#2986cc', # BLUE
            'Electro-mechanical': '#cc0000', # RED
            'Voice': '#6aa84f', #  green 6aa84f
            'Motorised transport': '#ffa500', # orange
            'Geonature': '#8e7cc3', # PURPLE
            'Animal': '#9b5f00', # BROWN
            'Music': '#d172a4', # PINK
            'Background': '#000000', # BLACK
            'Other Sounds': '#c9d631', # yellow
            'Social/communal': '#d8cbf8', # Light purple
            'Human movement': '#40b674', # light green 40b674
        }



COLOR_PALLET_PORT_L1 = {
            'Siren': '#2986cc', # BLUE
            'Sound Event': '#cc0000', # RED
            'Transport': '#6aa84f', #  green 6aa84f
            'Human': '#ffa500', # orange
            'Nature': '#8e7cc3', # PURPLE
            'Animal': '#9b5f00', # BROWN
            'Music': '#d172a4', # PINK
            'Engine': '#000000', # BLACK
        }


C_MAP_GRADIENT = {
    1: '#C8FFC8',
    2: '#00C800',
    3: '#007800',
    4: '#FFFF00',
    5: '#FFC878',
    6: '#FF9600',
    7: '#FF0000',
    8: '#780000',
    9: '#FF00FF',
    10: '#8C3CFF',
    11: '#000078',
}




####################################
# SPECTROGRAM CONFIGURATION 
####################################
SPECTOGRAM_COLUMNS_LX_ES = [
    '1/3 LZeq 20,0', '1/3 LZeq 25,0', '1/3 LZeq 31,5', '1/3 LZeq 40,0', 
    '1/3 LZeq 50,0', '1/3 LZeq 63,0', '1/3 LZeq 80,0', '1/3 LZeq 100',
    '1/3 LZeq 125', '1/3 LZeq 160', '1/3 LZeq 200', '1/3 LZeq 250',
    '1/3 LZeq 315', '1/3 LZeq 400', '1/3 LZeq 500', '1/3 LZeq 630',
    '1/3 LZeq 800', '1/3 LZeq 1000', '1/3 LZeq 1250', '1/3 LZeq 1600',
    '1/3 LZeq 2000', '1/3 LZeq 2500', '1/3 LZeq 3150', '1/3 LZeq 4000',
    '1/3 LZeq 5000', '1/3 LZeq 6300', '1/3 LZeq 8000', '1/3 LZeq 10000',
    '1/3 LZeq 12500', '1/3 LZeq 16000', '1/3 LZeq 20000'
]