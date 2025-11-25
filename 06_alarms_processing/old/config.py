# relative path to the yamnet class map, urban taxonomy and port taxonomy
RELATIVE_PATH_YAMNET_MAP = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\AAC_IA_Puerto\yamnet_class_AAC_v3_0.csv"
RELATIVE_PATH_TAXONOMY_URBAN = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\taxonomies_json\urban_taxonomy_map_v1_0.json"
RELATIVE_PATH_TAXONOMY_PORT = r"\AAC - CENTRO DE ACUSTICA APLICADA, S.L\I + D + i - Documentos\Modelos_IA\taxonomies_json\port_1_taxonomy_mapping_v2.0.json"


# Remove start time and end time
REMOVE_START_TIME = 900
REMOVE_END_TIME = 900


# dB limits
DB_UPPER_LIMIT = 105
DB_LOWER_LIMIT = 30


#Font size
SMALL_SIZE = 10
MEDIUM_SIZE = 12
BIGGER_SIZE = 20
BIGGEST_SIZE = 22
BIGGEST_15_MIN_SIZE = 24
BIGGEST_PREDICT_MIN_SIZE = 50
BIGGEST_PREDICT_TITLE_SIZE = 60


# Limits
LIMITE_DIA = 75
LIMITE_TARDE = 75
LIMITE_NOCHE = 65



# Time limits for evaluation indicadores
LD_SECONDS = 21600
LE_SECONDS = 7200
LN_SECONDS = 14400



# CORRECTION FOR FREQUENCY COMPOSITION
LOW_FREQ_CORRECTION = -21
MEDIUM_FREQ_CORRECTION = -3
HIGH_FREQ_CORRECTION = +1

LOW_FREQ_BANDS = ["49.61Hz", "62.50Hz",  "78.75Hz", "99.21Hz", "125.00Hz", "157.49Hz"]
MEDIUM_FREQ_BANDS = ["198.43Hz", "250.00Hz", "314.98Hz", "396.85Hz", "500.00Hz", "629.96Hz", "793.70Hz", "1000.00Hz", "1259.92Hz"]
HIGH_FREQ_BANDS = ["2000.00Hz", "2519.84Hz", "3174.80Hz", "4000.00Hz", "5039.68Hz", "6349.60Hz", "8000.00Hz", "10079.37Hz", "12699.21Hz", "16000.00Hz", "20158.74Hz"]


######################### ALAMARMS ############################
# BANDS FOR TONAL_FREQ
LOW_BAND_TONAL_FREQ = ["12.40Hz", "15.62Hz",  "19.69Hz", "24.80Hz", "31.25Hz", "39.37Hz", "49.61Hz", "62.50Hz", "78.75Hz", "99.21Hz", "125.00Hz"]
MEDIUM_BAND_TONAL_FREQ = ["157.49Hz", "198.43Hz", "250.00Hz", "314.98Hz", "396.85Hz"]
HIGH_BAND_TONAL_FREQ = ["500.00Hz", "629.96Hz", "793.70Hz", "1000.00Hz", "1259.92Hz", "1587.40Hz", "2000.00Hz", "2519.84Hz", "3174.80Hz", "4000.00Hz", "5039.68Hz", "6349.60Hz", "8000.00Hz", "10079.37Hz", "12699.21Hz", "16000.00Hz", "20158.74Hz"]


# Desire order bands
TONAL_FREQ_BANDS_ORDERED = ["12.40Hz", "15.62Hz", "19.69Hz", "24.80Hz", "31.25Hz", "39.37Hz", "49.61Hz", "62.50Hz", "78.75Hz", "99.21Hz", "125.00Hz", "157.49Hz", "198.43Hz", "250.00Hz", "314.98Hz", "396.85Hz", "500.00Hz", "629.96Hz", "793.70Hz", "1000.00Hz", "1259.92Hz", "1587.40Hz", "2000.00Hz", "2519.84Hz", "3174.80Hz", "4000.00Hz", "5039.68Hz", "6349.60Hz", "8000.00Hz", "10079.37Hz", "12699.21Hz", "16000.00Hz"]

# columns to remove from the data when tonal frequencies analysis
COLUMNS_DISCARD = ["LA", "LC", "LZ", "LAmax", "LAmin", "filename", "date"]
COLUMNS_NOT_TO_PLOT = ["LA", "LC", "LZ", "LAmax", "LAmin"]


# frequency thresholds
LOW_BAND_THRESHOLD = 15
MEDIUM_BAND_THRESHOLD = 8
HIGH_BAND_THRESHOLD = 5



######################################################
CLASSES = {
            'display_name': 'display_name',
            'iso_taxonomy': 'iso_taxonomy',
            'class': 'class'
}

TAXONOMIES = {
    'brown_1': 'Brown_Level_1',
    'brown_2': 'Brown_Level_2',
    'brown_3': 'Brown_Level_3',
    'noiseport_1': 'NoisePort_Level_1',
    'noiseport_2': 'NoisePort_Level_2'
}

CLASS = CLASSES['class']
TAXONOMY = TAXONOMIES['noiseport_1']
######################################################



######################### PLOTTING FLAGS ############################
# Plotting Flags there are 15 flags to plot the following plots
OCA_ALARM = False
LMAX_ALARM = False
LC_LA_ALARM = False
L90_ALARM = False
L90_ALARM_DYNAMIC = True
FREQUENCY_COMPOSITION = False
TONAL_FREQUENCY = False

# peak
# predictions
PLOT_PEAK_PREDICTION = False

# evolution
PLOT_PEAK_DISTRI_HEATMAP = False
PLOT_PEAK_DISTRI = False
PLOT_DENSITY_DISTRI = False
PLOT_BOX_PLOT_PREDICTION = False

PLOT_SPECTROGRAM_1_3 = False





### REGULAR PLTS ###
PLOT_NIGHT_EVOLUTION = False
PLOT_NIGHT_EVOLUTION_15_MIN = False 

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