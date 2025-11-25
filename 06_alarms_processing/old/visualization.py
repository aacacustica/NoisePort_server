import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from utils import *
import os
from config import *
import plotly.express as px
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
# importing ast module
import ast
from tqdm import tqdm
from scipy.stats import gaussian_kde



plt.rc('font', size=MEDIUM_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=MEDIUM_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=MEDIUM_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title



cmap_dict = sns.color_palette(palette=["#C8FFC8", "#00C800", "#007800", "#FFFF00", "#FFC878", "#FF9600", "#FF0000", "#780000", "#FF00FF", "#8C3CFF", "#000078"],n_colors=11)

hex_colors = [mcolors.to_hex(color) for color in cmap_dict]
custom_color_scale = [[i/len(hex_colors), color] for i, color in enumerate(hex_colors)]
custom_color_scale.append([1, hex_colors[-1]])
    

def plot_night_evolution(df, folder_output_dir: str, logger, laeq_column:str, plotname:str, indicador_noche:str):
    try:
        df = df.dropna(subset=[laeq_column])
        logger.info(f"Using the laeq_column: {laeq_column}")
        sns.set_style("whitegrid")
        sns.set_palette("tab10")
        
        df['Día'] = df['night_str']
        
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by=['date', 'hour'], inplace=True)

        night_data = pd.DataFrame()
        unique_dates = df['date'].dt.date.unique()

        for current_date in unique_dates:
            next_date = current_date + pd.Timedelta(days=1)
            data_23 = df[(df['date'].dt.date == current_date) & (df['hour'] == 23)]
            data_00_06 = df[(df['date'].dt.date == next_date) & (df['hour'].isin(range(0, 7)))]

            if not data_23.empty and not data_00_06.empty:
                combined_data = pd.concat([data_23, data_00_06])
                night_data = pd.concat([night_data, combined_data])

        night_data['plot_hour'] = night_data['hour'].replace({23: -1}).astype(int)
        night_data.sort_values(by=['date', 'plot_hour'], inplace=True)
        
        # save to excel
        os.makedirs(folder_output_dir, exist_ok=True)
        night_data.to_csv(f"{folder_output_dir}/{plotname}_{indicador_noche}_evolution.csv", index=False)
        logger.info(f"Night evolution data saved to {folder_output_dir}/{plotname}_{indicador_noche}_evolution.csv")

        fig = sns.relplot(
            data=night_data, 
            x="plot_hour", 
            y=laeq_column, 
            kind="line", 
            hue="Día",
            estimator=leq, 
            aspect=1.3,
            palette=C_MAP_WEEKDAY_NIGHT
        )
        
        plt.xticks(range(-1, 7), ['23:00', '00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00'])
        plt.yticks(range(30, 105, 5), [str(level) for level in range(30, 105, 5)])

        plt.xlim(-1.5, 6.5)

        for ax in fig.axes.flat:
            ax.spines['top'].set_visible(True)
            ax.spines['right'].set_visible(True)

        plt.title(f'Evolución {indicador_noche}')
        plt.ylabel('dB(A)')
        plt.xlabel('Hora')

        os.makedirs(folder_output_dir, exist_ok=True)

        logger.info(f"Saving the plot {plotname}_{indicador_noche}")
        fig.savefig(f"{folder_output_dir}/{plotname}_{indicador_noche}_evolution.png", dpi=150)
        logger.info(f"Night evolution plot saved to {folder_output_dir}/{plotname}_{indicador_noche}_evolution.png")
    
    except Exception as e:
        logger.error(f"Error in plot_night_evolution: {e}")



def plot_night_evolution_15_min(df, folder_output_dir: str, logger, name_extension, laeq_column:str, plotname:str, indicador_noche:str):
    try:
        df = df.dropna(subset=[laeq_column])
        logger.info(f"Using the laeq_column: {laeq_column}")        
        sns.set_style("whitegrid")
        sns.set_palette("tab10")
        
        df['Día'] = df['night_str']
        df.index = pd.to_datetime(df.index)

        df_resampled = df.resample('15min')[laeq_column].mean()
        df_night_str = df.resample('15min')['Día'].agg(lambda x: x.value_counts().index[0] if len(x) > 0 else None)
        df_resampled = pd.DataFrame(df_resampled).join([df_night_str])
        
        # date and time columns
        df_resampled['date'] = df_resampled.index.date
        df_resampled['time'] = df_resampled.index.time
        
        # convert time column to a plottable format with a 15-minute offset
        df_resampled['plot_time'] = [(t.hour * 60 + t.minute - 15) - (23 * 60) if t.hour >= 23 else (t.hour * 60 + t.minute - 15) + 60 for t in df_resampled['time']]

        # filter data
        unique_dates = pd.to_datetime(df_resampled.index.date).unique()
        night_data = pd.DataFrame()

        # data for each night
        for current_date in unique_dates:
            #  time, which is the last 15-minute interval of the previous day
            start_time = pd.Timestamp(current_date - pd.Timedelta(days=1)).replace(hour=23, minute=0)
            
            # end time, which is the first 6 hours and 45 minutes of the current day
            end_time = pd.Timestamp(current_date).replace(hour=6, minute=45)
            
            # slice the data, which is the last 15-minute interval of the previous day and the first 6 hours and 45 minutes of the current day
            data_slice = df_resampled[start_time:end_time]
            
            # if the slice is not empty and the minimum index hour is 23, then it is a night
            if not data_slice.empty and data_slice.index.min().hour == 23:
                # so we add it to the night data
                night_data = pd.concat([night_data, data_slice])
        
        #save to csv
        os.makedirs(folder_output_dir, exist_ok=True)
        
        logger.info(f"Saving the data {plotname}_{indicador_noche}")
        night_data.to_csv(f"{folder_output_dir}/{plotname}_{indicador_noche}_evolution_{name_extension}.csv", index=False)
        logger.info(f"Night evolution data saved to {folder_output_dir}/{plotname}_{indicador_noche}_evolution_{name_extension}.csv")
        
        fig = sns.relplot(
                data=night_data, 
                x="plot_time", 
                y=laeq_column, 
                kind="line",
                errorbar=None,
                hue="Día",
                estimator=leq, 
                aspect=1.3,
                palette=C_MAP_WEEKDAY_NIGHT
            )

        x_labels = [f'{hour:02d}:{minute:02d}' for hour in range(23, 24) for minute in range(0, 60, 15)] + \
                [f'{hour:02d}:{minute:02d}' for hour in range(0, 7) for minute in range(0, 60, 15)]
                
        x_ticks = range(-15, 465, 15)
        plt.xticks(x_ticks, x_labels, rotation=90)
        plt.yticks(range(30, 105, 5), [str(level) for level in range(30, 105, 5)])
        
        plt.xlim(-30, 465)
        
        for ax in fig.axes.flat:
            ax.spines['top'].set_visible(True)
            ax.spines['right'].set_visible(True)
        
        plt.title(f'Evolución {indicador_noche} cada 15 minutos')
        plt.ylabel('dB(A)')
        plt.xlabel('Hora')

        # save the plot
        logger.info(f"Saving the plot {plotname}_{indicador_noche}")
        fig.savefig(os.path.join(folder_output_dir, f'{plotname}_{indicador_noche}_evolution_{name_extension}.png'),  dpi=150)
        logger.info(f"Night evolution plot saved to {folder_output_dir}/{plotname}_{indicador_noche}_evolution_{name_extension}.png")
    except Exception as e:
        logger.error(f"Error in plot_night_evolution_15_min: {e}")



  
def plot_predic_laeq_15_min(df: pd.DataFrame, yamnet_csv:pd.DataFrame, taxonomy_map, df_Pred:pd.DataFrame, folder_output_dir: str, logger, columns_dict: dict, agg_period: int, plotname: str):
    try:
        # remove nan values
        df = df.dropna(subset=[columns_dict['LAEQ_COLUMN_COEFF']])
        logger.info(f"Using the columns_dict: {columns_dict}")

        # # check
        spl_start_date = df['datetime'].iloc[0]
        spl_end_date = df['datetime'].iloc[-1]
        spl_difference_between_first_days = df['datetime'].iloc[10] - df['datetime'].iloc[9]
        logger.info(f"SPL file: Start date {spl_start_date} and End date {spl_end_date}")
        logger.info(f"SPL file: Difference between first and second date: {spl_difference_between_first_days}")

        pred_start_date = df_Pred['date'].iloc[0]
        pred_end_date = df_Pred['date'].iloc[-1]
        logger.info(f"Pred file: Start date {pred_start_date} and End date {pred_end_date}")
        pred_difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Pred file: Difference between first and second date: {pred_difference_between_first_days}")

        agg_funcs = {
            columns_dict['LAEQ_COLUMN_COEFF']: leq,
        }

        if pred_difference_between_first_days >= pd.Timedelta(minutes=15):
            logger.info(f"Resampling the SPL file to 15 minutes")
            df_LAeq = df.resample(f'{agg_period}s').agg(agg_funcs)
        else:
            logger.info(f"No Resampling the SPL file")
            df_LAeq = df


        start_date = max(df_LAeq.index.min(), df_Pred.index.min())
        end_date = min(df_LAeq.index.max(), df_Pred.index.max())

        df_LAeq = df_LAeq[start_date:end_date]
        df_Pred = df_Pred[start_date:end_date]
        df_Pred.index = df_Pred.index.round('15min')

        # check if the first date for lae and pred is the same
        check_dilay = df_LAeq.index[0] - df_Pred.index[0]
        if check_dilay != pd.Timedelta(seconds=0):
            logger.info(f"The mismatch for LAeq and Pred date is {check_dilay}")

            # check which is earlier, and apply the shift
            if df_LAeq.index[0] < df_Pred.index[0]:
                df_LAeq = df_LAeq.shift(periods=abs(check_dilay.seconds), freq='s')
            else:
                df_Pred = df_Pred.shift(periods=abs(check_dilay.seconds), freq='s')
            logger.info(f"Shifted the data to match the dates")


        # merge df
        df_aligned = df_LAeq.merge(df_Pred, how='left', left_index=True, right_index=True)
        # remove rows with NaN values
        df_aligned.dropna(inplace=True)

        # print(df_aligned)
        # set date_y as index
        if "date_y" in df_aligned.columns:
            df_aligned.set_index('date_y', inplace=True, drop=False)
        else:
            df_aligned.set_index('date', inplace=True, drop=False)


        ####################################################################
        df_aligned['class_probability'] = df_aligned.apply(
            lambda x: (x['class'], x['probability']) if isinstance(x['class'], float) else list(zip(x['class'], x['probability'])),
            axis=1
        )
        df_exploded = df_aligned.explode('class_probability')
        df_exploded['class'] = df_exploded['class_probability'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
        df_exploded['probability'] = df_exploded['class_probability'].apply(lambda x: x[1] if isinstance(x, tuple) else None)
        ####################################################################

        # create the df_all, merge with the audioset dataframe
        df_exploded['display_name'] = df_exploded['class']
        df_all = df_exploded.merge(yamnet_csv, how='left', on='display_name')
        df_all = df_all.dropna(subset=['display_name'])
    
        #########################################################
        #### Plotting the data ####
        
        display_name = 'display_name'
        iso_taxonomy = 'iso_taxonomy'
        classes = 'class'

        brown_2 = 'Brown_Level_2'
        brown_3 = 'Brown_Level_3'
        noiseport_1 = 'NoisePort_Level_1'
        noiseport_2 = 'NoisePort_Level_2'

        if 'Siren' in set(taxonomy_map.values()):
            class_to_plot = noiseport_1
        else:
            class_to_plot = brown_2  


        grouped_df = df_all.groupby(class_to_plot).agg(
            number=(classes, 'size'),
            LAeq=('LA_corrected', lambda x: leq(x))
        ).reset_index()

        fig = px.treemap(grouped_df, 
                        path=[class_to_plot],  
                        values='number',
                        color='LAeq',
                        color_continuous_scale=custom_color_scale,
                        range_color=[30, 85],
                        hover_data={'LAeq': True, 'number': True},
                        custom_data=['LAeq'],                  
                        )

        fig.update_layout(title=f'{plotname} | Promedio Energético (LAeq) por Clases')
        fig.update_traces(hovertemplate='<b>%{label}</b><br>LAeq: %{customdata[0]:.2f} dB<br>Count: %{value}')
        fig.update_traces(texttemplate='%{label}<br><br>LAeq: %{customdata[0]:.2f} dB')

        os.makedirs(folder_output_dir, exist_ok=True)

        logger.info(f"Saving the plot {plotname}")
        fig.write_html(f"{folder_output_dir}/{plotname}_LAeq_class_mean.html")
        logger.info(f"LAeq class mean plot saved to {folder_output_dir}/{plotname}_LAeq_class_mean.html")

        logger.info(f"Saving the data {plotname}")
        grouped_df.to_csv(f"{folder_output_dir}/{plotname}_LAeq_class_mean.csv", index=False)
        logger.info(f"LAeq class mean data saved to {folder_output_dir}/{plotname}_LAeq_class_mean.csv")

    except Exception as e:
        logger.error(f"Error in plot_predic_laeq_15_min: {e}")



def plot_predic_laeq_15_min_period(df: pd.DataFrame, yamnet_csv:pd.DataFrame, taxonomy_map, df_Pred:pd.DataFrame, folder_output_dir: str, logger, columns_dict: dict, agg_period: int, plotname: str):
    try:
        folder_output_dir = os.path.join(folder_output_dir, 'Prediction_LAeq_15_min_Period')
        # remove nan values
        df = df.dropna(subset=[columns_dict['LAEQ_COLUMN_COEFF']])
        logger.info(f"Using the columns_dict: {columns_dict}")

        # # check
        spl_start_date = df['datetime'].iloc[0]
        spl_end_date = df['datetime'].iloc[-1]
        spl_difference_between_first_days = df['datetime'].iloc[10] - df['datetime'].iloc[9]
        logger.info(f"SPL file: Start date {spl_start_date} and End date {spl_end_date}")
        logger.info(f"SPL file: Difference between first and second date: {spl_difference_between_first_days}")

        pred_start_date = df_Pred['date'].iloc[0]
        pred_end_date = df_Pred['date'].iloc[-1]
        logger.info(f"Pred file: Start date {pred_start_date} and End date {pred_end_date}")
        pred_difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Pred file: Difference between first and second date: {pred_difference_between_first_days}")

        agg_funcs = {
            columns_dict['LAEQ_COLUMN_COEFF']: leq,
        }

        if pred_difference_between_first_days >= pd.Timedelta(minutes=15):
            logger.info(f"Resampling the SPL file to 15 minutes")
            df_LAeq = df.resample(f'{agg_period}s').agg(agg_funcs)
        else:
            logger.info(f"No Resampling the SPL file")
            df_LAeq = df


        start_date = max(df_LAeq.index.min(), df_Pred.index.min())
        end_date = min(df_LAeq.index.max(), df_Pred.index.max())

        df_LAeq = df_LAeq[start_date:end_date]
        df_Pred = df_Pred[start_date:end_date]
        df_Pred.index = df_Pred.index.round('15min')

        # check if the first date for lae and pred is the same
        check_dilay = df_LAeq.index[0] - df_Pred.index[0]
        if check_dilay != pd.Timedelta(seconds=0):
            logger.info(f"The mismatch for LAeq and Pred date is {check_dilay}")

            # check which is earlier, and apply the shift
            if df_LAeq.index[0] < df_Pred.index[0]:
                df_LAeq = df_LAeq.shift(periods=abs(check_dilay.seconds), freq='s')
            else:
                df_Pred = df_Pred.shift(periods=abs(check_dilay.seconds), freq='s')
            logger.info(f"Shifted the data to match the dates")


        # merge df
        df_aligned = df_LAeq.merge(df_Pred, how='left', left_index=True, right_index=True)
        # remove rows with NaN values
        df_aligned.dropna(inplace=True)

        # set date_y as index
        if "date_y" in df_aligned.columns:
            df_aligned.set_index('date_y', inplace=True, drop=False)
        else:
            df_aligned.set_index('date', inplace=True, drop=False)
        

        ####################################################################
        df_aligned['class_probability'] = df_aligned.apply(
            lambda x: (x['class'], x['probability']) if isinstance(x['class'], float) else list(zip(x['class'], x['probability'])),
            axis=1
        )
        df_exploded = df_aligned.explode('class_probability')
        df_exploded['class'] = df_exploded['class_probability'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
        df_exploded['probability'] = df_exploded['class_probability'].apply(lambda x: x[1] if isinstance(x, tuple) else None)
        ####################################################################

        # create the df_all, merge with the audioset dataframe
        df_exploded['display_name'] = df_exploded['class']
        df_all = df_exploded.merge(yamnet_csv, how='left', on='display_name')
        df_all = df_all.dropna(subset=['display_name'])

        if 'datetime_y' in df_all.columns and 'hour_x' in df_all.columns:
            logger.info("Using 'datetime_y' and 'hour_x' columns for datetime and hour categorization.")
            df_all['datetime_y'] = pd.to_datetime(df_all['datetime_y'])
            df_all['time_of_day'] = df_all['hour_x'].apply(categorize_time_of_day)

        else:
            logger.info("Using 'date' and 'hour' columns for datetime and hour categorization.")
            df_all['datetime_y'] = pd.to_datetime(df_all['date'])
            df_all['time_of_day'] = df_all['hour'].apply(categorize_time_of_day)
    
        #########################################################
        #### Plotting the data ####
        
        display_name = 'display_name'
        iso_taxonomy = 'iso_taxonomy'
        classes = 'class'

        brown_1 = 'Brown_Level_1'
        brown_2 = 'Brown_Level_2'
        brown_3 = 'Brown_Level_3'
        noiseport_1 = 'NoisePort_Level_1'
        noiseport_2 = 'NoisePort_Level_2'

        if 'Siren' in set(taxonomy_map.values()):
            class_to_plot = noiseport_1
        else:
            class_to_plot = brown_2

        order_time_of_day = ['Ld', 'Le', 'Ln']

        df_all['time_of_day'] = pd.Categorical(df_all['time_of_day'], categories=order_time_of_day, ordered=True)
        df_all['order_index'] = df_all['time_of_day'].cat.codes

        grouped_df = df_all.groupby([class_to_plot, df_all['time_of_day']]).agg(
            number=(classes, 'size'),
            LAeq=('LA_corrected', lambda x: leq(x)),
            order_index=('order_index', 'first')
        ).reset_index()

        grouped_df = grouped_df.dropna(subset=['LAeq'])

        # iterate for each period and save it individually
        for period in order_time_of_day:
            period_df = grouped_df[grouped_df['time_of_day'] == period]

            fig = px.treemap(period_df, 
                            path=[px.Constant(period),class_to_plot],  
                            values='number',
                            color='LAeq',
                            color_continuous_scale=custom_color_scale,
                            range_color=[30, 85],
                            hover_data={'LAeq': True, 'number': True},
                            custom_data=['LAeq'],                  
                            )

            fig.update_layout(title=f'{plotname} | Promedio Energético (LAeq) distribución por Periodo {period} por Clases')
            fig.update_traces(hovertemplate='<b>%{label}</b><br>LAeq: %{customdata[0]:.2f} dB<br>Count: %{value}')
            fig.update_traces(texttemplate='%{label}<br><br>LAeq: %{customdata[0]:.2f} dB')

            os.makedirs(folder_output_dir, exist_ok=True)

            logger.info(f"Saving the plot {plotname}")
            fig.write_html(f"{folder_output_dir}/{plotname}_LAeq_class_period_{period}.html")
            logger.info(f"LAeq class period plot saved to {folder_output_dir}/{plotname}_LAeq_class_period_{period}.html")

            logger.info(f"Saving the data {plotname}")
            period_df.to_csv(f"{folder_output_dir}/{plotname}_LAeq_class_period_{period}.csv", index=False)
            logger.info(f"LAeq class period data saved to {folder_output_dir}/{plotname}_LAeq_class_period_{period}.csv")

    except Exception as e:
        logger.error(f"Error in plot_predic_laeq_15_min_period: {e}")




def plot_predic_laeq_15_min_4h(df: pd.DataFrame, yamnet_csv:pd.DataFrame, taxonomy_map, df_Pred:pd.DataFrame, folder_output_dir: str, logger, columns_dict: dict, agg_period: int, plotname: str):
    try:
        folder_output_dir = os.path.join(folder_output_dir, 'Prediction_LAeq_15_min_4h')
        # remove nan values
        df = df.dropna(subset=[columns_dict['LAEQ_COLUMN_COEFF']])
        logger.info(f"Using the columns_dict: {columns_dict}")

        # # check
        spl_start_date = df['datetime'].iloc[0]
        spl_end_date = df['datetime'].iloc[-1]
        spl_difference_between_first_days = df['datetime'].iloc[10] - df['datetime'].iloc[9]
        logger.info(f"SPL file: Start date {spl_start_date} and End date {spl_end_date}")
        logger.info(f"SPL file: Difference between first and second date: {spl_difference_between_first_days}")

        pred_start_date = df_Pred['date'].iloc[0]
        pred_end_date = df_Pred['date'].iloc[-1]
        logger.info(f"Pred file: Start date {pred_start_date} and End date {pred_end_date}")
        pred_difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Pred file: Difference between first and second date: {pred_difference_between_first_days}")

        agg_funcs = {
            columns_dict['LAEQ_COLUMN_COEFF']: leq,
        }

        if pred_difference_between_first_days >= pd.Timedelta(minutes=15):
            logger.info(f"Resampling the SPL file to 15 minutes")
            df_LAeq = df.resample(f'{agg_period}s').agg(agg_funcs)
        else:
            logger.info(f"No Resampling the SPL file")
            df_LAeq = df


        start_date = max(df_LAeq.index.min(), df_Pred.index.min())
        end_date = min(df_LAeq.index.max(), df_Pred.index.max())

        df_LAeq = df_LAeq[start_date:end_date]
        df_Pred = df_Pred[start_date:end_date]
        df_Pred.index = df_Pred.index.round('15min')

        # check if the first date for lae and pred is the same
        check_dilay = df_LAeq.index[0] - df_Pred.index[0]
        if check_dilay != pd.Timedelta(seconds=0):
            logger.info(f"The mismatch for LAeq and Pred date is {check_dilay}")

            # check which is earlier, and apply the shift
            if df_LAeq.index[0] < df_Pred.index[0]:
                df_LAeq = df_LAeq.shift(periods=abs(check_dilay.seconds), freq='s')
            else:
                df_Pred = df_Pred.shift(periods=abs(check_dilay.seconds), freq='s')
            logger.info(f"Shifted the data to match the dates")

        # merge df
        df_aligned = df_LAeq.merge(df_Pred, how='left', left_index=True, right_index=True)
        # remove rows with NaN values
        df_aligned.dropna(inplace=True)

        # set date_y as index
        if "date_y" in df_aligned.columns:
            df_aligned.set_index('date_y', inplace=True, drop=False)      
        else:
            df_aligned.set_index('date', inplace=True, drop=False)

        ####################################################################
        df_aligned['class_probability'] = df_aligned.apply(
            lambda x: (x['class'], x['probability']) if isinstance(x['class'], float) else list(zip(x['class'], x['probability'])),
            axis=1
        )
        df_exploded = df_aligned.explode('class_probability')
        df_exploded['class'] = df_exploded['class_probability'].apply(lambda x: x[0] if isinstance(x, tuple) else x)
        df_exploded['probability'] = df_exploded['class_probability'].apply(lambda x: x[1] if isinstance(x, tuple) else None)
        ####################################################################

        # create the df_all, merge with the audioset dataframe
        df_exploded['display_name'] = df_exploded['class']
        df_all = df_exploded.merge(yamnet_csv, how='left', on='display_name')
        df_all = df_all.dropna(subset=['display_name'])

        # convert data time type datetime_y column
        if 'datetime_y' in df_all.columns and 'hour_x' in df_all.columns:
            logger.info("Using 'datetime_y' and 'hour_x' columns for datetime and hour categorization.")
            df_all['datetime_y'] = pd.to_datetime(df_all['datetime_y'])
            df_all['time_of_day'] = df_all['hour_x'].apply(categorize_time_of_day)

        else:
            logger.info("Using 'date' and 'hour' columns for datetime and hour categorization.")
            df_all['datetime_y'] = pd.to_datetime(df_all['date'])
            df_all['time_of_day'] = df_all['hour'].apply(categorize_time_of_day)

    
        #########################################################
        #### Plotting the data ####
        
        display_name = 'display_name'
        iso_taxonomy = 'iso_taxonomy'
        classes = 'class'

        brown_1 = 'Brown_Level_1'
        brown_2 = 'Brown_Level_2'
        brown_3 = 'Brown_Level_3'
        noiseport_1 = 'NoisePort_Level_1'
        noiseport_2 = 'NoisePort_Level_2'

        if 'Siren' in set(taxonomy_map.values()):
            class_to_plot = noiseport_1
        else:
            class_to_plot = brown_2

        order_time_of_day = ['Ld_1', 'Ld_2', 'Ld_3', 'Le', 'Ln_1', 'Ln_2']

        df_all['time_of_day'] = pd.Categorical(df_all['time_of_day'], categories=order_time_of_day, ordered=True)
        df_all['order_index'] = df_all['time_of_day'].cat.codes


        grouped_df = df_all.groupby([class_to_plot, df_all['time_of_day']]).agg(
            number=(classes, 'size'),
            LAeq=('LA_corrected', lambda x: leq(x)),
            order_index=('order_index', 'first')
        ).reset_index()

        # remove rows with nan values
        grouped_df = grouped_df.dropna(subset=['LAeq'])

        for period in order_time_of_day:
            period_df = grouped_df[grouped_df['time_of_day'] == period]

            fig = px.treemap(period_df, 
                            path=[px.Constant(period),class_to_plot],  
                            values='number',
                            color='LAeq',
                            color_continuous_scale=custom_color_scale,
                            range_color=[30, 85],
                            hover_data={'LAeq': True, 'number': True},
                            custom_data=['LAeq'],                  
                            )

            fig.update_layout(title=f'{plotname} | Promedio Energético (LAeq) distribución por Periodo {period} por Clases')
            fig.update_traces(hovertemplate='<b>%{label}</b><br>LAeq: %{customdata[0]:.2f} dB<br>Count: %{value}')
            fig.update_traces(texttemplate='%{label}<br><br>LAeq: %{customdata[0]:.2f} dB')

            os.makedirs(folder_output_dir, exist_ok=True)

            logger.info(f"Saving the plot {plotname}")
            fig.write_html(f"{folder_output_dir}/{plotname}_LAeq_class_period_{period}.html")
            logger.info(f"LAeq class period plot saved to {folder_output_dir}/{plotname}_LAeq_class_period_{period}.html")

            logger.info(f"Saving the data {plotname}")
            period_df.to_csv(f"{folder_output_dir}/{plotname}_LAeq_class_period_{period}.csv", index=False)
            logger.info(f"LAeq class period data saved to {folder_output_dir}/{plotname}_LAeq_class_period_{period}.csv")


    except Exception as e:
        logger.error(f"Error in PLOT_PREDIC_LAEQ_4H: {e}")




def plot_prediction_stack_bar(df_Pred:pd.DataFrame, yamnet_csv, taxonomy_map, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("white")
        sns.set_palette("tab10")

        # make duration to make the resample to 15 minutes
        start_date = df_Pred['date'].iloc[0]
        end_date = df_Pred['date'].iloc[-1]
        difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Start date {start_date} and End date {end_date}")
        logger.info(f"Difference between first and second date: {difference_between_first_days}")

        # explode
        df_exploded = df_Pred.explode('class')
        df_exploded['display_name'] = df_exploded['class']
        df_exploded['number'] = 1

        #~insert date
        df_exploded = insert_dates(df_exploded)
        df_exploded['mapped_class'] = df_exploded['class'].map(taxonomy_map)

        #################################
        home_dir = os.path.expanduser('~')
        full_path = os.path.join(home_dir, RELATIVE_PATH_YAMNET_MAP.lstrip('\\'))
        union = pd.read_csv(full_path, sep=';')
        # merge classes with ontology
        df_exploded = df_exploded.merge(union,how='left',on='display_name')

        # remove Unnamed columns
        df_exploded = df_exploded.loc[:, ~df_exploded.columns.str.contains('^Unnamed')]
        # rename columns
        df_exploded.rename(columns={"fullday": "Día", "hour": "Hora", "mid": "Distribución de clases"}, inplace=True)
        unique_día_weekday = df_exploded['Día'].unique()
        #set to categorical
        df_exploded['Día'] = pd.Categorical(df_exploded['Día'], categories=unique_día_weekday, ordered=True)


        display_name = 'display_name'
        iso_taxonomy = 'iso_taxonomy'
        classes = 'class'

        brown_2 = 'Brown_Level_2'
        brown_3 = 'Brown_Level_3'
        noiseport_1 = 'NoisePort_Level_1'
        noiseport_2 = 'NoisePort_Level_2'

        if 'Siren' in set(taxonomy_map.values()):
            class_to_plot = noiseport_1
            color_pallet = COLOR_PALLET_PORT_L1
        else:
            class_to_plot = brown_2  
            color_pallet = COLOR_PALLET_URBAN

        dfg = df_exploded.groupby([class_to_plot,'Día']).count().reset_index()
        fig = px.bar(
            dfg, 
            x='Día',
            y='Distribución de clases',
            color=class_to_plot,
            title=f'{plotname} | Clases por día desde {start_date} hasta {end_date}',
            color_discrete_sequence=px.colors.qualitative.Alphabet, 
            color_discrete_map=color_pallet,
            height=900,
            width=2000
        )

        fig.write_html(f"{folder_output_dir}/{plotname}_prediction_stack_map.html")
        logger.info(f"Prediccion stack bar saved at: {folder_output_dir}/{plotname}_prediction_stack_map.html")

        # save csv with the data
        dfg.to_csv(f"{folder_output_dir}/{plotname}_prediction_stack_map.csv")
        logger.info(f"Saved csv at {folder_output_dir}/{plotname}_prediction_stack_map.csv")

    except Exception as e:
        logger.error(f"Error in plot_prediction_stack_bar: {e}")



def plot_prediction_map(df_Pred:pd.DataFrame, taxonomy_map, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("white")
        sns.set_palette("tab10")

        # remove empty entries in class column
        df_Pred = df_Pred.dropna(subset=['class'])

        # make duration to make the resample to 15 minutes
        start_date = df_Pred['date'].iloc[0]
        end_date = df_Pred['date'].iloc[-1]
        difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Start date {start_date} and End date {end_date}")
        logger.info(f"Difference between first and second date: {difference_between_first_days}")

        # explode
        df_exploded = df_Pred.explode('class')
        df_exploded['display_name'] = df_exploded['class']
        df_exploded['number'] = 1

        #~insert date
        df_exploded = insert_dates(df_exploded)
        df_exploded = df_exploded.dropna(subset=['class'])
        # print if there is nan values
        if df_exploded['class'].isnull().values.any():
            logger.error("There are nan values in the class column")

        df_exploded['mapped_class'] = df_exploded['class'].map(taxonomy_map)
        df_exploded = df_exploded.dropna(subset=['mapped_class'])

        ################################ PLOT ################################
        # if plot_prediction_stack_bar is greater than 1 second
        if difference_between_first_days == pd.Timedelta(seconds=1):
            logger.info(f"Plotting the prediction map for {plotname} equal to 1 second")
            resampled_df = df_exploded.resample('15min').agg({
                'filename': 'first',  # taking the first filename in each bin
                'class': 'first',
                'probability': 'first', 
                'display_name': 'first', 
                'number': 'sum',  # summing up the numbers in each bin
                'year': 'first',
                'month': 'first',
                'day': 'first',
                'hour': 'first',
                'minute': 'first',
                'second': 'first',
                'weekday': 'first',
                'fullday': 'first',
                'mapped_class': 'first'
            })
            ###################### PLOTTING ######################
            df_resampled = resampled_df.sort_values(by=["year", "month", "fullday"])
        
        else:
            logger.info(f"Plotting the prediction map for {plotname} greater than 1 second")
            ###################### PLOTTING ######################
            df_resampled = df_exploded.sort_values(by=["year", "month", "fullday"])


        # drop None values
        df_resampled = df_resampled.dropna(subset=['mapped_class'])
        # map class to number
        class_to_num = {class_name: index+1 for index, class_name in enumerate(df_resampled['mapped_class'].unique())}

        df_resampled['class_num'] = df_resampled['mapped_class'].map(class_to_num)
        # drop nan values
        df_resampled = df_resampled.dropna(subset=['class_num'])
        
        # inverting the dictionary to get the name of the class for the legend
        name_class = {v: k for k, v in class_to_num.items()}
        
        # mapping from classes numbers to colors
        if 'Siren' in set(taxonomy_map.values()):
            num_to_color = {num: COLOR_PALLET_PORT_L1[class_name] for class_name, num in class_to_num.items()}
        else:
            num_to_color = {num: COLOR_PALLET_URBAN[class_name] for class_name, num in class_to_num.items()}
        cmap = [num_to_color[cls_num] for cls_num in name_class.keys()]
        
        # leggend elements colors
        legend_elements = [Patch(facecolor=num_to_color[cls_num], label=f"Clase {cls_num} - {name_class.get(cls_num, '')}") for cls_num in name_class.keys()]
        day_class = pd.pivot_table(data=df_resampled, columns=df_resampled.index.time, index=["year", "month", "fullday"], values="class_num", aggfunc='mean')


        day_class.columns = [pd.Timestamp.combine(pd.to_datetime("2024-01-01"), t) for t in day_class.columns]
        time_interval = pd.date_range(start=day_class.columns.min(), end=day_class.columns.max(), freq='1H')

        plt.figure(figsize=(45, 35))
        if day_class.isna().all().all() or day_class.empty:
            logger.warning("No valid data. Skipping...")
        else:
            ax = sns.heatmap(day_class, annot=False, cmap=cmap, linewidth=0.5, cbar=False)

            # set intervals for the x datetime axis
            ax.set_xticks([day_class.columns.get_loc(t) for t in time_interval])
            ax.set_xticklabels([t.strftime('%H:%M:%S') for t in time_interval], rotation=90, fontsize=BIGGEST_PREDICT_MIN_SIZE)

            yticklabels = [f"{idx[0]}-{idx[1]}-{idx[2]}" for idx in day_class.index]
            # remove ".0" from the string
            yticklabels = [label.replace('.0', '') for label in yticklabels]
            ax.set_yticklabels(yticklabels, rotation=0, fontsize=BIGGEST_PREDICT_MIN_SIZE)

            plt.title(f"{plotname} Predicciones", fontsize=BIGGEST_PREDICT_TITLE_SIZE)
            plt.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=BIGGEST_PREDICT_MIN_SIZE)

            # save
            plt.savefig(f"{folder_output_dir}/{plotname}_prediction_map.png", bbox_inches='tight')
            logger.info(f"Saved image at {folder_output_dir}/{plotname}_prediction_map.png")

            # save csv with the data
            day_class.to_csv(f"{folder_output_dir}/{plotname}_prediction_map.csv")
            logger.info(f"Saved csv at {folder_output_dir}/{plotname}_prediction_map.csv")
          
    except Exception as e:
        logger.error(f"Error in plot_prediction_map: {e}")





def plot_tree_map(df_Pred:pd.DataFrame,taxonomy_map, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("white")
        sns.set_palette("tab10")

        # prediction map foñder
        folder_output_dir = os.path.join(folder_output_dir, 'Prediction_Tree_Map')
        os.makedirs(folder_output_dir, exist_ok=True)

        # make duration to make the resample to 15 minutes
        start_date = df_Pred['date'].iloc[0]
        end_date = df_Pred['date'].iloc[-1]
        difference_between_first_days = df_Pred['date'].iloc[10] - df_Pred['date'].iloc[9]
        logger.info(f"Start date {start_date} and End date {end_date}")
        logger.info(f"Difference between first and second date: {difference_between_first_days}")

        # explode
        df_exploded = df_Pred.explode('class')
        df_exploded['display_name'] = df_exploded['class']
        df_exploded['number'] = 1

        #~insert date
        df_exploded = insert_dates(df_exploded)
        df_exploded['mapped_class'] = df_exploded['class'].map(taxonomy_map)

        #################################
        home_dir = os.path.expanduser('~')
        full_path = os.path.join(home_dir, RELATIVE_PATH_YAMNET_MAP.lstrip('\\'))
        union = pd.read_csv(full_path, sep=';')

        # merge classes with ontology
        df_exploded = df_exploded.merge(union, how='left', on='display_name')
        # remove Unnamed columns
        df_exploded = df_exploded.loc[:, ~df_exploded.columns.str.contains('^Unnamed')]


        display_name = 'display_name'
        iso_taxonomy = 'iso_taxonomy'
        classes = 'class'

        brown_1 = 'Brown_Level_1'
        brown_2 = 'Brown_Level_2'
        brown_3 = 'Brown_Level_3'
        noiseport_1 = 'NoisePort_Level_1'
        noiseport_2 = 'NoisePort_Level_2'

        if 'Siren' in set(taxonomy_map.values()):
            class_to_plot = noiseport_1
            color_pallet = COLOR_PALLET_PORT_L1
        else:
            class_to_plot = brown_2  
            color_pallet = COLOR_PALLET_URBAN

        df_exploded = df_exploded.dropna(subset=[class_to_plot, 'class'])
        # exit()

        fig = px.treemap(df_exploded, 
                 path=[class_to_plot, 'class'], 
                 values='number',
                 color=class_to_plot,  #for coloring
                color_discrete_map=color_pallet
                )

        fig.update_layout(title=f'{plotname} | Clases por día desde {start_date} hasta {end_date}')

        fig.write_html(f"{folder_output_dir}/{plotname}_prediction_tree_map.html")
        logger.info(f"{folder_output_dir}/{plotname}_prediction_tree_map.html")

        # save csv with the data
        df_exploded.to_csv(f"{folder_output_dir}/{plotname}_prediction_tree_map.csv")
        logger.info(f"Saved csv at {folder_output_dir}/{plotname}_prediction_tree_map.csv")

        #####################################      
        for day in df_exploded['day'].unique():
            day_df = df_exploded[df_exploded['day'] == day]
            fig = px.treemap(day_df, 
                     path=[class_to_plot, 'class'], 
                     values='number',
                     color=class_to_plot,  #for coloring
                    color_discrete_map=color_pallet
                    )
            
            fig.update_layout(title=f'{plotname} | {day_df["year"].iloc[0]}-{day_df["month"].iloc[0]}-{day}')

            fig.write_html(f"{folder_output_dir}/{plotname}_prediction_tree_map{day}.html")
            logger.info(f"{folder_output_dir}/{plotname}_prediction_tree_map{day}.html")

            # save csv with the data
            day_df.to_csv(f"{folder_output_dir}/{plotname}_prediction_tree_map{day}.csv")
            logger.info(f"Saved csv at {folder_output_dir}/{plotname}_prediction_tree_map{day}.csv")

    except Exception as e:
        logger.error(f"Error in plot_tree_map: {e}")




def make_time_plot(df: pd.DataFrame, folder_output_dir: str, logger, columns_dict: dict, agg_period: int, plotname: str, percentiles: list):
    try:
        logger.info(f"Using the columns_dict: {columns_dict}")
        # add an hour to the dataframe
        # df.index = df.index + pd.DateOffset(hours=1)
        # remove nan values
        df = df.dropna(subset=[columns_dict['LAEQ_COLUMN_COEFF']])
        
        # if there is just LAEQ_COLUMN_COEFF, then we use it for all the columns, otherwise use the max and min
        if columns_dict['LAEQ_COLUMN'] == 'Value':
            agg_funcs = {
                columns_dict['LAEQ_COLUMN_COEFF']: leq
            }
            logger.info(f"Using the columns_dict: df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']]")
        
        else:
            agg_funcs = {
                columns_dict['LAEQ_COLUMN_COEFF']: leq,
                columns_dict['LAMAX_COLUMN_COEFF']: 'max',
                columns_dict['LAMIN_COLUMN_COEFF']: 'min'
            }
            logger.info(f"Using the columns_dict: df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']], df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']], df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']]")

        logger.info(f"Using the agg_funcs: {agg_funcs}")
        df_LAeq = df.resample(f'{agg_period}s').agg(agg_funcs)
        oca = df.resample(f'{agg_period}s').agg({'oca': 'min'})

        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(20, 10))
        ax.set_facecolor("white")

        logger.info(f"Using the percentiles: {percentiles}")
        logger.info(f"Using the agg_period: {agg_period}")
        
        
        if columns_dict['LAEQ_COLUMN'] == 'Value':
            x = df_LAeq.index
            ax.plot(x, df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']], linewidth=3, color='red', label='LAeq')
            # OCA
            ax.plot(x, oca.values, color='#00B0F0')

        else:
            x = df_LAeq.index
            ax.plot(x, df_LAeq[columns_dict['LAEQ_COLUMN_COEFF']], linewidth=3, color='red', label='LAeq')
            ax.plot(x, df_LAeq[columns_dict['LAMAX_COLUMN_COEFF']], linewidth=1, color='#FF99FF', label='Lmax')
            ax.plot(x, df_LAeq[columns_dict['LAMIN_COLUMN_COEFF']], linewidth=1, color='#92D050', label='Lmin')
            # OCA
            ax.plot(x, oca.values, color='#00B0F0', label='OCA')


            for percentile in percentiles:
                values = df[columns_dict['LAEQ_COLUMN_COEFF']].resample(f'{agg_period}s').quantile((100 - percentile) / 100)
                ax.plot(
                    x, 
                    values, 
                    linewidth=0.5, 
                    label=f'L{percentile}', 
                    color=PERCENTIL_COLOUR[percentile]
                )

        # debugg time of the plot
        hours = mdates.HourLocator(interval=5)
        h_fmt = mdates.DateFormatter('%d-%m-%y %H:%M')
        
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(h_fmt)

        plt.xlim(df.index.min(), df.index.max())
        plt.ylim([DB_LOWER_LIMIT, DB_UPPER_LIMIT])
        plt.ylabel('dB(A)', fontsize=BIGGEST_SIZE)
        plt.xlabel('Hora', fontsize=BIGGEST_SIZE)
        plt.title(f'{plotname} Nivel equivalente {agg_period}s', fontsize=BIGGEST_SIZE)

        plt.xticks(rotation=90, fontsize=BIGGEST_SIZE)
        plt.yticks(fontsize=BIGGEST_SIZE)

        plt.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0.1, fancybox=True, framealpha=1, edgecolor='black', fontsize=BIGGEST_SIZE)
        
        plt.tight_layout()
        
        # make grid
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)

        # save
        os.makedirs(folder_output_dir, exist_ok=True)

        logger.info(f"Saving the plot {plotname}")
        plt.savefig(f'{folder_output_dir}/{plotname}_{agg_period}s_time_plot.png', dpi=350) # dpi stands for dots per inch, the more the dpi the better the quality of the image
        logger.info(f"Timeplot saved to {folder_output_dir}/{plotname}_{agg_period}s_time_plot.png")

        logger.info(f"Saving the data {plotname}")
        df_LAeq.to_csv(f'{folder_output_dir}/{plotname}_{agg_period}s_time_plot.csv', index=True)
        logger.info(f"Timeplot data saved to {folder_output_dir}/{plotname}_{agg_period}s_time_plot.xlsx")

        plt.close()

    except Exception as e:
        logger.error(f"Error in make_timeplot: {e}")





def plot_heatmap_evolution_hour(df, folder_output_dir: str, logger, values_column: str, agg_func: str, plotname:str):
    try:
        # remove nan values
        df = df.dropna(subset=[values_column])
        logger.info(f"Using the values_column: {values_column}")
        sns.set_style("white")
        sns.set_palette("tab10")
        
        df['Día'] = df['day_name'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
        df['date_day'] = df['date'].astype(str) + ' ' + df['Día']
        
        leq_day_hour = pd.pivot_table(
            df, 
            values=values_column, 
            index=['date_day'],
            columns=['hour'], 
            aggfunc=agg_func
        ).round(1)
  
        leq_day_hour.columns = [f"{hour:02d}:00" for hour in leq_day_hour.columns]
        
        plt.figure(figsize=(20,10))
        heatmap =sns.heatmap(
            leq_day_hour, 
            vmin=30, 
            vmax=85, 
            cmap=cmap_dict, 
            annot=True,
            annot_kws={"size": BIGGER_SIZE}
        )
        
        plt.xlabel('Hora', fontsize=BIGGEST_SIZE)
        plt.ylabel('Día', fontsize=BIGGEST_SIZE)
        plt.title(f'{plotname} Nivel equivalente', fontsize=BIGGEST_SIZE)
        
        plt.yticks(rotation=0, fontsize=BIGGEST_SIZE)
        plt.xticks(rotation=90, fontsize=BIGGEST_SIZE)

        cbar = heatmap.collections[0].colorbar
        cbar.ax.tick_params(labelsize=BIGGEST_SIZE)

        plt.tight_layout()
        
        os.makedirs(f'{folder_output_dir}', exist_ok=True)
        
        logger.info(f"Saving the plot {plotname}")
        plt.savefig(f'{folder_output_dir}/{plotname}_heatmap_evolucion.png',dpi=350)
        logger.info(f"Heatmap plot saved to {folder_output_dir}/{plotname}_heatmap_evolucion.png")

        logger.info(f"Saving the data {plotname}")
        leq_day_hour.to_csv(f'{folder_output_dir}/{plotname}_heatmap_evolucion.csv', index=False)
        logger.info(f"Heatmap data saved to {folder_output_dir}/{plotname}_heatmap_evolucion.csv")
        
        plt.close()
        
    except Exception as e:
        logger.error(f"Error in plot_heatmap_evolution_hour: {e}")






def plot_heatmap_evolution_15_min(df, folder_output_dir: str, logger, values_column: str, agg_func: str, plotname:str):
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.error("DataFrame index is not a datetime index.")
        return

    try:
        # remove nan values
        df = df.dropna(subset=[values_column])
        logger.info(f"Using the values_column: {values_column}")
        sns.set_style("white")
        sns.set_palette("tab10")
        
        def get_15min_interval(dt):
            return f"{dt.hour:02d}:{(dt.minute // 15) * 15:02d}"

        df['Día'] = df['day_name'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
        df['date_day'] = df['date'].astype(str) + ' ' + df['Día']
        df['15min_interval'] = df.index.map(get_15min_interval)

        leq_day_15min = pd.pivot_table(
            df, 
            values=values_column, 
            index=['date_day'],
            columns=['15min_interval'], 
            aggfunc=agg_func
        ).round(1)
        

        plt.figure(figsize=(20,10))
        heatmap =sns.heatmap(
            leq_day_15min, 
            vmin=30, 
            vmax=85, 
            cmap=cmap_dict, 
            # annot=True,
            # annot_kws={"size": MEDIUM_SIZE}
        )
        
        plt.xlabel('Hora', fontsize=BIGGEST_15_MIN_SIZE)
        plt.ylabel('Día', fontsize=BIGGEST_15_MIN_SIZE)
        plt.title(f'{plotname} Nivel equivalente 15 minutos', fontsize=30)
        
        plt.yticks(rotation=0, fontsize=BIGGEST_15_MIN_SIZE)
        plt.xticks(rotation=90, fontsize=BIGGEST_15_MIN_SIZE)

        cbar = heatmap.collections[0].colorbar
        cbar.ax.tick_params(labelsize=BIGGEST_15_MIN_SIZE)
        
        plt.tight_layout()
        
        os.makedirs(f'{folder_output_dir}', exist_ok=True)

        logger.info(f"Saving the plot {plotname}")
        plt.savefig(f'{folder_output_dir}/{plotname}_heatmap_evolucion_15_min.png',dpi=150)
        logger.info(f"Heatmap plot saved to {folder_output_dir}/{plotname}_heatmap_evolucion_15_min.png")

        logger.info(f"Saving the data {plotname}")
        leq_day_15min.to_csv(f'{folder_output_dir}/{plotname}_heatmap_evolucion_15_min.csv', index=False)
        logger.info(f"Heatmap data saved to {folder_output_dir}/{plotname}_heatmap_evolucion_15_min.csv")
        
        plt.close()
        
    except Exception as e:
        logger.error(f"Error in plot_heatmap: {e}")





def plot_indicadores_heatmap(df, folder_output_dir: str, logger, plotname:str, ind_column:str):
    try:
        # remove nan values
        df = df.dropna(subset=[ind_column])
        logger.info(f"Using the ind_column: {ind_column}")
        sns.set_style("white")
        sns.set_palette("tab10")

        if "Fecha" not in df.columns and "Date hour" in df.columns:
            logger.info(f"Date hour column found, using it as Fecha")
            # df["Fecha"] = df["Date hour"]
            df["Fecha"] = pd.to_datetime(df['Date hour'], dayfirst=True)
            logger.info(f"Date hour column found, using it as Fecha")
        
        if "Fecha" not in df.columns and "Time" in df.columns:
            logger.info(f"Time column found, using it as Fecha")
            df["Fecha"] = pd.to_datetime(df['Time'], dayfirst=True)
            logger.info(f"Time column found, using it as Fecha")
            
        if "Fecha" not in df.columns and "marcadores" in df.columns:
            logger.info("No Fecha column found")
            # reindex datetime index column
            if isinstance(df.index, pd.DatetimeIndex):
                df.reset_index(inplace=True)
                df["Fecha"] = pd.to_datetime(df['datetime'], dayfirst=True)
                logger.info(f"Using the datetime index as Fecha")

        if "Fecha" not in df.columns and 'OVLD' not in df.columns:
            # copy 'date' colimn and named it as 'Fecha'
            df['Fecha'] = df['datetime']

        df_indicadores = (df.groupby(['date','indicador_str'])['Fecha'].agg(['first','last']))
        df_indicadores['duration'] = df_indicadores.apply(lambda row: calculate_duration(row['first'], row['last']), axis=1)
        
        # set weekday in the plot
        df['date_weekday'] = df['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df['Fecha'].dt.day_name().replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
        
        # indicators to check
        indicators_to_check = ['Ld', 'Le', 'Ln']

        # select first and last day
        first_day = df['date'].min()
        last_day = df['date'].max()

        # check presence of indicators
        presence_first_day = {indicator: indicator in df[df['date'] == first_day]['indicador_str'].unique() for indicator in indicators_to_check}
        presence_last_day = {indicator: indicator in df[df['date'] == last_day]['indicador_str'].unique() for indicator in indicators_to_check}
        logger.info(f"Presence of indicators in first day {first_day}: {presence_first_day}")
        logger.info(f"Presence of indicators in last day {last_day}: {presence_last_day}")

        # 'duration of indicators
        duration_first_day = {indicator: df_indicadores.loc[(first_day, indicator), 'duration'] if presence_first_day[indicator] else 0 for indicator in indicators_to_check}
        duration_last_day = {indicator: df_indicadores.loc[(last_day, indicator), 'duration'] if presence_last_day[indicator] else 0 for indicator in indicators_to_check}
        
        # log duration information
        for indicator in indicators_to_check:
            logger.info(f"Duration of {indicator} on the first day {first_day}: {duration_first_day[indicator]}")
            logger.info(f"Duration of {indicator} on the last day {last_day}: {duration_last_day[indicator]}")

        # apply filter based on duration and presence
        for indicator in indicators_to_check:
            if presence_first_day[indicator] and duration_first_day[indicator] <= LE_SECONDS:
                df = df[~((df['date'] == first_day) & (df['indicador_str'] == indicator))]
                logger.info(f"{indicator} indicator from first day {first_day} removed, less than {LE_SECONDS} seconds")

            if presence_last_day[indicator] and duration_last_day[indicator] <= LE_SECONDS:
                df = df[~((df['date'] == last_day) & (df['indicador_str'] == indicator))]
                logger.info(f"{indicator} indicator from last day {last_day} removed, less than {LE_SECONDS} seconds")
        
        # make the energy average of the indicators
        indicadores_table = pd.pivot_table(
            data=df,
            index="date_weekday",
            columns="indicador_str",
            values=ind_column,
            aggfunc=leq
        ).round(1)

        desired_order = ["Ln", "Ld", "Le"]
        indicadores_table = indicadores_table.reindex(columns=desired_order)

        plt.figure(figsize=(15, 8))
        
        ax = sns.heatmap(
            indicadores_table, 
            annot=True, 
            fmt=".1f", 
            linewidth=0.5, 
            cmap=cmap_dict, 
            vmin=30, 
            vmax=85,
            annot_kws={"size": MEDIUM_SIZE}
        )
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0)
        
        plt.ylabel('Día', fontsize=BIGGEST_SIZE)
        plt.xlabel('Indicador', fontsize=BIGGEST_SIZE)
        plt.title(f'{plotname} Indicadores', fontsize=BIGGEST_SIZE)

        plt.yticks(rotation=0, fontsize=BIGGEST_SIZE)
        plt.xticks(rotation=0, fontsize=BIGGEST_SIZE)

        cbar = ax.collections[0].colorbar
        cbar.ax.tick_params(labelsize=BIGGEST_SIZE)

        plt.tight_layout()
        
        os.makedirs(f'{folder_output_dir}', exist_ok=True)

        logger.info(f"Saving the plot {plotname}")
        plt.savefig(f"{folder_output_dir}/{plotname}_indicadores.png")
        logger.info(f"Indicadores plot saved to {folder_output_dir}\{plotname}_indicadores.png")
        
        logger.info(f"Saving the data {plotname}")
        indicadores_table.to_csv(f"{folder_output_dir}/{plotname}_indicadores.csv", index=True)
        logger.info(f"Indicadores data saved to {folder_output_dir}\{plotname}_indicadores.csv")
        
        plt.close()
        
        
        # indicador power average
        general_power_averages = indicadores_table.apply(leq).round(1)
        general_power_averages_df = general_power_averages.to_frame().transpose()
        
        os.makedirs(f'{folder_output_dir}', exist_ok=True)

        logger.info(f"Indicadores generales data {plotname}")
        general_power_averages_df.to_csv(f'{folder_output_dir}/{plotname}_indicadores_generales.csv', index=False)
        logger.info(f"Indicadores generales data saved to {folder_output_dir}/{plotname}_indicadores_generales.csv")
    
    except Exception as e:
        logger.error(f"Error in plot_indicadores_heatmap: {e}")





def plot_day_evolution(df, folder_output_dir: str, logger, laeq_column: str, plotname: str):
    try:
        # remove nan values
        df = df.dropna(subset=[laeq_column])
        df = df.reset_index(drop=True)
        df = df.drop_duplicates()
        logger.info(f"Using the laeq_column: {laeq_column}")

        sns.set_style("whitegrid")
        sns.set_palette("tab10")
        
        # translate the day name to Spanish
        df['Día'] = df['day_name'].replace(
            ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
        )
        
        weekdays = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
        df['Día'] = pd.Categorical(df['Día'], categories=weekdays, ordered=True)
        
        fig = sns.relplot(
            data=df,
            x="hour",
            y=laeq_column,
            kind="line", # kind is the type of plot to draw
            hue="Día", # hue is the column to split the data
            estimator=leq,  # estimator is the function to apply to the data
            aspect=1.3, # aspect is the width/height ratio
            palette=C_MAP_WEEKDAY,
        )

        fig.set(xlim=(-1, 24), ylim=(30, 105))

        # change the x-axis labels to 24-hour format
        hour_labels = [f"{hour:02d}:00" for hour in range(24)]
        plt.xticks(range(24), hour_labels, rotation=90)

        plt.yticks(range(30, 105, 5), [str(level) for level in range(30, 105, 5)])

        for ax in fig.axes.flat:
            ax.spines['top'].set_visible(True)
            ax.spines['right'].set_visible(True)
        
        plt.axvline(x=6.50, color=".7", dashes=(2, 1), zorder=0)  # 6:45 AM
        plt.axvline(x=18.50, color=".7", dashes=(2, 1), zorder=0)  # 6:45 PM
        plt.axvline(x=22.50, color=".7", dashes=(2, 1), zorder=0)  # 10:45 PM

        plt.text(s="Ln", x=0.13, y=0.97, transform=plt.gca().transAxes, c="Black", weight="bold")
        plt.text(s="Ld", x=0.53, y=0.97, transform=plt.gca().transAxes, c="Black", weight="bold")
        plt.text(s="Le", x=0.85, y=0.97, transform=plt.gca().transAxes, c="Black", weight="bold")
        plt.text(s="Ln", x=0.96, y=0.97, transform=plt.gca().transAxes, c="Black", weight="bold")
        
        plt.title(f"Evolución día {plotname} Date {df['date'].iloc[0]} - {df['date'].iloc[-1]}", fontsize=14)
        plt.ylabel('dB(A)')
        plt.xlabel('Hora')

        logger.info(f"Day evolution plot created for {plotname} Date {df['date'].iloc[0]} - {df['date'].iloc[-1]}")
        
        os.makedirs(folder_output_dir, exist_ok=True)

        logger.info(f"Saving the plot {plotname}")
        fig.savefig(f"{folder_output_dir}/{plotname}_day_evolution.png", dpi=300)
        logger.info(f"Day evolution plot saved to {folder_output_dir}/{plotname}_day_evolution.png")

        logger.info(f"Saving the data {plotname}")
        df.to_csv(f"{folder_output_dir}/{plotname}_day_evolution.csv", index=False)
        logger.info(f"Day evolution data saved to {folder_output_dir}/{plotname}_day_evolution.csv")

        plt.close()

    except Exception as e:
        logger.error(f"Error in plot_day_evolution: {e}")



def plot_period_evolution(df,  folder_output_dir: str, logger, laeq_column:str, plotname:str):
    try:
        df = df.dropna(subset=[laeq_column])
        df = df.reset_index(drop=True)
        df = df.drop_duplicates()
        logger.info(f"Using the laeq_column: {laeq_column}")
        
        sns.set_style("whitegrid")
        sns.set_palette("tab10")
        
        # translate the day name to spanish from english in day_name
        df['Día'] = df['day_name'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
        
        weekdays = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
        df['Día'] = pd.Categorical(df['Día'], categories=weekdays, ordered=True)
        
        for ind in df["indicador_str"].unique():
            if ind == 'Ln':
                continue
            
            df_temp = df[df["indicador_str"] == ind]
            
            fig = sns.relplot(
                data=df_temp,
                x="hour",
                y=laeq_column,
                kind="line", # kind is the type of plot to draw
                hue="Día", # hue is the column to split the data
                estimator=leq,  # estimator is the function to apply to the data
                aspect=1.3, # aspect is the width/height ratio
                palette=C_MAP_WEEKDAY,
            )
            
            if ind == 'Ld':
                fig.set(xlim=(6, 19), ylim=(30, 105))
                plt.xticks(range(7, 19), [f"{hour:02d}:00" for hour in range(7, 19)])
                logger.info(f"Plotted Ld")
                
            elif ind == 'Le':
                fig.set(xlim=(18.7, 22.3), ylim=(30, 105))
                plt.xticks([18.7, 19, 20, 21, 22, 22.3], ['', '19:00', '20:00', '21:00', '22:00', ''])
                logger.info(f"Ploted Le")

            plt.yticks(range(30, 105, 5), [str(level) for level in range(30, 105, 5)])

            for ax in fig.axes.flat:
                ax.spines['top'].set_visible(True)
            
            ax.spines['right'].set_visible(True)
            plt.title(f"Evolución {ind}")
            plt.ylabel('dB(A)')
            plt.xlabel('Hora')
        
            os.makedirs(f'{folder_output_dir}', exist_ok=True)
            
            logger.info(f"Saving the plot {plotname}_{ind}")
            fig.savefig(f"{folder_output_dir}/{plotname}_{ind}_evolution.png",dpi=150)
            logger.info(f"Period evolution plot saved to {folder_output_dir}/{plotname}_{ind}_evolution.png")

            logger.info(f"Saving the data {plotname}_{ind}")
            df_temp.to_csv(f"{folder_output_dir}/{plotname}_{ind}_evolution.csv", index=False)
            logger.info(f"Period evolution data saved to {folder_output_dir}/{plotname}_{ind}_evolution.csv")
        
            plt.close()
        
    
    except Exception as e:
        logger.error(f"Error in plot_period_evolution: {e}")


def plt_spectrogram(df, folder_output_dir, logger, plotname):
    frequency_columns = df.columns[5:-2] 
    frequencies = [float(col.replace('Hz', '').replace('k', '000')) for col in frequency_columns]
    times = pd.to_datetime(df['date'])

    # select datatime from 22:30 to 23:30 of just one day
    # df = df[(df['date'] >= '2024-07-09 22:30:00') & (df['date'] <= '2024-07-09 23:30:00')]
    # times = pd.to_datetime(df['date'])

    spectrogram_data = df[frequency_columns].T.values
    spectrogram_data = spectrogram_data.clip(20, 110)
    freq_labels = [f"{freq} Hz" for freq in frequencies]
    
    plt.figure(figsize=(20, 10))
    plt.pcolormesh(times, frequencies, spectrogram_data, shading='auto', cmap='inferno')
    plt.colorbar(label='Magnitude (dB)')

    plt.yticks(frequencies, freq_labels)
    plt.ylabel('Frequency (Hz)')

    plt.title(f'Spectrogram: {plotname}')
    plt.yscale('log')
    plt.ylim([min(frequencies), max(frequencies)])

    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=300))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    plt.xticks(rotation=90)
    plt.tight_layout()
    # remove the grid
    plt.grid(False)

    # Save the plot
    output_file = f'{folder_output_dir}/Spectrogram/{plotname}_spectrogram.png'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    plt.savefig(output_file, dpi=150)
    plt.close()
    logger.info(f"Spectrogram saved to {output_file}")




########## OCA ALARM ##########
def oca_alarm(df_1h_leq, folder_output_dir: str, logger, plotname: str):
    # remove grey color background
    sns.set_style("whitegrid")
    
    # filter the data based on the thresholds
    filtered_df_1h_leq = df_1h_leq[
        (df_1h_leq["LA_corrected_leq"] > df_1h_leq["oca"])
    ]

    plt.figure(figsize=(15, 8))
    plt.plot(df_1h_leq["LA_corrected_leq"], label="LAeq")
    plt.plot(filtered_df_1h_leq["LA_corrected_leq"], "ro", label="LAeq Alarm")


    plt.title(f"{plotname} LAeq - OCA | Alarm")
    plt.ylabel("dB(A)")

    # draw horizontal lines for the thresholds for the OCA values
    plt.plot(df_1h_leq.index, df_1h_leq["oca"], "r--", label="OCA", linewidth=2)

    plt.legend()
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))


    plt.xlim(df_1h_leq.index.min(), df_1h_leq.index.max())
    plt.ylim([DB_LOWER_LIMIT, DB_UPPER_LIMIT])
    plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))

    plt.tight_layout()

    # save imgage
    plt.savefig(f'{folder_output_dir}/{plotname}_OCA_Alarm.png', dpi=150)
    logger.info(f"Saved plot at {folder_output_dir}/{plotname}_OCA_Alarm.png")




########## LMAX ALARM ##########
def lmax_alarm(df_1h_leq: pd.DataFrame, folder_output_dir: str, logger, plotname: str, threshold: int):
    sns.set_style("whitegrid")
    filter_df = df_1h_leq[df_1h_leq['LAmax_corrected_max'] > threshold]

    plt.figure(figsize=(15, 8))
    plt.plot(df_1h_leq['LAmax_corrected_max'], label='LAmax')
    plt.plot(filter_df['LAmax_corrected_max'], 'ro', label='LAeqmax Alarm')
    
    plt.legend()
    plt.grid(True)
    
    plt.title(f"{plotname} LAmax | Alarm")
    plt.ylabel('dB(A)')

    plt.plot(df_1h_leq.index, [threshold] * len(df_1h_leq), 'r--', label='Threshold', linewidth=2)
    plt.xlim(df_1h_leq.index.min(), df_1h_leq.index.max())
    
    plt.xticks(rotation=90)
    plt.legend(loc='upper left', bbox_to_anchor=(1.02, 1))
    
    plt.tight_layout()

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))
    
    plt.ylim([DB_LOWER_LIMIT, DB_UPPER_LIMIT])
    
    # save image
    plt.savefig(f'{folder_output_dir}/{plotname}_LAmax_Alarm.png', dpi=150)
    logger.info(f"Saved plot at {folder_output_dir}/{plotname}_LAmax_Alarm.png")



########## LC_LA ALARM ##########
def LC_LA_alarm(df_1h_leq: pd.DataFrame, folder_output_dir: str, logger, plotname: str, threshold_norma: int, threshold_dB: int):
    sns.set_style("whitegrid")
    df_1h_leq['LC_LA_threshold'] = df_1h_leq.groupby("indicador_str")["LCeq-LAeq_corrected_leq"].transform(leq).round(2)

    # filter [2]: threshold_dB upper the average threshold (Ld, Le, Ln)
    filter1 = (df_1h_leq["LCeq-LAeq_corrected_leq"] > df_1h_leq["LC_LA_threshold"] + threshold_dB)
    # filter [1]: LC-LA > 10 dB which is the law
    filter2 = df_1h_leq["LCeq-LAeq_corrected_leq"] > threshold_norma
    # combine the filters
    combined_filter = filter1 | filter2
    filtered_df_1h_leq = df_1h_leq[combined_filter]



    plt.figure(figsize=(15, 8))
    plt.plot(df_1h_leq.index, df_1h_leq["LCeq-LAeq_corrected_leq"], label="LC-LA")
    plt.plot(filtered_df_1h_leq.index, filtered_df_1h_leq["LCeq-LAeq_corrected_leq"], "ro", label="Alarm")

    # horizontal lines for the threshold values
    plt.plot(df_1h_leq.index, df_1h_leq["LC_LA_threshold"], "r--", label="Threshold", linewidth=2)
    # plot horizontal line for the threshold + constant
    plt.plot(df_1h_leq.index, df_1h_leq["LC_LA_threshold"] + threshold_dB, "y--", label=f"Threshold + {threshold_dB}", linewidth=2)
    # DRAW HORITZONTAL LINES FOR THE 10 dB THRESHOLD
    plt.plot(df_1h_leq.index, [threshold_norma] * len(df_1h_leq), "g--", label="10 dB Threshold", linewidth=2)


    plt.title(f"{plotname} LCeq - LAeq | Alarm")
    plt.ylabel("dB(A)")
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    plt.xlim(df_1h_leq.index.min(), df_1h_leq.index.max())
    plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
    plt.tight_layout()

    # save the plot
    plt.savefig(f"{folder_output_dir}/{plotname}_LC_LA_Alarm.png", dpi=150)
    logger.info(f"Saved plot at {folder_output_dir}/{plotname}_LC_LA_Alarm.png")



########## L90 ALARM ##########
def l90_alarm(df_1h_leq: pd.DataFrame, folder_output_dir: str, logger, plotname: str, threshold_dB: int):
    sns.set_style("whitegrid")

    l90_column = df_1h_leq["90percentile"].dropna()

    # average of the week and jump when it exceeds a specific value (+5?)
    # [1] average of the week (leq function)
    avg_week = leq(l90_column).round(2)
    # print(f"Leq of the 90th percentile: {avg_week}")

    # [2] alarm when the difference is greater than 5 dB
    # filter df
    filter_df = l90_column > avg_week + threshold_dB
    # print(filter_df)


    plt.figure(figsize=(15, 8))
    plt.plot(df_1h_leq.index, l90_column, label="90th Percentile")
    plt.plot(df_1h_leq[filter_df].index, df_1h_leq[filter_df]["90percentile"], "ro", label="Alarm")
    
    # horizontal line for the average of the week
    plt.axhline(avg_week, color="r", linestyle="--", label="avg", linewidth=2)
    plt.axhline(avg_week + threshold_dB, color="g", linestyle="--", label=f"avg + {threshold_dB}", linewidth=2)

    plt.title("90th Percentile | Alarm")
    plt.ylabel("dB(A)")
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    plt.xlim(df_1h_leq.index.min(), df_1h_leq.index.max())
    plt.ylim([DB_LOWER_LIMIT, DB_UPPER_LIMIT])
    plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
    plt.tight_layout()


    # save the plot
    plt.savefig(f'{folder_output_dir}/{plotname}_L90_Alarm.png', dpi=150)
    logger.info(f"Saved plot at {folder_output_dir}/{plotname}_L90_Alarm.png")



########## L90 ALARM DYNAMIC ##########
def l90_alarm_dynamic(df_1h_leq: pd.DataFrame, folder_output_dir: str, logger, plotname: str, threshold_dB: int):
    sns.set_style("whitegrid")

    l90_column = df_1h_leq["90percentile"].dropna()
    
    print(l90_column)

    # making a rolling window of 3 hours
    df_1h_leq["90percentile_median"] = (
        df_1h_leq["90percentile"].rolling(window=3, min_periods=1).median()
    )
    print(df_1h_leq)
    exit()


    # l90_column['90percentile_median'] = l90_column.rolling(window=10800, min_periods=1).quantile(0.5)
    print(l90_column)
    # l90_column['90percentile_median'] = l90_column['90percentile'].rolling(window=10800, min_periods=1).quantile(0.5)
    # print(l90_column)

    # average of the week and jump when it exceeds a specific value (+5?)
    # [1] average of the week (leq function)
    avg_week = leq(l90_column).round(2)
    # print(f"Leq of the 90th percentile: {avg_week}")

    # [2] alarm when the difference is greater than 5 dB
    # filter df
    filter_df = l90_column > avg_week + threshold_dB
    # print(filter_df)


    plt.figure(figsize=(15, 8))
    plt.plot(df_1h_leq.index, l90_column, label="90th Percentile")
    plt.plot(df_1h_leq[filter_df].index, df_1h_leq[filter_df]["90percentile"], "ro", label="Alarm")
    
    # horizontal line for the average of the week
    plt.axhline(avg_week, color="r", linestyle="--", label="avg", linewidth=2)
    plt.axhline(avg_week + threshold_dB, color="g", linestyle="--", label=f"avg + {threshold_dB}", linewidth=2)

    plt.title("90th Percentile | Alarm")
    plt.ylabel("dB(A)")
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    plt.xlim(df_1h_leq.index.min(), df_1h_leq.index.max())
    plt.ylim([DB_LOWER_LIMIT, DB_UPPER_LIMIT])
    plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
    plt.tight_layout()


    # save the plot
    plt.savefig(f'{folder_output_dir}/{plotname}_L90_Alarm.png', dpi=150)
    logger.info(f"Saved plot at {folder_output_dir}/{plotname}_L90_Alarm.png")



# FREQUENCY COMPOSITION
def frequency_composition(df_oct: pd.DataFrame, folder_output_dir_1h_folder: str, logger, plotname: str, threshold_comp: int):
    sns.set_style("whitegrid")

    df_oct["date"] = pd.to_datetime(df_oct["date"])
    df_oct = df_oct.set_index("date", drop=True)

    df_oct_cp = df_oct.copy()

    # for each row, select 1h values and sum for low, medium and high bands
    df_oct_cp["low_freq"] = df_oct_cp[LOW_FREQ_BANDS].apply(sum_dBs, axis=1)
    df_oct_cp["medium_freq"] = df_oct_cp[MEDIUM_FREQ_BANDS].apply(sum_dBs, axis=1)
    df_oct_cp["high_freq"] = df_oct_cp[HIGH_FREQ_BANDS].apply(sum_dBs, axis=1)

    df_oct_cp[["low_freq", "medium_freq", "high_freq"]]

    df_oct_1hour = df_oct_cp.resample("h").agg(
        {"low_freq": leq, "medium_freq": leq, "high_freq": leq}
    )


    # apply correction
    df_oct_1hour["low_freq"] = df_oct_1hour["low_freq"] + LOW_FREQ_CORRECTION
    df_oct_1hour["medium_freq"] = df_oct_1hour["medium_freq"] + MEDIUM_FREQ_CORRECTION
    df_oct_1hour["high_freq"] = df_oct_1hour["high_freq"] + HIGH_FREQ_CORRECTION
    # remove nan values
    df_oct_1hour = df_oct_1hour.dropna()
    
    # copy for the low and high frequency analñysis
    df_oct_1hour_lineal_comparsion = df_oct_1hour.copy()

    df_oct_1hour["predominant_freq"] = "None"

    for index, row in df_oct_1hour.iterrows():
        if row["low_freq"] > row["medium_freq"] or row["low_freq"] > row["high_freq"]:
            df_oct_1hour.at[index, "predominant_freq"] = "Low frequency"
        elif row["medium_freq"] > row["low_freq"] or row["medium_freq"] > row["high_freq"]:
            df_oct_1hour.at[index, "predominant_freq"] = "Medium frequency"
        elif row["high_freq"] > row["low_freq"] or row["high_freq"] > row["medium_freq"]:
            df_oct_1hour.at[index, "predominant_freq"] = "High frequency"
        else:
            df_oct_1hour.at[index, "predominant_freq"] = "No predominant frequency"

    df_oct_1hour = df_oct_1hour.dropna()



    plt.figure(figsize=(20, 6))
    color_map = {
        "Low frequency": "blue",
        "Medium frequency": "green",
        "High frequency": "red",
    }
    colors = df_oct_1hour["predominant_freq"].map(color_map)

    # set a X for the points value
    plt.scatter(
        df_oct_1hour.index,
        df_oct_1hour["predominant_freq"],
        c=colors,
        marker="X",
        linewidths=0.1,
        s=100,
        label="Predominant Frequency",
    )

    plt.title(f"{plotname} Predominant Frequency")
    plt.xlim(df_oct_1hour.index.min(), df_oct_1hour.index.max())
    plt.xticks(rotation=90)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    # legend
    legend_labels = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color, markersize=10)
        for color in color_map.values()
    ]
    plt.legend(
        legend_labels,
        color_map.keys(),
        title="Frequency Bands",
        loc="upper left",
        bbox_to_anchor=(1, 1),
    )
    plt.tight_layout()

    # save the plot
    plt.savefig(f"{folder_output_dir_1h_folder}/{plotname}_Predominant_Frequency.png", dpi=150)
    logger.info(f"Saved plot at {folder_output_dir_1h_folder}/{plotname}_Predominant_Frequency.png")




    # LINEAL COMPARISON
    logger.info(f"[6] Plotting frequency composition comparation for folder {plotname}")
    low_freq_filter = df_oct_1hour_lineal_comparsion["low_freq"] > df_oct_1hour_lineal_comparsion["low_freq"].shift(1) + threshold_comp
    high_freq_filter = df_oct_1hour_lineal_comparsion["high_freq"] > df_oct_1hour_lineal_comparsion["high_freq"].shift(1) + threshold_comp

    # plot the low frequency values
    plt.figure(figsize=(20, 6))
    plt.plot(df_oct_1hour_lineal_comparsion["high_freq"], label="hIGH Freq")
    plt.plot(df_oct_1hour_lineal_comparsion[high_freq_filter].index, df_oct_1hour_lineal_comparsion[high_freq_filter]["high_freq"], "ro", label="Alarm")

    plt.plot(df_oct_1hour_lineal_comparsion["low_freq"], label="Low Freq", color="orange")
    plt.plot(df_oct_1hour_lineal_comparsion[low_freq_filter].index, df_oct_1hour_lineal_comparsion[low_freq_filter]["low_freq"], "go", label="Alarm")

    plt.title(f"{plotname} Low and High Frequency | Alarm")
    plt.ylabel("dB")
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%d-%m-%Y %H-%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    plt.xlim(df_oct_1hour_lineal_comparsion.index.min(), df_oct_1hour_lineal_comparsion.index.max())
    plt.ylim([0, DB_UPPER_LIMIT])
    plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
    plt.tight_layout()

    # save the plot
    plt.savefig(f"{folder_output_dir_1h_folder}/{plotname}_Low_High_Frequency_Alarm.png", dpi=150)
    logger.info(f"Saved plot at {folder_output_dir_1h_folder}/{plotname}_Low_High_Frequency_Alarm.png")





def tonal_frequency(df_oct: pd.DataFrame, folder_output_dir_1h_folder: str, logger, plotname: str):
    """ Detect tonal frequencies in the dataframe and plot them """
    sns.set_style("whitegrid")

    # df_oct = df_oct[:100000]

    tonal_frequency = []
    tonal_frequency_value = []
    previous_tonal_frequency = []
    previous_tonal_value_frequency = []
    next_tonal_frequency = []
    next_tonal_value_frequency = []
    previous_diff = []
    next_diff = []
    filenames = []
    dates = []
    bands = []
    band_thresholds = []

    logger.info("Detecting tonal frequencies. It may take a while (5-10 min)...")
    for _, row in df_oct.iterrows():
        for i in range(len(df_oct.columns)):
            current_column = df_oct.columns[i]
            current_filename = row["filename"]
            current_date = row["date"]

            if current_column in COLUMNS_DISCARD:
                continue

            current_column_value = row[current_column]

            # get previous and next columns and values
            previous_column = df_oct.columns[i - 1] if i - 1 >= 0 else None
            previos_column_value = row[previous_column] if previous_column else None
            next_column = df_oct.columns[i + 1] if i + 1 < len(df_oct.columns) else None
            next_column_value = row[next_column] if next_column else None

            # convert values to float
            current_column_value = pd.to_numeric(current_column_value, errors="coerce")
            previos_column_value = pd.to_numeric(previos_column_value, errors="coerce")
            next_column_value = pd.to_numeric(next_column_value, errors="coerce")

            if pd.isna(previos_column_value):
                previos_column_value = next_column_value
            if pd.isna(next_column_value):
                next_column_value = previos_column_value

            # differences
            diff_previous_band = (
                current_column_value - previos_column_value
                if pd.notna(previos_column_value)
                else None
            )
            diff_next_band = (
                current_column_value - next_column_value
                if pd.notna(next_column_value)
                else None
            )
            diff_previous_band = (
                round(diff_previous_band, 2) if diff_previous_band is not None else None
            )
            diff_next_band = (
                round(diff_next_band, 2) if diff_next_band is not None else None
            )

            detected_band = None
            band_threshold = None

            if current_column in LOW_BAND_TONAL_FREQ:
                if (
                    diff_previous_band is not None
                    and diff_next_band is not None
                    and diff_previous_band > LOW_BAND_THRESHOLD
                    and diff_next_band > LOW_BAND_THRESHOLD
                ):
                    detected_band = "LOW"
                    band_threshold = LOW_BAND_THRESHOLD

            elif current_column in MEDIUM_BAND_TONAL_FREQ:
                if (
                    diff_previous_band is not None
                    and diff_next_band is not None
                    and diff_previous_band > MEDIUM_BAND_THRESHOLD
                    and diff_next_band > MEDIUM_BAND_THRESHOLD
                ):
                    detected_band = "MEDIUM"
                    band_threshold = MEDIUM_BAND_THRESHOLD

            elif current_column in HIGH_BAND_TONAL_FREQ:
                if (
                    diff_previous_band is not None
                    and diff_next_band is not None
                    and diff_previous_band > HIGH_BAND_THRESHOLD
                    and diff_next_band > HIGH_BAND_THRESHOLD
                ):
                    detected_band = "HIGH"
                    band_threshold = HIGH_BAND_THRESHOLD



            if detected_band:
                bands.append(detected_band)
                tonal_frequency.append(current_column)
                tonal_frequency_value.append(current_column_value)
                previous_tonal_frequency.append(previous_column)
                previous_tonal_value_frequency.append(previos_column_value)
                next_tonal_frequency.append(next_column)
                next_tonal_value_frequency.append(next_column_value)
                previous_diff.append(diff_previous_band)
                next_diff.append(diff_next_band)
                filenames.append(current_filename)
                dates.append(current_date)
                band_thresholds.append(band_threshold)



    df_tonal_freq = pd.DataFrame(
        {
            "date": dates,
            "filename": filenames,
            "tonal_frequency": tonal_frequency,
            "tonal_frequency_value": tonal_frequency_value,
            "previous_column": previous_tonal_frequency,
            "previous_column_value": previous_tonal_value_frequency,
            "next_column": next_tonal_frequency,
            "next_column_value": next_tonal_value_frequency,
            "previous_diff": previous_diff,
            "next_diff": next_diff,
            "threshold": band_thresholds,
            "band": bands,
        }
    )

    # plotting
    df_tonal_freq['date'] = pd.to_datetime(df_tonal_freq['date'], errors='coerce')

    # drop row with values -np.inf, -np.inf and np.nan
    # TODO --> TEST THIS
    df_tonal_freq["date"] = pd.to_datetime(df_tonal_freq["date"])

    # filter out any invalid values
    df_tonal_freq = df_tonal_freq[
        (df_tonal_freq["tonal_frequency_value"] != -np.inf)
        & (df_tonal_freq["tonal_frequency_value"] != np.inf)
        & (~df_tonal_freq["tonal_frequency_value"].isna())
    ]


    # setup desired order
    df_tonal_freq["tonal_frequency"] = pd.Categorical(
        df_tonal_freq["tonal_frequency"], categories=TONAL_FREQ_BANDS_ORDERED, ordered=True
    )

    plt.figure(figsize=(18, 10))
    # plt.figure(figsize=(25, 12))
    plt.scatter(df_tonal_freq["date"], df_tonal_freq["tonal_frequency"].cat.codes, marker="x", color="orange")
    plt.title(f"{plotname} | Tonal Frequency Alarm Detection")
    plt.xticks(rotation=90)
    plt.yticks(range(len(TONAL_FREQ_BANDS_ORDERED)), TONAL_FREQ_BANDS_ORDERED)  # !!!!!! set y-ticks to ordered frequencies !!!!!! 

    plt.grid(True)

    # # date column format --> 2023-12-11 13:37:24
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))

    # X AND Y LMIT
    plt.xlim(df_tonal_freq["date"].min(), df_tonal_freq["date"].max())
    plt.ylim(-1, len(TONAL_FREQ_BANDS_ORDERED)) 

    plt.tight_layout()
    
    # # save the plot
    plt.savefig(f"{folder_output_dir_1h_folder}/{plotname}_tonal_frequency.png", dpi=150)
    logger.info(f"Saved plot at {folder_output_dir_1h_folder}/{plotname}_tonal_frequency.png")






def plot_peak_predictions(df_merged: pd.DataFrame, folder_output_dir: str, start_date, end_date,logger, plotname: str):
    try:
        df_merged = df_merged.copy()

        grouped_df = df_merged.groupby(TAXONOMY).agg(
                    number=(CLASS, 'size'),
                    LAeq=('LAeq', lambda x: leq(x))
                ).reset_index()

        fig = px.treemap(grouped_df, 
                        path=[TAXONOMY],  
                        values='number',
                        color='LAeq',
                        color_continuous_scale=custom_color_scale,
                        range_color=[30, 85],
                        hover_data={'LAeq': True, 'number': True},
                        custom_data=['LAeq'],                  
                        )

        fig.update_layout(title=f'{plotname} Promedio Energético (LAeq) por Clases de Picos encontrados')
        fig.update_traces(hovertemplate='<b>%{label}</b><br>LAeq: %{customdata[0]:.2f} dB<br>Count: %{value}')
        fig.update_traces(texttemplate='%{label}<br><br>LAeq: %{customdata[0]:.2f} dB')

        # save the plot
        fig.write_html(f"{folder_output_dir}/{plotname}_peak_analysis.html")
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_analysis.html")


        ##############################
        ##############################
        dfg = df_merged.groupby([TAXONOMY, 'day']).count().reset_index()
        fig = px.bar(
            dfg, 
            x='day',
            y='start_time',
            color=TAXONOMY,
            title=f'{plotname} Clases por día desde {start_date} hasta {end_date}',
            color_discrete_sequence=px.colors.qualitative.Alphabet, 
            color_discrete_map=COLOR_PALLET_PORT_L1,
            height=900,
            width=2000
        )

        # save the plot
        fig.write_html(f"{folder_output_dir}/{plotname}_peak_analysis_day.html")
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_analysis_day.html")


    except Exception as e:
        logger.error(f"Error in plot_peak_analysis: {e}")

    

def plot_peak_distribution_heatmap(df_merged: pd.DataFrame, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("whitegrid")
        df_merged = df_merged.copy()

        df_merged['full_date'] = pd.to_datetime(df_merged['start_time']).dt.strftime('%Y-%m-%d') + \
                                 '\n' + pd.to_datetime(df_merged['start_time']).dt.day_name()
        
    
        pivot_table = df_merged.pivot_table(
            index='full_date',
            columns='hour', 
            aggfunc='size',
            )

        pivot_table.replace(0, np.nan, inplace=True)

        plt.figure(figsize=(20, 9))
        sns.heatmap(pivot_table, annot=True, fmt=".0f", cmap=cmap_dict)
        
        plt.title(f'{plotname} Heatmap of Peak Occurrences')
        plt.xlabel('Hour of Day')
        # plt.ylabel('Day of Week')

        plt.yticks(np.arange(len(pivot_table.index)) + 0.5, pivot_table.index, rotation=0)
        plt.xticks(np.arange(0.5, len(pivot_table.columns), 1), range(24)) 
        
        plt.tight_layout()
        plt.grid(False)

        #save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_distribution_heatmap.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_distribution_heatmap.png")
        


        ########################################
        ########################################
        # plot the same informaiton but in 4h intervals
        ########################################
        ########################################
        # create 4 hour blocks
        df_merged['4HourBlock'] = (df_merged['start_time'].dt.hour // 4) * 4
        pivot_table_4h = df_merged.pivot_table(
            index='full_date',
            columns='4HourBlock',
            aggfunc='size',
            )
        
        pivot_table_4h.replace(0, np.nan, inplace=True)

        plt.figure(figsize=(20, 9))
        sns.heatmap(pivot_table_4h, annot=True, fmt=".0f", cmap=cmap_dict)

        plt.title(f'{plotname} Heatmap of Peak Occurrences in 4h intervals')
        plt.xlabel('4 Hour Block')
        # plt.ylabel('Day of Week')

        plt.yticks(np.arange(len(pivot_table_4h.index)) + 0.5, pivot_table_4h.index, rotation=0)
        plt.xticks(np.arange(0.5, len(pivot_table_4h.columns), 1), range(0, 24, 4))

        plt.tight_layout()
        plt.grid(False)

        #save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_distribution_heatmap_4h.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_distribution_heatmap_4h.png")




    except Exception as e:
        logger.error(f"Error in plot_peak_distribution_heatmap: {e}")




def plot_peak_distribution(df_merged: pd.DataFrame, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("whitegrid")
        df_merged = df_merged.copy()

        df_merged['start_time'] = pd.to_datetime(df_merged['start_time'])
        df_merged.sort_values('start_time', inplace=True)

        plt.figure(figsize=(25, 9))
        plt.plot(
            df_merged['start_time'], 
            df_merged['LAeq'], 
            marker='o', 
            linestyle='-', 
            color='red'
        )

        
        plt.title(f'{plotname} Peak Leq Values Over Time')
        plt.xlabel('Time')
        plt.ylabel('Leq (dB)')

        plt.xticks(rotation=90)
        plt.xlim(df_merged['start_time'].iloc[0], df_merged['start_time'].iloc[-1])
        
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=4))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
        plt.tight_layout()
        plt.grid(True)


        # #save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_distribution.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_distribution.png")



        ########################################
        ########################################
        # plot the same informaiton but with shadow area for night time
        ########################################
        ########################################
        min_date = df_merged['start_time'].dt.date.min()
        max_date = df_merged['start_time'].dt.date.max()

        plt.figure(figsize=(25, 9))
        plt.plot(df_merged['start_time'], df_merged['LAeq'], marker='o', linestyle='-', color='red')
        # highlighting night periods
        for single_date in pd.date_range(min_date, max_date):
            start_night = pd.Timestamp.combine(single_date, pd.Timestamp('20:00:00').time())
            end_night = pd.Timestamp.combine(single_date + pd.Timedelta(days=1), pd.Timestamp('07:00:00').time())
            plt.fill_betweenx(y=[df_merged['LAeq'].min(), df_merged['LAeq'].max()], 
                            x1=start_night, x2=end_night, color='grey', alpha=0.3)


        plt.title(f'{plotname} Leq Values Over Time with night periods highlighted (from 20h to 7h)')
        plt.xlabel('Time')
        plt.ylabel('Leq (dB)')

        plt.grid(True)
        plt.xticks(rotation=90)

        plt.xlim(df_merged['start_time'].iloc[0], df_merged['start_time'].iloc[-1])
        plt.ylim(df_merged['LAeq'].min(), df_merged['LAeq'].max())
        plt.tight_layout()


        # save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_distribution_night.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_distribution_night.png")


    except Exception as e:
        logger.error(f"Error in plot_peak_distribution: {e}")
        raise




def plot_density_distribution_peaks(df_merged: pd.DataFrame, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("whitegrid")
        df_merged = df_merged.copy()

        hourly_peaks = df_merged.groupby('hour').size()

        plt.figure(figsize=(12, 6))
        hourly_peaks.plot(kind='bar')

        plt.title('Peak Distribution per Hour')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Number of Peaks')
        # rotate the x-axis labels
        plt.xticks(rotation=0)
        
        plt.grid(True)
        plt.tight_layout()

        # save plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_distribution_hourly.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_distribution_hourly.png")



        ########################################
        ########################################
        # plot the same informaiton but with kernel density estimation
        ########################################
        ########################################
        time_of_day = df_merged['start_time'].dt.hour + df_merged['start_time'].dt.minute/60

        density = gaussian_kde(time_of_day)
        xs = np.linspace(0,24,100)
        density.covariance_factor = lambda : .25
        density._compute_covariance()

        plt.figure(figsize=(12, 6))
        plt.plot(xs, density(xs))
        
        plt.title(f'{plotname} Density Distribution of Peaks per Hour')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Density')

        # plt.xticks(range(25))
        plt.xlim(0, 24)
        plt.grid(True)

        plt.tight_layout()

        # save plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_density_distribution_hourly.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_density_distribution_hourly.png")



    except Exception as e:
        logger.error(f"Error in plot_density_distribution_peaks: {e}")




def plot_box_plot_prediction(df_merged: pd.DataFrame, folder_output_dir: str, logger, plotname: str):
    try:
        sns.set_style("whitegrid")
        df_merged = df_merged.copy()

        print(df_merged.columns)
        print(df_merged.head())

        sns.boxplot(data=df_merged, x=TAXONOMY, y='LAeq')

        plt.title(f'{plotname} Peak Predictions')
        plt.xlabel('Class')
        plt.ylabel('LAeq (dB)')

        plt.xticks(rotation=90)
        plt.grid(True)

        plt.ylim([30, 110])
        plt.tight_layout()

        # save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_box_plot.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_box_plot.png")



        ########################################
        ########################################
        bins = [0, 60, 70, 80, 90, 100, 110]
        labels = ['0-60', '60-70', '70-80', '80-90', '90-100', '100-110']
        df_merged['LAeq_bins'] = pd.cut(df_merged['LAeq'], bins=bins, labels=labels, include_lowest=True)

        heatmap_data = df_merged.pivot_table(
            index='NoisePort_Level_1', 
            columns='LAeq_bins', 
            values='LAeq', 
            aggfunc='count'
            ).fillna(0)
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap='Blues')
        
        plt.title(f'{plotname} Heatmap of Sound Classes by LAeq Bins')
        
        plt.xlabel('LAeq Bins (dB)')
        plt.ylabel('Class')

        plt.tight_layout()

        # save the plot
        plt.savefig(f"{folder_output_dir}/{plotname}_peak_heatmap.png", dpi=150)
        logger.info(f"Saved plot at {folder_output_dir}/{plotname}_peak_heatmap.png")



        ########################################
        ########################################
        # plot on x axies date
        # on y axis prediction and dB values
        ########################################
        plt.figure(figsize=(25, 9))
        

    except Exception as e:
        logger.error(f"Error in plot_box_plot_prediction: {e}")



def plt_spectrogram(df, folder_output_dir, logger, plotname):
    frequency_columns = df.columns[5:-2] 
    frequencies = [float(col.replace('Hz', '').replace('k', '000')) for col in frequency_columns]
    times = pd.to_datetime(df['date'])

    # select datatime from 22:30 to 23:30 of just one day
    # df = df[(df['date'] >= '2024-07-09 22:30:00') & (df['date'] <= '2024-07-09 23:30:00')]
    # times = pd.to_datetime(df['date'])

    spectrogram_data = df[frequency_columns].T.values
    spectrogram_data = spectrogram_data.clip(20, 110)
    freq_labels = [f"{freq} Hz" for freq in frequencies]
    
    plt.figure(figsize=(20, 10))
    plt.pcolormesh(times, frequencies, spectrogram_data, shading='auto', cmap='inferno')
    plt.colorbar(label='Magnitude (dB)')

    plt.yticks(frequencies, freq_labels)
    plt.ylabel('Frequency (Hz)')

    plt.xlabel('Time')
    plt.title(f'Spectrogram: {plotname}')
    plt.yscale('log')
    plt.ylim([min(frequencies), max(frequencies)])

    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=300))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    plt.xticks(rotation=90)
    plt.tight_layout()
    # remove the grid
    plt.grid(False)

    # Save the plot
    output_file = f'{folder_output_dir}/Spectrogram/{plotname}_spectrogram.png'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    plt.savefig(output_file, dpi=150)
    plt.close()
    logger.info(f"Spectrogram saved to {output_file}")