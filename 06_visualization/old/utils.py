import numpy as np
import pandas as pd
from datetime import datetime, time
import subprocess
import os
from config import *


def sum_dBs(dB_values):
    return 10 * np.log10(np.sum(np.power(10, np.array(dB_values) / 10)))


def calculate_duration(start_time, end_time):
    duration = end_time - start_time
    return duration.total_seconds()


def evaluation_period_str(hour_column):
    period = ""
    if hour_column >= 7 and hour_column < 19:
        period = "Ld"
    elif hour_column >= 19 and hour_column < 23:
        period = "Le"
    else:
        period = "Ln"
    return period


def evaluation_period_str_valencia(hour_column):
    period = ""
    if hour_column >= 8 and hour_column < 22:
        period = "Ld_valencia"
    else:
        period = "Ln_valencia"
    return period


def add_night_column(hour_column, day_col):
    night_list = [
        "Lunes-Martes",
        "Martes-Miércoles",
        "Miércoles-Jueves",
        "Jueves-Viernes",
        "Viernes-Sábado",
        "Sábado-Domingo",
        "Domingo-Lunes",
    ]
    night = ""
    if hour_column >= 23:
        night = night_list[day_col]
    elif hour_column < 7:
        night = night_list[day_col - 1]
    return night


def add_datetime_columns(df, logging, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # df['day_hour'] = df.apply(lambda x: str(x[date_col].day) + '-' + str(x[date_col].hour),axis=1)
    if df[date_col].dtype == "datetime64[ns]":
        df["date"] = df[date_col].dt.date
        df["day"] = df[date_col].dt.day
        df["hour"] = df[date_col].dt.hour
        df["weekday"] = df[date_col].dt.weekday
        df["day_name"] = df[date_col].dt.day_name()
    else:
        logging.error(f"Failed to convert {date_col} to datetime in some rows.")
    # df['min_sec_str'] = df.apply(lambda x: datetime.datetime.strftime(x[date_col],'%M:%S'),axis=1)
    # df['min_sec_15_str'] = df.apply(lambda x: str(x[date_col].minute % 15) + '-'+str(x[date_col].second),axis=1)
    return df


def add_datetime_columns_pred(df, logging, date_col):
    logging.info(f"Adding datetime columns to {date_col}...")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    if df[date_col].dtype == "datetime64[ns]":
        df["datetime"] = df[date_col].dt.date
        df["day"] = df[date_col].dt.day
        df["hour"] = df[date_col].dt.hour
        df["weekday"] = df[date_col].dt.weekday
        df["day_name"] = df[date_col].dt.day_name()
    else:
        logging.error(f"Failed to convert {date_col} to datetime in some rows.")

    return df


def db_limit(hour_column, ld_limit, le_limit, ln_limit):
    limit = 0
    if hour_column >= 7 and hour_column < 19:
        limit = ld_limit
    elif hour_column >= 19 and hour_column < 23:
        limit = le_limit
    else:
        limit = ln_limit
    return limit


def categorize_time_of_day(hour):
    # hour = time_obj.hour
    if 7 <= hour < 19:
        return "Ld"
    elif 19 <= hour < 23:
        return "Le"
    else:
        return "Ln"


def categorize_time_of_day_4(hour):
    if 7 <= hour < 11:
        return "Ld_1"
    elif 11 <= hour < 15:
        return "Ld_2"
    elif 15 <= hour < 19:
        return "Ld_3"
    elif 19 <= hour < 23:
        return "Le"
    elif 23 <= hour or hour < 3:
        return "Ln_1"
    elif 3 <= hour < 7:
        return "Ln_2"


def leq(levels):
    levels = levels[~np.isnan(levels)]
    l = np.array(levels)
    return 10 * np.log10(np.mean(np.power(10, l / 10)))


def get_day_levels(df, laeq_column):
    df["indicador_str"] = df.apply(lambda x: evaluation_period_str(x["hour"]), axis=1)
    indicadores = df.groupby("indicador_str").agg({laeq_column: [leq]}).round(1)
    return indicadores


def get_day_levels_valencia(df, laeq_column):
    df["indicador_valencia"] = df.apply(
        lambda x: evaluation_period_str_valencia(x["hour"]), axis=1
    )
    indicadores = df.groupby("indicador_valencia").agg({laeq_column: [leq]}).round(1)
    return indicadores


def remove_unnamed_columns(df_preds):
    df_preds = df_preds.loc[:, ~df_preds.columns.str.contains("^Unnamed")]
    df_preds = df_preds.drop(columns=["Brown_Level_1"])
    df_preds = df_preds.drop(columns=["index"])
    return df_preds


def yamnet_class_map_csv():
    home_dir = os.path.expanduser("~")
    yammnet_class_map_path = os.path.join(
        home_dir, RELATIVE_PATH_YAMNET_MAP.lstrip("\\")
    )
    df_audioset = pd.read_csv(yammnet_class_map_path, sep=";")
    df_audioset = remove_unnamed_columns(df_audioset)
    return df_audioset


def taxonomy_json():
    home_dir = os.path.expanduser("~")
    urban_taxonomy_map_path = os.path.join(
        home_dir, RELATIVE_PATH_TAXONOMY_URBAN.lstrip("\\")
    )
    urban_taxonomy_map = pd.read_json(urban_taxonomy_map_path, typ="series").to_dict()

    port_taxonomy_map_path = os.path.join(
        home_dir, RELATIVE_PATH_TAXONOMY_PORT.lstrip("\\")
    )
    port_taxonomy_map = pd.read_json(port_taxonomy_map_path, typ="series").to_dict()
    return urban_taxonomy_map, port_taxonomy_map


def prediction_csv(path_input):
    df_prediction = pd.read_csv(
        path_input, converters={"class": eval, "probability": eval}
    )
    columns_to_check = [
        "classes_custom",
        "probabilities_custom",
        "sum_probs_custom",
        "sum_probs_original",
    ]

    for col in columns_to_check:
        if col in df_prediction.columns:
            df_prediction = df_prediction.drop(col, axis=1)

    # columns to rename
    columns_to_rename = ["classes_original", "probabilities_original"]
    new_columns = ["classes", "probabilities"]

    for i in range(len(columns_to_rename)):
        if columns_to_rename[i] in df_prediction.columns:
            df_prediction = df_prediction.rename(
                columns={columns_to_rename[i]: new_columns[i]}
            )

    return df_prediction


def insert_dates(df):
    df["year"] = df.index.year
    df["month"] = df.index.month
    df["day"] = df.index.day
    df["hour"] = df.index.hour
    df["minute"] = df.index.minute
    df["second"] = df.index.second
    df["weekday"] = df.index.day_name()

    weekday_translation = {
        "Monday": " Lunes",
        "Tuesday": " Martes",
        "Wednesday": " Miércoles",
        "Thursday": " Jueves",
        "Friday": " Viernes",
        "Saturday": " Sábado",
        "Sunday": " Domingo",
    }
    df["weekday"] = df["weekday"].replace(weekday_translation)
    df["weekday"] = df["weekday"].astype(str)
    df["day"] = df["day"].astype(str).str.zfill(2)
    df["fullday"] = df["day"] + df["weekday"]
    return df


def remove_row_out_timespan(df_LAeq, df_Pred):
    df_LAeq.index = pd.to_datetime(df_LAeq.index)
    df_Pred["datetime"] = pd.to_datetime(df_Pred["datetime"])
    start_date = df_LAeq.index.min()
    end_date = df_LAeq.index.max()
    df_Pred_filtered = df_Pred[
        (df_Pred["datetime"] >= start_date) & (df_Pred["datetime"] <= end_date)
    ]

    return df_Pred_filtered




def apply_db_correction(df, coefficient, sufix_string, logger):
    if not "LC-LA" in df.columns and "LC" in df.columns and "LA" in df.columns:
        logger.info("Creating the LC-LA column")
        df["LC-LA"] = df["LC"] - df["LA"]

    if sufix_string == "AUDIOMOTH":
        logger.info("Applying the correction to the AUDIOMOTH data")
        if "LA" in df.columns:
            logger.info("Applying the correction to the LA column")
            df["LA_corrected"] = df["LA"] - coefficient
            df["LAmax_corrected"] = df["LAmax"] - coefficient
            df["LAmin_corrected"] = df["LAmin"] - coefficient
            df["LCeq-LAeq_corrected"] = df["LC-LA"] - coefficient

    if sufix_string == "SONOMETRO":
        logger.info("Applying the correction to the SONOMETRO data")
        if "LAeq" in df.columns:
            logger.info("Applying the correction to the LAeq column")
            df["LA_corrected"] = df["LAeq"] - coefficient
            df["LAmax_corrected"] = df["LAFmax"] - coefficient
            df["LAmin_corrected"] = df["LAFmin"] - coefficient
            df["LCeq-LAeq_corrected"] = df["LCeq-LAeq"] - coefficient


        elif "Value" in df.columns:
            logger.info("Applying the correction to the Value column")
            df["LA_corrected"] = df["Value"] - coefficient

    # elif '' in df.columns:
    #     df['LA_corrected'] = df[''] - coefficient

    else:
        logger.error("No column found to apply the correction")

    return df


def transform_1h(df, columns_dict, logger, agg_period):
    df = df.dropna(subset=[columns_dict["LAEQ_COLUMN_COEFF"]])

    # if there is just LAEQ_COLUMN_COEFF, then we use it for all the columns, otherwise use the max and min
    if columns_dict["LAEQ_COLUMN"] == "Value":
        agg_funcs = {
            columns_dict["LAEQ_COLUMN_COEFF"]: [leq, lambda x: x.quantile(0.9)]
        }
        logger.info(
            f"Using the columns_dict: df_LAeq[{columns_dict['LAEQ_COLUMN_COEFF']}]"
        )

    else:
        agg_funcs = {
            columns_dict["LAEQ_COLUMN_COEFF"]: [leq, lambda x: x.quantile(0.9)],
            columns_dict["LAMAX_COLUMN_COEFF"]: "max",
            columns_dict["LAMIN_COLUMN_COEFF"]: "min",
            columns_dict["LC-LA_COLUMN_COEFF"]: leq,
        }

    logger.info(f"Using the agg_funcs: {agg_funcs}")
    df_LAeq = df.resample(f"{agg_period}s").agg(agg_funcs)
    df_LAeq.columns = ["_".join(col).strip() for col in df_LAeq.columns.values]

    # rename column
    if "LA_corrected_<lambda_0>" in df_LAeq.columns:
        df_LAeq = df_LAeq.rename(columns={"LA_corrected_<lambda_0>": "90percentile"})

    # logger.info(f"Resampled data with 90th percentile: {df_LAeq}")

    return df_LAeq


def add_random_data(data):
    data["datetime"] = pd.to_datetime(data["datetime"])
    last_date = data["datetime"].max()

    # start and end date for the new data
    start_date = last_date + pd.Timedelta(hours=1)
    end_date = start_date + pd.DateOffset(years=2) - pd.Timedelta(hours=1)

    # date range for every hour between the start and end date
    date_range = pd.date_range(start=start_date, end=end_date, freq="h")
    mean_values = data.mean()
    std_values = data.std()

    # generate random data using normal distribution centered around the mean and std deviation
    np.random.seed(42)
    random_data = {
        "datetime": date_range,
        "LA_corrected_leq": np.random.normal(
            mean_values["LA_corrected_leq"],
            std_values["LA_corrected_leq"],
            len(date_range),
        ),
        "90percentile": np.random.normal(
            mean_values["90percentile"], std_values["90percentile"], len(date_range)
        ),
        "LAmax_corrected_max": np.random.normal(
            mean_values["LAmax_corrected_max"],
            std_values["LAmax_corrected_max"],
            len(date_range),
        ),
        "LAmin_corrected_min": np.random.normal(
            mean_values["LAmin_corrected_min"],
            std_values["LAmin_corrected_min"],
            len(date_range),
        ),
        "LCeq-LAeq_corrected_leq": np.random.normal(
            mean_values["LCeq-LAeq_corrected_leq"],
            std_values["LCeq-LAeq_corrected_leq"],
            len(date_range),
        ),
    }

    new_data = pd.DataFrame(random_data)
    full_data = pd.concat([data, new_data], ignore_index=True)
    full_data = full_data.sort_values(by="datetime")
    full_data = full_data.round(2)

    # set datetime as index but without dropping the column
    full_data.set_index("datetime", inplace=True, drop=False)

    return full_data


def transformation(df, logger, LIMITE_DIA, LIMITE_TARDE, LIMITE_NOCHE):
    # transformation
    df = add_datetime_columns(df, logger, date_col="datetime")
    df = df.sort_values("datetime")
    df.set_index("datetime", inplace=True, drop=False)
    df = df.rename(columns={"datetime": "date_time"})
    start_date = df.index[0]
    end_date = df.index[-1]
    # add indicators column
    logger.info(f"Adding indicators column")
    df["indicador_str"] = df.apply(lambda x: evaluation_period_str(x["hour"]), axis=1)
    # add nights column
    logger.info(f"Adding nights column")
    df["night_str"] = df.apply(
        lambda x: add_night_column(x["hour"], x["weekday"]), axis=1
    )
    # removing nan values
    df = df.dropna()
    # oca column
    df["oca"] = df.apply(
        lambda x: db_limit(
            x["hour"], ld_limit=LIMITE_DIA, le_limit=LIMITE_TARDE, ln_limit=LIMITE_NOCHE
        ),
        axis=1,
    )

    return df


def transformation_fake(df, logger, LIMITE_DIA, LIMITE_TARDE, LIMITE_NOCHE):
    df = df.rename(columns={"datetime": "date_time"})
    df = add_datetime_columns(df, logger, date_col="date_time")
    df = df.sort_values("date_time")
    df.set_index("date_time", inplace=True, drop=False)
    start_date = df.index[0]
    end_date = df.index[-1]
    logger.info("Adding indicators column")
    df["indicador_str"] = df.apply(lambda x: evaluation_period_str(x["hour"]), axis=1)
    logger.info("Adding nights column")
    df["night_str"] = df.apply(
        lambda x: add_night_column(x["hour"], x["weekday"]), axis=1
    )
    df = df.dropna()
    df["oca"] = df.apply(
        lambda x: db_limit(
            x["hour"], ld_limit=LIMITE_DIA, le_limit=LIMITE_TARDE, ln_limit=LIMITE_NOCHE
        ),
        axis=1,
    )
    return df


def list_git_tags():
    try:
        tags = tags = subprocess.check_output(["git", "tag"]).strip().decode()
        return tags.split("\n")
    except subprocess.CalledProcessError:
        return None


def select_tag(tags, logger):
    for i, tag in enumerate(tags):
        logger.info(f"{i}: {tag}")

    choice = int(input("Select the tag to use: "))
    tag_selected = tags[choice]
    tag_selected = tag_selected.replace(".", "_")
    return tag_selected


def get_stable_version(logger):
    tags = list_git_tags()
    # get the latest stable version
    tag_selected = tags[-1]
    logger.info(f"Latest stable version: {tag_selected}")

    tag_selected = tag_selected.replace(".", "_")
    logger.info(f"Latest stable version string: {tag_selected}")
    return tag_selected
