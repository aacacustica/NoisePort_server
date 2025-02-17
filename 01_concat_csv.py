import os
import pandas as pd


def get_csv_data(file_path:str):
    # open csv file
    df = pd.read_csv(file_path)
    return df


def concatenate_csv_files(folder_path: str):
    # get all the files in the folder
    files = os.listdir(folder_path)
    # filter only csv files
    csv_files = [file for file in files if file.endswith(".csv")]
    # print(csv_files)

    # read all the csv files
    dfs = []
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        # print(file_path)
        df = get_csv_data(file_path)
        # print(df)
        
        dfs.append(df)
    
    # concatenate all the dataframes
    df = pd.concat(dfs)
    df = df.sort_values(by='Timestamp')
    return df



def find_csv_files_path():
    path = '/home/aac_s3_test/retrieve_Data/downloads/CONTENEDORES/P1_CONTENEDORES/acoustic_params'
    return path



def main():
    cwd = os.path.dirname(os.path.realpath(__file__))
    home_dir = os.getenv("HOME")
    # print(cwd)
    # print(home_dir)

    folder_path =find_csv_files_path() 

    # folder_path = 'asdasd'
    df = concatenate_csv_files(folder_path)
    df = df.sort_values(by='Timestamp')
    print(df)

    # save csv in the folder_path
    df.to_csv(os.path.join(folder_path, "concatenated.csv"), index=False)
    print("CSV file saved")


if __name__ == '__main__':
    main()