# read all .csv files in a directory and create a single dataframe using pandas

import pandas as pd
import glob as glob

def read_files(file_path):
    all_files = glob.glob(file_path + "/*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    return pd.concat(li, axis=0, ignore_index=True)


def main():
    df = read_files('data')
    print(df)


if __name__ == '__main__':

    main()
    