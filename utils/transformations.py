# write a native python funtion to group all records in csv data/channel_data.csv by channel and rec_device_id and get count in each group

# In[ ]:
import pandas as pd
import numpy as np

def read_file(file_path):
    df = pd.read_csv(file_path)
    # create an rdd using key as (channel, rec_device_id)
    paired_rdd = df.groupby(['channel', 'rec_device_id'])
    return paired_rdd

def main():
    rdd = read_file( 'data/channel_data.csv')
    grouped_rdd = rdd
    # print all unique keys in the rdd
    print(grouped_rdd.groups.keys())
    # print max of column value in each group of the rdd grouped_rdd
    print(grouped_rdd['value'].max())
    # get number of records in each group of the rdd grouped_rdd
    print(grouped_rdd['value'].count())


if __name__ == '__main__':
    main()
    
