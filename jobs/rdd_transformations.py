from spark_initial import spark
global appName 
appName= "firstsparkapp"
def read_file(file_path):
    df = spark(appName).read.csv(file_path, header=True, inferSchema=True)
    # create an rdd using key as (channel, rec_device_id)

    paired_rdd = df.rdd.map(lambda row: ((row.channel, row.rec_device_id), (row.timestamp, row.value)))
    return paired_rdd
def main():
    rdd = read_file( 'data/channel_data.csv')
    grouped_rdd = rdd.groupByKey()

    # print all unique keys in the rdd
    # print(grouped_rdd.keys().distinct().collect())

    # print max of column value in each group of the rdd grouped_rdd
    # print(grouped_rdd.map(lambda x: (x[0], max([y[1] for y in x[1]]))).collect())

    # get number of records in each group of the rdd grouped_rdd
    print(grouped_rdd.map(lambda x: (x[0], len(x[1]))).collect())

if __name__ == '__main__':
    main()
    spark(appName).stop()
