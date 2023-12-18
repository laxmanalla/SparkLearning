from spark_initial import spark
import datetime
import datetime
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

    # print all records in first group of the rdd grouped_rdd
    # first_key = grouped_rdd.first()[0]
    # first_key_records = grouped_rdd.filter(lambda x: x[0] == first_key).flatMap(lambda x: x[1])
    # timestamp_value = first_key_records.collect()
    # print(timestamp_value)
    # print(grouped_rdd.take(4))

    # print all unique keys in the rdd
    # print(grouped_rdd.keys().distinct().collect())

    # print max of column value in each group of the rdd grouped_rdd
    # print(grouped_rdd.map(lambda x: (x[0], max([y[1] for y in x[1]]))).collect())

    # get number of records in each group of the rdd grouped_rdd
    # print(grouped_rdd.map(lambda x: (x[0], len(x[1]))).collect())

    # get a specific record in the value column based on timestamp datetime.datetime(2023, 12, 18, 6, 23, 42, 707386)
    print(grouped_rdd.map(lambda x: (x[0], [y[1] for y in x[1] if y[0] == datetime.datetime(2023, 12, 18, 6, 23, 42, 707386)])).collect())
    timestamp = datetime.datetime(2023, 1, 12, 17, 0, 34, 943478)


if __name__ == '__main__':
    main()
    spark(appName).stop()
