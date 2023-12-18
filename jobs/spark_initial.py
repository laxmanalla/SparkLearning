import pyspark
from pyspark.sql import SparkSession


def spark(application_name):
    return SparkSession.builder \
            .appName(application_name) \
            .getOrCreate()


