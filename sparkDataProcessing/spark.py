import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

df_load = sparkSession.read.csv('hdfs://cluster/user/hdfs/test/example.csv')

df_load.show()

df_load.groupBy('vehicle').count().select('vehicle', f.col('count').alias('count')).show()