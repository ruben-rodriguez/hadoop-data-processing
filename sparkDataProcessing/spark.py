import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.functions import sum, avg, max, min, mean, count

sparkSession = SparkSession.builder.appName("example-pyspark-read").getOrCreate()

df_load = sparkSession.read.option("header",True).option("mode", "DROPMALFORMED").csv('hdfs://hadoop-master:9000/dataProcessing/input/sample.csv')

df_load.show()

df_load.groupBy('vehicle_type').count().select('vehicle_type', f.col('count').alias('count')).show()

df_load.groupBy('destination').count().select('destination', f.col('count').alias('count')).show()
df_load.groupBy('origin').count().select('origin', f.col('count').alias('count')).show()

df_load.groupBy('destination', 'vehicle_type').count().select('destination', f.col('count').alias('count')).show()
df_load.groupBy('origin', 'vehicle_type').count().select('origin', f.col('count').alias('count')).show()

df_load.groupBy('vehicle_class').agg(
    avg("price").alias("avg_price"), \
    max("price").alias("max_price") \
).show(truncate=False)