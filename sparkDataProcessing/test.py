import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

#from pyspark.sql.functions import sum, avg, max, min, mean, count, date_format

sparkSession = SparkSession.builder.appName("example-pyspark-read").getOrCreate()

df_load = sparkSession.read.option("header",True).csv('hdfs://hadoop-master:9000/dataProcessing/input/sample.csv')

df_load.show()

df_load.groupBy('vehicle_type').count().select('vehicle_type', f.col('count').alias('count')).show()
#df_load.write.csv("hdfs://hadoop-master:9000/dataProcessing/output/vehicleTypes.csv")

df_load.groupBy('destination').count().select('destination', f.col('count').alias('count')).show()

df_load.groupBy('origin').count().select('origin', f.col('count').alias('count')).show()


df_load.groupBy('destination', 'vehicle_type').count().select('destination', f.col('count').alias('count')).show()


df_load.groupBy('origin', 'vehicle_type').count().select('origin', f.col('count').alias('count')).show()


df_load.groupBy('vehicle_class').agg(
    f.avg("price").alias("avg_price"), \
    f.max("price").alias("max_price") \
).dropna().show()

df_load.withColumn('Day', f.date_format('departure', 'EEEE')).groupBy('Day').count().select('Day', f.col('count').alias('count')).show()

df_load.withColumn('Time', f.date_format('departure', 'Hm')).groupBy('Time').count().select('Time', f.col('count').alias('count')).show()
