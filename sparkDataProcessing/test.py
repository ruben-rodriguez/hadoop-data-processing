import sys, time

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


start = time.time()
print("hello")
end = time.time()
print(end - start)

sparkSession = SparkSession.builder.appName("Hadoop-Data-Processing").getOrCreate()
df_load = sparkSession.read.option("header",True).csv('hdfs://hadoop-master:9000/dataProcessing/input/sample.csv')

print("Counting Vehicle types")
start = time.time()
df_load.groupBy('vehicle_type').count().select('vehicle_type', f.col('count').alias('count')).show()
end = time.time()
print("It took: ", end - start, " seconds")


df_load.groupBy('destination').count().select('destination', f.col('count').alias('count')).show()

df_load.groupBy('origin').count().select('origin', f.col('count').alias('count')).show()


df_load.groupBy('destination', 'vehicle_type').count().select('destination' , 'vehicle_type', f.col('count').alias('count')).show()
df_load.groupBy('origin', 'vehicle_type').count().select('origin', 'vehicle_type', f.col('count').alias('count')).show()


df_load.groupBy('vehicle_class').agg(
    f.avg("price").alias("avg_price"), \
    f.max("price").alias("max_price") \
).dropna().show()

df_load.withColumn('Day', f.date_format('departure', 'EEEE')).groupBy('Day').count().select('Day', f.col('count').alias('count')).show()

df_load.withColumn('Time', f.date_format('departure', 'H:m')).groupBy('Time').count().select('Time', f.col('count').alias('count')).show()
