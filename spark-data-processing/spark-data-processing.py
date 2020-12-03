import sys, time

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

sparkSession = SparkSession.builder.appName("Hadoop-Data-Processing").getOrCreate()
df_load = sparkSession.read.option("header",True).csv('hdfs://hadoop-master:9000/dataProcessing/input/sample.csv')

print("Vehicle types count")
start = time.time()
df_load.groupBy('vehicle_type').count().select('vehicle_type', f.col('count').alias('count')).show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")

print("Destination and origin counts:")
start = time.time()
df_load.groupBy('destination').count().select('destination', f.col('count').alias('count')).show(False)
df_load.groupBy('origin').count().select('origin', f.col('count').alias('count')).show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")

print("Vehicle type counts by origin and destination")
start = time.time()
df_load.groupBy('destination', 'vehicle_type').count().select('destination' , 'vehicle_type', f.col('count').alias('count')).show(False)
df_load.groupBy('origin', 'vehicle_type').count().select('origin', 'vehicle_type', f.col('count').alias('count')).show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")

print("Price mean and max")
start = time.time()
df_load.groupBy('vehicle_class').agg(
    f.avg("price").alias("avg_price"), \
    f.max("price").alias("max_price") \
).dropna().show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")

print("Schedule counts by weekday")
start = time.time()
df_load.withColumn('Day', f.date_format('departure', 'EEEE')).groupBy('Day').count().select('Day', f.col('count').alias('count')).show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")


print("Schedule counts by time")
start = time.time()
df_load.withColumn('Time', f.date_format('departure', 'H:m')).groupBy('Time').count().select('Time', f.col('count').alias('count')).show(False)
end = time.time()
print("It took: ", round( end - start, 2), " seconds\n\n")