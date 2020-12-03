# hadoop-data-processing
Data Processing applications.
The purpose is to extract statistics from this Dataset: [Spanish Rail Tickets Pricing - Renfe](https://www.kaggle.com/thegurusteam/spanish-high-speed-rail-system-ticket-pricing)

## Base Java App: javaDataProcessing

This Java application shows a menu on sysout of different calculations available to perform.
Calculations are performed taking ```sample.csv``` file located at ```javaDataProcessing/src/main/resources/sample.csv```.
More info: [javaDataProcessing README](javaDataProcessing/README.md)

## Hadoop Java App: javaHadoopDataProcessing

This Java application accepts cli options to execute Hadoop mapreduce calculations.
Calculations are performed taking files located at Hadoop input dir (-i arg) and output is then placed in -o arg indicated
More info: [javaHadoopDataProcessing README](javaHadoopDataProcessing/README.md)

## Spark Python app: spark-data-processing

This Python application performs all calculations in a single shot leveraging Spark on hadoop:
Calculations are performed taking files located at Hadoop static dir ```hdfs://hadoop-master:9000/dataProcessing/input/sample.csv```
More info: [spark-data-processing README](spark-data-processing/README.md)