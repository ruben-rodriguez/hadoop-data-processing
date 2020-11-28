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
