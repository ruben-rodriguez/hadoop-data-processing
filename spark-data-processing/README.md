## Spark Python app: spark-data-processing

This Python application performs all calculations in a single shot leveraging Spark on hadoop:

- Count schedules.
- Count trip origins and destinations.
- Count type of vehicles.
- Mean/max prices per trip class.
- Type of train by origin and destination.

Calculations are performed taking files located at Hadoop static dir ```hdfs://hadoop-master:9000/dataProcessing/input/sample.csv```

#### Requirements

- Python 3.8.5+
- Hadoop 3.3.0
- Spark 3.0.1 (on hadoop)

#### Run

The script is executed by submitting as spark job, redirecting stdout to a file so we can keep results of calculations:

```
$: spark-submit sparkDataProcessing/test.py > spark-run.log
```

