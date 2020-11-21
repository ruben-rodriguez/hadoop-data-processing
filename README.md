# java-hadoop-data-processing
Data Processing applications written in Java and ported to Hadoop.
The purpose is to extract statistics from this Dataset: [Spanish Rail Tickets Pricing - Renfe](https://www.kaggle.com/thegurusteam/spanish-high-speed-rail-system-ticket-pricing)

## Base Java App: javaDataProcessing

This Java application shows a menu on sysout of different calculations available to perform:

```
        1 - Most frequent schedules.
        2 - Most frequent trip origins and destinations.
        3 - Most frequent type of vehicle.
        4 - Mean price per trip class.
        5 - Type of train by origin and destination.
        6 - Exit
```

Calculations are performed taking ```sample.csv``` file located at ```javaDataProcessing/src/main/resources/sample.csv```.

#### Requirements

- Java: 1.8.0
- Maven: Apache Maven 3.6.3

#### Compile

```
$: mvn compile
```

#### Run

```
$: mvn exec:java -Dexec.mainClass="com.ruben.javaDataProcessing.App"
```

#### Assembly

```
$: mvn clean compile assembly:single
```

After assembly is complete, all application dependencies and resources (sample.csv) are packaged into a jar that can be executed by issuing:

```
$: java -jar target/javaDataProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar
```