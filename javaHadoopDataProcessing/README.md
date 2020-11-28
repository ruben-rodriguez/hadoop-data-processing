## Hadoop Java App: javaHadoopDataProcessing

This Java application accepts cli options to execute Hadoop mapreduce calculations:

```
usage: javaHadoopDataProcessing [-a <arg>] [-h] [-i <arg>] [-o <arg>]
 -a,--app <arg>      Application to execute: Shedules, Vehicles, Locations, Prices, LocationsVehicles
 -h,--help           Show help
 -i,--input <arg>    Hadoop input dir
 -o,--output <arg>   Hadoop output dir
```
Applications - calcualtions mapping:

- Schedules: Count schedules.
- Locations: Count trip origins and destinations.
- Vehicles: Count type of vehicles.
- Prices: Mean prices per trip class.
- LocationsVehicles: Type of train by origin and destination.


Calculations are performed taking files located at Hadoop input dir (-i arg) and output is then placed in -o arg indicated

#### Requirements

- Java: 1.8.0
- Maven: Apache Maven 3.6.3
- Hadoop 3.3.0

#### Compile

```
$: mvn clean compile
```

#### Assembly

```
$: mvn clean compile assembly:single
```

After assembly is complete, all application dependencies are packaged into a jar that can be executed.

#### Run in Hadoop

```
$: hadoop jar target/javaHadoopDataProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar -a Prices -i /dataProcessing/input -o /dataProcessing/output
```

