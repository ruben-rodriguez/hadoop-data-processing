# java-hadoop-data-processing
Data Processing applications written in Java and ported to Hadoop.
The purpose is to extract statistics from this Dataset [Spanish Rail Tickets Pricing - Renfe](https://www.kaggle.com/thegurusteam/spanish-high-speed-rail-system-ticket-pricing)

## Base Java App: javaDataProcessing

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

After assembly is complete, jar can be executed by issuing:

```
$: java -jar target/javaDataProcessing-1.0-SNAPSHOT-jar-with-dependencies.jar
```