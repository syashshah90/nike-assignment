# Nike Assignment

The idea of this assignment is to create JSON extract files from various csv sources based on the conditions (weekly aggregator).

## Tools and Technologies used
- Spark 2.0.0
- Scala 2.10
- Intellij 2018.1.5 (Community Edition)

## Prerequisites

1. Downloaded sparkscalaassignment_jar.jar from the given link
2. Up and running Spark Cluster

## Step-by-step

1. Download the JAR sparkscalaassignment_jar.jar in your working folder
2. Get all the input csv files and place them in your input folder (Source files sales.csv, calendar.csv, store.csv and product.csv)
3. Determine the path for your output
5. Run the following spark-submit command to execute the code

$./bin/spark-submit --class SparkTest /\
--master local[*] /\
--deploy-mode client /\
/path-to-jar/sparkscalaassignment_jar.jar /path/input-dir-path /path/output-dir-path
