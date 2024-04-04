# Statistics using Apache Spark v 1.0.1
The statistics algorithms are written in Scala and run using Apache Spark. Before being executed they must be built into a .jar file using these commands inside the repo:

    sbt clean
    sbt assembly

The statistics available at the moment are:

 - Mean
 - Standard Deviation
 - Maximum
 - Minimum

Versions of technologies used in the process:
 
 - [x] Spark 17
 - [x] Spark 3.4.0
 - [x] Scala 2.12.17
 - [x] Circe 0.13.0
 - [x] HBase 2.5.3
 - [x] sbt 1.6.0
