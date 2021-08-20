# taxi-demand-regression
This is a MapReduce program that uses ST-Hadoop in querying spatial-temporal taxi data at a specific area in a specific time interval to predict taxi demand in NYC.

# Prerequisites
In order to run taxi demand regression, you need to have at least **Java 1.8**, Hadoop installed and running, we used **Hadoop 2.9** but any **Hadoop 2.x** should work fine, in case you need to download and compile [ST-Hadoop](https://github.com/lmarabi/st-hadoop) on your own machine then we suggest to have **Maven 3.6** installed for compiling and assembling, otherwise, you can use pre-compiled `sthadoop-2.4.1-SNAPSHOT-uber.jar` directly.

# Setup
To run Taxi Demand Regression, you need to place NYC taxi data in Hadoop HDFS, let's suppose that you have created an input directory in Hadoop HDFS as following `hadoop fs -mkdir /input_dir/` then you will place input files by running something like `hadoop fs -put <file-path>/<file-name> /input_dir/`, now you have input files in the HDFS ready, you need to run ST-Hadoop indexer to index the input files by time and locations, here is an example of how to run indexer `hadoop jar <path to sthadoop jar file>/sthadoop-2.4.1-SNAPSHOT-uber.jar stmanager /input_dir/* /output_dir/ time:day shape:edu.umn.cs.sthadoop.core.STpointsTaxi sindex:rtree -overwrite`, for more information of how to use indexer read [ST-Hadoop indexer](http://st-hadoop.cs.umn.edu/getting-started/spatio-temporal-index). Now you have input file ready for quering.

# Run
After preparing data in HDFS input directory, you can run Taxi Demand Regression by using ST-Hadoop Range Query by running command like ``
