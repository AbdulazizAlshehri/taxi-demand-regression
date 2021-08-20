# taxi-demand-regression
This is a MapReduce program that uses ST-Hadoop in querying spatial-temporal taxi data at a specific area in a specific time interval to predict taxi demand in NYC.

# Prerequisites
In order to run taxi demand regression, you need to have at least **Java 1.8**, Hadoop installed and running, we used **Hadoop 2.9** but any **Hadoop 2.x** should work fine, in case you need to download and compile [ST-Hadoop](https://github.com/lmarabi/st-hadoop) on your own machine then we suggest to have **Maven 3.6** installed for compiling and assembling, otherwise, you can use pre-compiled `sthadoop-2.4.1-SNAPSHOT-uber.jar` directly.

# Setup
To run Taxi Demand Regression, you need to place NYC taxi data in Hadoop HDFS, let's suppose that you have created an input directory in Hadoop HDFS as following `hadoop fs -mkdir /input_dir/` then you will place input files by running something like `hadoop fs -put <file-path>/<file-name> /input_dir/`, now you have input files in the HDFS ready, you need to run ST-Hadoop indexer to index the input files by time and locations, here is an example of how to run indexer `hadoop jar <path to sthadoop jar file>/sthadoop-2.4.1-SNAPSHOT-uber.jar stmanager /input_dir/* /output_dir/ time:day shape:edu.umn.cs.sthadoop.core.STpointsTaxi sindex:rtree -overwrite`, for more information of how to use indexer read [ST-Hadoop indexer](http://st-hadoop.cs.umn.edu/getting-started/spatio-temporal-index). Now you have input file ready for quering.

# Running
After having the input data prepared and indexed in the HDFS, you can:
1. run query the data that the model will be trained on by running ST-Hadoop Range Query by using commands like `hadoop jar <path to sthadoop jar file>/sthadoop-2.4.1-SNAPSHOT-uber.jar strangequery /<HDFS indexed data directory>/ /<query output directory> rect:<main_latitude>,<min_longitude>,<max_latitude>,<max_longitude> interval:<start_time>,<end_time> shape:edu.umn.cs.sthadoop.core.STpointsTaxi time:day -overwrite`, for more information on how to use this query read [ST-Hadoop Range Query](http://st-hadoop.cs.umn.edu/getting-started/spatio-temporal-range-query). 
2. then you can run the MapReduce Taxi Demand Regression by running comands like 
```
hadoop  jar /<path to hadoop-streaming jar file>/hadoop-streaming-2.9.x.jar \
	-file Mapper.py -mapper Mapper.py \
	-file Reducer.py -reducer Reducer.py \
	-input /<range query output directory>/* -output /predictions_output/
```
Please note that you need to place `Mapper.py` & `Reducer.py` in the same directory where you run this command otherwise, you need to provide the full path to mapper and reducer files.
