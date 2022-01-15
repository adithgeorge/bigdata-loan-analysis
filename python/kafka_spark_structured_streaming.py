

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import datetime


# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5


def createSparkSession(master, appName):
    """
    Function to create a spark session
    
    """
    #  Set your local host to be the master node of your cluster
    # Set the appName for your Spark session
    # Join session for app if it exists, else create a new one

    spark = SparkSession.builder.master(master)\
        .appName(appName)\
        .getOrCreate()

    # ERROR log level will generate fewer lines of output compared to INFO and DEBUG
    spark.sparkContext.setLogLevel("ERROR")

    return spark



def add_timestamp():
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp



def main():
    spark = createSparkSession("local","SparkKafkaStreamingApp")

    # Reading from two Kafka topics to Pyspark. Here the input format is mentioned as Kafka
    # and stream is being read from the earliest offset from topics test_read and test_read2

    df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","test_read,test_read2")\
    .option("startingOffsets","earliest")\
    .load()    


    # Printing the schema to understand the streaming data structure. Here we observe that
    # additional data like key, topic, partition, offset number, timestamp also come along with
    # the “value” i.e the data itself. Further processing can be done on the value to extract
    # what we require.

    df.printSchema()

    # Retrieving specific columns from the data stream and applying window and watermark
    # operations on the timestamp column and also grouping based on specific topic.

    df = df.select(col("value").cast(StringType()),col("topic"),col("offset"),col("timestamp"))
    df = df.select(col("value").cast(StringType()),col("topic"),col("timestamp"))
    windowCountDf = df.withWatermark("timestamp","2 seconds").groupBy("topic",window("timestamp","2 seconds", "1 seconds")).count()

    # Writing the processed data to multiple sinks in this order:
    # Console with output mode “append”
    # Local File Sink with output mode “append”. The path location is specified.
    # Kafka Consumer with output mode “append”. The kafka port and topic is
    # mentioned here.

    query = windowCountDf.writeStream\
    .outputMode("append")\
    .format("console")\
    .option("truncate","false")\
    .option("numRows",200)\
    .start()

    query = df.writeStream\
    .outputMode("append")\
    .format("json")\
    .option("path","file:///home/ak/project_data/kafka_out")\
    .option("truncate","false")\
    .option("checkpointLocation", "file:///home/ak/checkpoint")\
    .option("numRows",2000)\
    .start()

    query = df.writeStream\
    .outputMode("append")\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","test_read3")\
    .option("truncate","false")\
    .option("checkpointLocation", "file:///home/ak/checkpoint")\
    .option("numRows",2000)\
    .start()

    


if __name__ == "__main__":
    main()

