
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import datetime

# Configuration to be mentioned in spark-default.conf for integration

# spark.hadoop.fs.s3a.access.key *******************
# spark.hadoop.fs.s3a.secret.key *********************************
# spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
# spark.hadoop.fs.s3n.access.key *******************
# spark.hadoop.fs.s3n.secret.key *********************************
# spark.hadoop.fs.s3n.impl org.apache.hadoop.fs.s3native.NativeS3FileSystem


# pyspark --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7

# Dependencies and package required for snowflake integration

# net.snowflake:snowflake-jdbc:3.11.1, net.snowflake:spark-snowflake_2.11:2.5.7-spark_2.4


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
    timestamp = datetime.datetime.fromtimestamp(
        ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp


def main():
    spark = createSparkSession("local", "SparkAWSApp")

    # Setting AWS access credentials for spark

    spark._jsc.hadoopConfiguration().set("fs.s3n.aws.AccessKeyId","***************")
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey","********************************")


    # Reading file from AWS S3

    df = spark.read.option("header","True")  \
    .option("inferSchema","True") \
    .csv("s3n://lti858/Adith/hive_ext/latest_loans_ext/IBRD_Statement_of_Loans_-_Latest_ Available_Snapshot.csv")
    
    df.write.csv("s3n://lti858/Adith/loans.csv")


    # Connecting to Snowflake DB

    sfOptions = {"sfURL" : "zta84516.us-east-1.snowflakecomputing.com", 
    "sfUser" : "Kartiq", "sfPassword" : "*******",
    "sfDatabase" : "LTISNOWDB",
    "sfSchema" : "PUBLIC", 
    "sfWarehouse" : "COMPUTE_WH"}

    df = spark.read.format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("query", "select * from LOANS_FINANCE_2 limit 5") \
    .load()

    df.show()


    tempS3Dir="s3://path/for/temp/data"
    jdbcURL="jdbc:redshift://redshifthost:5439/database?user=username&password=pass"

    df = spark.read.format("redshift") \
    .option("url", jdbcURL) \
    .option("dbtable", "tbl").option("tempdir", tempS3Dir) \
    .load()
    

    df = spark.read.format("com.qubole.spark.redshift") \
    .option("url", jdbcURL) \
    .option("query", "select col1, col2 from tbl group by col3") \
    .option("forward_spark_s3_credentials", "true").option("tempdir",tempS3Dir).load()

    

    


if __name__ == "__main__":
    main()

