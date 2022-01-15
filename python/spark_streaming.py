
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import datetime

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
    spark = createSparkSession("local","SparkStreamingApp")

    # A user defined schema for streaming data being read real time

    loan_schema = StructType()\
    .add("end_of_period",StringType(),True)\
    .add("loan_number",StringType(),True)\
    .add("region",StringType(),True)\
    .add("country_code",StringType(),True)\
    .add("country",StringType(),True)\
    .add("borrower",StringType(),True)\
    .add("guarantor_country_code",StringType(),True)\
    .add("guarantor",StringType(),True)\
    .add("loan_type",StringType(),True)\
    .add("loan_status",StringType(),True)\
    .add("interest_rate",FloatType(),True)\
    .add("currency_of_commitment",StringType(),True)\
    .add("project_id",StringType(),True)\
    .add("project_name",StringType(),True)\
    .add("original_principal_amount",DoubleType(),True)\
    .add("cancelled_amount",DoubleType(),True)\
    .add("undisbursed_amount",DoubleType(),True)\
    .add("disbursed_amount",DoubleType(),True)\
    .add("repaid_to_ibrd",DoubleType(),True)\
    .add("due_to_ibrd",DoubleType(),True)\
    .add("exchange_adjustment",DoubleType(),True)\
    .add("borrowers_obligation",DoubleType(),True)\
    .add("sold_3rd_party",DoubleType(),True)\
    .add("repaid_3rd_party",DoubleType(),True)\
    .add("due_3rd_party",IntegerType(),True)\
    .add("loans_held",DoubleType(),True)\
    .add("first_repayment_date",StringType(),True)\
    .add("last_repayment_date",StringType(),True)\
    .add("agreement_signing_date",StringType(),True)\
    .add("board_approval_date",StringType(),True)\
    .add("effective_date_most_recent",StringType(),True)\
    .add("closed_date_most_recent", StringType(),True)\
    .add("last_disbursement_date",StringType(),True)


    # Reading the data from a drop location with the the user defined schema and also
    # explicitly mentioning the maximum number of file to be read for a trigger

    loanDF = spark.readStream\
    .option("header", "true")\
    .schema(loan_schema)\
    .option("maxFilesPerTrigger",1)\
    .csv("file:///home/ak/project_data/kafka_in/")

    print(loanDF.isStreaming)

    print(loanDF.printSchema())

    # Selecting specific columns and adding a timestamp column for performing window and watermark operations further

    trimDF = loanDF.select( "loan_number", "country", "loan_type", "project_name","loan_status")

    timestampDF = trimDF.withColumn("timestamp",current_timestamp())

    add_timestamp_udf = udf(add_timestamp, StringType())

    timestampDF = trimDF.withColumn("timestamp", add_timestamp_udf())

    # Performing aggregation on the data to get an idea of the loan status column. Window
    # and watermarking concepts are used to perform aggregation for a particular time frame

    countDF = timestampDF.groupBy("loan_status").count().orderBy("count",ascending= False)

    countDF = timestampDF \
    .withWatermark("timestamp","2 seconds") \
    .groupBy("loan_status", window("timestamp","2 seconds", "1 seconds")) \
    .count().orderBy("count",ascending=False)

    # Writing the output to the console where the output mode is “complete” so that we get all the aggregated result

    query = countDF.writeStream\
    .outputMode("complete")\
    .format("console")\
    .option("truncate", "false")\
    .option("numRows", 30)\
    .start()\
    .awaitTermination()


    


if __name__ == "__main__":
    main()

