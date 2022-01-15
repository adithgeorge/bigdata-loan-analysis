
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from delta.tables import *

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


def main():
    spark = createSparkSession("local","SparkDeltaApp")

    events = spark.read.format("jdbc")  \
    .option("url", "jdbc:mysql://localhost:3306/finance").option("driver",
    "com.mysql.jdbc.Driver").option("dbtable", "latest_loans_5").option("lowerBound",
    "1000").option("upperBound", "5000").option("numPartitions", "5").option("partitionColumn",
    "serial_no").option("user", "hiveuser").option("password", "hivepassword").load()

    # Command to create a delta table

    events.write.format("delta").save("file:///home/ak/delta1/loans/")

    spark.sql("CREATE TABLE latest_finance USING DELTA LOCATION 'file:///home/ak/delta1/loans/'")

    # Dynamic partition done on Pyspark based on the region and then on the country

    events.write.format("delta").partitionBy("region","country").save("file:///home/ak/delta1_part_regionon_country/loans/")

    # Dynamic partition done on Pyspark based on year, month and date extracted from loan most recent closing date

    latest_date=events.withColumn("year",f.year("closed_date_most_recent")) \
    .withColumn("month",f.month("closed_date_most_recent")).withColumn("day",f.dayofmonth("closed_date_most_recent"))

    latest_date.write.partitionBy("year","month","day").mode("overwrite").parquet("file:///home/ak/delta_latest_timestamp_part/")

    df.write.partitionBy("region").bucketBy(10,"country").saveAsTable("latest_loans_bucket")

    spark.sql("select count(*) from latest_loans_bucket").show()


    # Using Merge Schema

    df = events.where("country='India'")

    df1 = df.withColumn("constant",lit(5))

    df2 = events.where("country='Denmark'")

    df2.write.format("delta").save("file:///home/ak/Documents/data_latest/")

    df1.write.format("delta").mode("append").option("mergeSchema","true").save("file:///home/ak/Documents/data_latest/")


    # Performing Delete on Delta Table

    # Reading data from file local as delta table format

    deltaTable = DeltaTable.forPath(spark, "file:///home/ak/delta1/loans/")

    # Deleting specific rows to understand deletion on delta tables

    deltaTable.delete(" serial_no = 10")

    deltaTable.delete("borrower= null")

    # Performing Update on Delta Table

    deltaTable.update("country=='India'",{"country":"'Bharat'"})

    



if __name__ == "__main__":
    main()

