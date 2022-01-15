from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


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
    spark = createSparkSession("local","SparkApp")

    # Loading Data to Pyspark using JDBC Connection:

    finance_data = spark.read \
    .format("jdbc").option("url","jdbc:mysql://localhost:3306/finance") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "latest_loans") \
    .option("lowerBound", "1000") \
    .option("upperBound", "5000") \
    .option("numPartitions", "5") \
    .option("partitionColumn", "serial_no") \
    .option("user", "hiveuser") \
    .option("password", "hivepassword").load()


    # Filter Operations

    filtered_finance_data = finance_data.filter(finance_data["country"] == "India")
    filtered_finance_data.count()
    filtered_finance_data_D= finance_data.filter(finance_data["country"] == "Denmark")
    
    df_latest1.select(col("due_to_ibrd")).where(col("due_to_ibrd") < 0).show()

    df_latest1.select(col("country"),col("loan_status")) \
    .where(col("loan_status") == 'FullyCancelled') \
    .groupBy("country").count().orderBy("count",ascending=False).show(5,False)


    # Creating New Columns & Renaming Operations

    filtered_finance_data = filtered_finance_data.withColumnRenamed('country','Country')
    filtered_finance_data.show(5)

    df = filtered_finance_data.withColumn("end_of_period_date" ,to_date(filtered_finance_data.end_of_period))
    
    df_latest.withColumn("end_of_period",to_date("end_of_period",'MM/dd/yyyy'))

    df_latest_date = latest_loans.withColumn("end_of_period_date",to_date(col("end_of_period")))

    df_latest_date = df_latest_date.withColumn("end_of_period_year",year(col("end_of_period_date"))) \
    .withColumn("end_of_period_month",month(col("end_of_period_date"))) \
    .withColumn("end_of_period_day",dayofmonth(col("end_of_period_date")))

    # Aggregation Operations:

    latest_data.agg({'loans_held':'avg'}).show()
    latest_data.agg({'loans_held':'min'}).show()
    latest_data.agg({'loans_held':'max'}).show()

    df_latest1.select(col("original_principal_amount"),col("cancelled_amount"),col("loan_status"),col("country"),col("interest_rate")) \
    .where(col("original_principal_amount") == 0).groupBy("loan_status").count().show()

    # Ranking Operations:

    window = Window.partitionBy(latest_data['country']).orderBy(latest_data['loans_held'].desc())

    latest_data.select('country','loans_held', rank().over(window).alias('rank')).filter(col('rank') <= 20).show(10)


        
    # Reading Data From Local

    latest_loans = spark.read.option("inferSchema","True").option("header","True").csv("file:///home/ak/project_gladiator/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv")
    
    
    # Creating the user defined schema for the dataset

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


    df_latest = spark.createDataFrame(data = latest_loans, schema = loan_schema)

    df_latest = df_latest.withColumn("end_of_period", split(col("end_of_period")," ").getItem(0))
    
    df_latest.withColumn("end_of_period",to_date("end_of_period",'MM/dd/yyyy'))


    # Dropping Duplicates & Checking for Null

    df_latest_date.dropDuplicates()

    df_latest.where(col("end_of_period").isNull()).select(col("end_of_period")).count()

    df_latest.select(col("end_of_period")).distinct().show()


    # Further Exploration & Processing on the Data

    df_latest.where(col("loan_number").like("IBRD%")).select(max(col("loan_number"))).show()

    df_latest.where(col("loan_number").like("IBRD%")).select(min(col("loan_number"))).show()

    df_latest.select(col("country_code"),col("country"),col("guarantor"),col("guarantor_country_code")).distinct().orderBy("country").show(50)

    df_latest1 = df_latest \
    .withColumn("original_principal_amount",col("original_principal_amount").cast(DecimalType(20,3)))\
    .withColumn("cancelled_amount",col("cancelled_amount").cast(DecimalType(20,3)))\
    .withColumn("undisbursed_amount",col("undisbursed_amount").cast(DecimalType(20,3)))\
    .withColumn("disbursed_amount",col("disbursed_amount").cast(DecimalType(20,3)))\
    .withColumn("repaid_to_ibrd",col("repaid_to_ibrd").cast(DecimalType(20,3)))\
    .withColumn("due_to_ibrd",col("due_to_ibrd").cast(DecimalType(20,3)))\
    .withColumn("exchange_adjustment",col("exchange_adjustment").cast(DecimalType(20,3)))\
    .withColumn("borrowers_obligation",col("borrowers_obligation").cast(DecimalType(20,3)))\
    .withColumn("sold_3rd_party",col("sold_3rd_party").cast(DecimalType(20,3)))\
    .withColumn("repaid_3rd_party",col("repaid_3rd_party").cast(DecimalType(20,3)))\
    .withColumn("due_3rd_party",col("due_3rd_party").cast(DecimalType(20,3)))\
    .withColumn("loans_held",col("loans_held").cast(DecimalType(20,3)))

    # Writing the Data

    df_loan_relations.write.option("compression","snappy").parquet("file:///home/ak/project_data/df_loans_relations.parquet")

    df_latest.repartition(1).write.json("file:///home/ak/project_data/query_data/df_query1.json")




if __name__ == "__main__":
    main()