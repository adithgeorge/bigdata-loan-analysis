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
    spark = createSparkSession("local", "SparkApp")

    # Reading Data From Local

    latest_loans = spark.read.option("inferSchema", "True").option("header", "True").csv(
        "file:///home/ak/project_gladiator/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv")

    # Creating the user defined schema for the dataset

    loan_schema = StructType()\
        .add("end_of_period", StringType(), True)\
        .add("loan_number", StringType(), True)\
        .add("region", StringType(), True)\
        .add("country_code", StringType(), True)\
        .add("country", StringType(), True)\
        .add("borrower", StringType(), True)\
        .add("guarantor_country_code", StringType(), True)\
        .add("guarantor", StringType(), True)\
        .add("loan_type", StringType(), True)\
        .add("loan_status", StringType(), True)\
        .add("interest_rate", FloatType(), True)\
        .add("currency_of_commitment", StringType(), True)\
        .add("project_id", StringType(), True)\
        .add("project_name", StringType(), True)\
        .add("original_principal_amount", DoubleType(), True)\
        .add("cancelled_amount", DoubleType(), True)\
        .add("undisbursed_amount", DoubleType(), True)\
        .add("disbursed_amount", DoubleType(), True)\
        .add("repaid_to_ibrd", DoubleType(), True)\
        .add("due_to_ibrd", DoubleType(), True)\
        .add("exchange_adjustment", DoubleType(), True)\
        .add("borrowers_obligation", DoubleType(), True)\
        .add("sold_3rd_party", DoubleType(), True)\
        .add("repaid_3rd_party", DoubleType(), True)\
        .add("due_3rd_party", IntegerType(), True)\
        .add("loans_held", DoubleType(), True)\
        .add("first_repayment_date", StringType(), True)\
        .add("last_repayment_date", StringType(), True)\
        .add("agreement_signing_date", StringType(), True)\
        .add("board_approval_date", StringType(), True)\
        .add("effective_date_most_recent", StringType(), True)\
        .add("closed_date_most_recent", StringType(), True)\
        .add("last_disbursement_date", StringType(), True)

    df_latest = spark.createDataFrame(data=latest_loans, schema=loan_schema)

    df_latest = df_latest.withColumn("end_of_period", split(col("end_of_period"), " ").getItem(0))

    

    df_latest8 = df_latest \
    .select(col("country"), col("loan_status")) \
    .where(col("loan_status") == 'Fully Cancelled').groupBy("country") \
    .agg(count("loan_status").alias("count")) \
    .orderBy("count", ascending=False) \
    .withColumn("percentage_of_loans_cancelled", col("count")/sum("count").over(Window.partitionBy())*100).count()

    df_latest8.repartition(1).write.json("file:///home/ak/project_data/query_data/df_query8.json")


    df_latest6 = df_latest \
    .withColumn("days_for_signing",datediff("agreement_signing_date", "board_approval_date")) \
    .groupBy("loan_status", "loan_type") \
    .agg(max("days_for_signing").alias("max_days_signing"),avg("days_for_signing").alias("avg_days_signing"), min("days_for_signing").alias("min_days_signing"))

    df_latest6.repartition(1).write.csv("file:///home/ak/project_data/query_data/df_query6.csv")

    df_latest7 = df_latest \
    .withColumn("days_for_repayment",datediff("last_repayment_date", "first_repayment_date")) \
    .groupBy("loan_status", "loan_type") \
    .agg(max("days_for_repayment").alias("max_days_repayment"),avg("days_for_repayment").alias("avg_days_repayment"),min("days_for_repayment").alias("min_days_repayment"))


    df_latest7.repartition(1).write.csv("file:///home/ak/project_data/query_data/df_query7.csv")


    df_latest1 = df_latest \
    .select(col("country_code"), col("country"), col("region"), col("borrower"), col("guarantor"), col("guarantor_country_code"), col("loan_status"), col("loan_type")) \
    .distinct().orderBy("country")

    df_latest1.repartition(1).write.option("compression", "snappy") \
    .mode("overwrite") \
    .parquet("file: // /home/ak/project_data/query_data/df_query1.parquet")

    lst_country = ['India', 'China', 'Japan', 'World', 'Zimbabwe', 'Bolivia', 'Nepal']


    df_latest5 = df_latest \
    .where(col("country").isin(lst_country)) \
    .groupBy(col("country"), col("loan_status"), col("loan_type")) \
    .agg(round(avg("interest_rate"), 6).alias("avg_interest_rate"), \
    round(max("interest_rate"), 6).alias("max_interest_rate"), round(min("interest_rate"),6) \
    .alias("min_interest_rate")) \
    .orderBy("country")

    
    df_latest5.repartition(1).write.mode("overwrite") \
    .csv("file: // /home/ak/project_data/query_data/df_query5.csv")


    df_latest4 = df_latest \
    .select(col("original_principal_amount"), col("cancelled_amount"), col("loan_status"),col("loan_type"),col("country"),col("interest_rate"),col("project_name")) \
    .where(col("original_principal_amount") == 0).orderBy("loan_status").count()


    df_latest4.repartition(1).write.option("compression", "snappy") \
    .mode("overwrite").parquet("file:///home/ak/project_data/query_data/df_query4.parquet")


    df_latest10 = df_latest \
    .groupBy(col("region"), col("loan_status"), col("loan_type")) \
    .agg(round(avg("original_principal_amount"),30).alias("avg_original_principal_amount"), \
    round(max("original_principal_amount"),6).alias("max_original_principal_amount"), \
    round(min("original_principal_amount"),6).alias("min_original_principal_amount")).orderBy("region")

    df_latest10.repartition(1).write.json("file:///home/ak/project_data/query_data/df_query10.json")


    df_latest9 = df_latest.select(col("country"), col("loan_status")).groupBy("country") \
    .agg(count("loan_status").alias("total_loans"), \
    count(when(col("loan_status") == 'Fully Repaid', True)).alias("count_of_repaid_loans"),\
    count(when(col("loan_status") == 'Fully Cancelled', True)).alias("count_of_cancelled_loans")) \
    .withColumn("repayment_rate", (col("count_of_repaid_loans")/col("total_loans"))*100) \
    .withColumn("cancellation_rate", (col("count_of_cancelled_loans")/col("total_loans"))*100) \
    .select("country", "total_loans", "count_of_repaid_loans", "repayment_rate", "count_of_cancelled_loans","cancellation_rate") \
    .orderBy("total_loans", ascending=False)


    df_latest9.repartition(1).write.json("file:///home/ak/project_data/query_data/df_query9.json")




if __name__ == "__main__":
    main()
