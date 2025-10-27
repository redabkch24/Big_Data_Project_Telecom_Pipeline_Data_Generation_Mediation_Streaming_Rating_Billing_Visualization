from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from db_config import jdbc_url, db_properties


spark = SparkSession.builder \
    .appName("BillingBatch") \
    .config("spark.driver.extraClassPath", "C:/libs/mssql-jdbc-10.2.1.jre17.jar") \
    .config("spark.executor.extraClassPath", "C:/libs/mssql-jdbc-10.2.1.jre17.jar") \
    .getOrCreate()

rating_df = spark.read.jdbc(url=jdbc_url, table="rating_cdrs", properties=db_properties)

billing_df = rating_df.withColumn("month", F.month(F.col("timestmp")))


billing_df = billing_df.groupBy("user_id", "month").agg(
    F.sum(F.when(F.col("record_type") == "sms", 1).otherwise(0)).alias("nb_sms"),
    F.sum(F.when(F.col("record_type") == "voice", F.col("duration_sec")).otherwise(0)).alias("total_duration_sec"),
    F.sum("cost").alias("total_amount")
)

columns_to_keep = ["user_id","month","nb_sms","total_duration_sec","total_amount"]
billing_df = billing_df.select(*columns_to_keep)

billing_df.write.jdbc(url=jdbc_url, table="billing_cdrs", mode="append", properties=db_properties)

print("Billing success !")

spark.stop()

