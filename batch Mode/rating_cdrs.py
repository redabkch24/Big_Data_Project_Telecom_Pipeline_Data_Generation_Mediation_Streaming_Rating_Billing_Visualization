from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from db_config import jdbc_url, db_properties


# Initialisation Spark
spark = SparkSession.builder \
    .appName("RatingBatch") \
    .config("spark.driver.extraClassPath", "C:/libs/mssql-jdbc-10.2.1.jre17.jar") \
    .config("spark.executor.extraClassPath", "C:/libs/mssql-jdbc-10.2.1.jre17.jar") \
    .getOrCreate()

# Lire les données clean_cdrs
cdr_df = spark.read.jdbc(url=jdbc_url, table="clean_cdrs", properties=db_properties)
tarifs_df = spark.read.jdbc(url=jdbc_url, table="tarifs", properties=db_properties)


# Calcul du rating
rating_df = cdr_df.join(
    tarifs_df,
    on=["record_type", "technology"],
    how="left"
)


rating_df = rating_df.filter((F.col("sender_id").isNotNull()) | (F.col("caller_id").isNotNull())) # we can this filter in mediation part
rating_df = rating_df.withColumn(
    "user_id",
    F.coalesce(F.col("sender_id"), F.col("caller_id"))
) # we can also this line in mediation part


rating_df = rating_df.withColumn(
    "cost",
    col("price") * (col("duration_sec") / 60)  
)

rating_df = rating_df.withColumn(
    "cost",
    when(col("record_type") == "sms", col("price")).otherwise(col("cost"))
)

columns_to_keep = ["record_type","timestmp","caller_id","callee_id",
                   "sender_id","receiver_id","user_id","duration_sec","cell_id",
                   "technology","cost","tarif_id"]
rating_df = rating_df.select(*columns_to_keep)

# Sauvegarde dans MSSQL
rating_df.write.jdbc(url=jdbc_url, table="rating_cdrs", mode="append", properties=db_properties)

print("Rating success !")

spark.stop()
