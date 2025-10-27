from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName("CDR_Mediation_Streaming").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

cdr_schema = StructType([
    StructField("record_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True)
])


df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-cdrs") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), cdr_schema).alias("cdr")) \
    .select("cdr.*")


valid_tech = ["2G", "3G", "4G", "5G"]

# Règles d'erreurs
invalid_df = df_parsed.filter(
    (col("record_type").isNull()) |
    (col("timestamp").isNull()) |
    (col("cell_id").isNull()) |
    (col("technology").isNull()) |
    (~col("technology").isin(valid_tech)) |
    ((col("record_type") == "voice") & (col("duration_sec") <= 0))
)

# Données valides (clean)
valid_df = df_parsed.filter(
    (col("record_type").isNotNull()) &
    (col("timestamp").isNotNull()) &
    (col("cell_id").isNotNull()) &
    (col("technology").isNotNull()) &
    (col("technology").isin(valid_tech)) &
    ~((col("record_type") == "voice") & (col("duration_sec") <= 0))
)

# ======================
# 5. Envoi des erreurs vers Kafka (topic cdrs_error)
# ======================
error_kafka = invalid_df.selectExpr("to_json(struct(*)) AS value")

error_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "error-cdrs") \
    .option("checkpointLocation", "/tmp/chk_cdrs_error") \
    .start()

# ======================
# 6. Envoi des données valides dans SQL Server
# ======================
valid_df_clean = valid_df.withColumnRenamed("timestamp", "timestmp")

valid_df_clean.writeStream \
    .foreachBatch(lambda batch_df, batch_id: (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:sqlserver://100.93.97.250:1433;databaseName=telecom_db;encrypt=false") 
        .option("dbtable", "clean_cdrs")
        .option("user", "wsl_user")
        .option("password", "0000")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode("append")
        .save()
    )) \
    .option("checkpointLocation", "/tmp/chk_cdrs_clean") \
    .start()


spark.streams.awaitAnyTermination()
