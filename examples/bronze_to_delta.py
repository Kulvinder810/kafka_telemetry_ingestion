from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_PATH = "/opt/spark-apps/data/bronze_robot_telemetry"
CHECKPOINT_PATH = "/opt/spark-apps/checkpoints/bronze_kafka_ingestion"

spark = (
    SparkSession.builder.appName("lionsbot-bronze-kafka-ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "robot_telemetry")
    .option("startingOffsets", "earliest")
    .load()
    .select(
        col("key").cast("string").alias("robot_id"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_ingested_at"),
    )
)

(
    bronze_events.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(BRONZE_PATH)
    .awaitTermination()
)
