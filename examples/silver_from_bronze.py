from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BRONZE_PATH = "/opt/spark-apps/data/bronze_robot_telemetry"
SILVER_PATH = "/opt/spark-apps/data/silver_robot_telemetry"
CHECKPOINT_PATH = "/opt/spark-apps/checkpoints/silver_from_bronze"

spark = (
    SparkSession.builder.appName("lionsbot-silver-from-bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("battery_pct", DoubleType(), True),
    StructField("cumulative_area_m2", DoubleType(), True),
    StructField("mission_id", StringType(), True),
    StructField("event_time", StringType(), True),
])

silver_events = (
    spark.readStream.format("delta").load(BRONZE_PATH)
    .withColumn("event_data", from_json("raw_json", event_schema))
    .select(
        "robot_id", "raw_json", "kafka_partition", "kafka_offset", "kafka_ingested_at",
        col("event_data.event_id").alias("event_id"),
        col("event_data.event").alias("event_type"),
        col("event_data.battery_pct").alias("battery_pct"),
        col("event_data.cumulative_area_m2").alias("cumulative_area_m2"),
        col("event_data.mission_id").alias("mission_id"),
        to_timestamp("event_data.event_time").alias("event_time"),
    )
    .withWatermark("event_time", "20 minutes")
    .dropDuplicates(["event_id"])
)

(
    silver_events.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
    .awaitTermination()
)
