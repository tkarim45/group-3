from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("TrafficDataStreamingAnalysis")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)


schema = StructType(
    [
        StructField("sensor_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("vehicle_count", IntegerType()),
        StructField("average_speed", FloatType()),  # Updated
        StructField("congestion_level", StringType()),
    ]
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic_data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


traffic_df = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
)


traffic_df = traffic_df.filter(
    col("sensor_id").isNotNull() & col("timestamp").isNotNull()
)
traffic_df = traffic_df.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
traffic_df = traffic_df.dropDuplicates(["sensor_id", "timestamp"])

# 1.
volume_per_sensor = (
    traffic_df.withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes"), col("sensor_id")
    )  # Updated to 5 minutes
    .agg(sum("vehicle_count").alias("total_vehicle_count"))
    .select(
        col("window.start").alias("window_start"),
        col("sensor_id"),
        col("total_vehicle_count"),
    )
    .filter(col("total_vehicle_count") > 0)
)

# 2.
congestion_hotspots = (
    traffic_df.withWatermark("timestamp", "2 minutes")
    .filter(col("congestion_level") == "HIGH")
    .groupBy(window(col("timestamp"), "1 minutes"), col("sensor_id"))
    .agg(count("*").alias("high_congestion_count"))
    .filter(col("high_congestion_count") > 0)
)

# 3.
average_speed_per_sensor = (
    traffic_df.withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "10 minutes"), col("sensor_id")
    )  # Updated to 10 minutes
    .agg(avg("average_speed").alias("average_speed"))  # Updated
    .select(
        col("window.start").alias("window_start"),
        col("sensor_id"),
        col("average_speed"),
    )
)


# 4.
def detect_speed_drops(df):
    windowed_df = (
        df.withWatermark("timestamp", "2 minutes")
        .groupBy("sensor_id", window("timestamp", "2 minutes"))
        .agg(
            F.min("average_speed").alias("min_speed"),
            F.max("average_speed").alias("max_speed"),
        )
        .filter((F.col("max_speed") / F.col("min_speed")) < 0.5)
    )
    return windowed_df


sudden_speed_drops = detect_speed_drops(traffic_df)

# 5.
busiest_sensors = (
    traffic_df.withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "30 minutes"), col("sensor_id")
    )  # Updated to 30 minutes
    .agg(sum("vehicle_count").alias("total_vehicle_count"))
    .filter(col("total_vehicle_count") > 0)
)


def write_to_kafka(df, topic, query_name, output_mode="append"):
    return (
        df.select(to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .outputMode(output_mode)
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", topic)
        .option("checkpointLocation", f"/tmp/{query_name}_checkpoint")
        .start()
    )


volume_to_kafka = write_to_kafka(
    volume_per_sensor, "volume_data", "volume_query", "append"
)
congestion_to_kafka = write_to_kafka(
    congestion_hotspots, "congestion_data", "congestion_query", "append"
)
average_speed_to_kafka = write_to_kafka(
    average_speed_per_sensor, "speed_avg_data", "avg_speed_query", "append"
)
sudden_speed_drops_to_kafka = write_to_kafka(
    sudden_speed_drops, "speed_drops_data", "speed_drops_query", "append"
)
busiest_sensors_to_kafka = write_to_kafka(
    busiest_sensors, "busiest_sensors_data", "busiest_sensors_query", "append"
)


spark.streams.awaitAnyTermination()
