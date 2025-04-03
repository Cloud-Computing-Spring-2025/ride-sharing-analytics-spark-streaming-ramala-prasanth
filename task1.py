from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

from pyspark.sql.functions import sum, avg
from pyspark.sql.functions import window, to_timestamp


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for the ride event
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Ingest streaming data from the socket
ride_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9998) \
    .load()

# Parse the incoming JSON messages
ride_df = ride_stream.select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_count = 0

# Define a function to stop the stream after 100 records
def stop_stream(query):
    global processed_count
    if processed_count >= 100:
        query.stop()


# Print the parsed data to the console
query = ride_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination(timeout=60)





# Aggregate data in real-time by driver_id
aggregated_df = ride_df.groupBy("driver_id") \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# Output the aggregation results to CSV
query_agg = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", "output/aggregated_data") \
    .option("checkpointLocation", "output/checkpoint_aggregated") \
    .start()

query_agg.awaitTermination(timeout=60)







# Convert the timestamp column to TimestampType
ride_df = ride_df.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute)
windowed_df = ride_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    "driver_id"
).agg(
    sum("fare_amount").alias("total_fare_in_window")
)

# Output the windowed results to CSV
query_windowed = windowed_df.writeStream \
    .outputMode("update") \
    .format("csv") \
    .option("path", "output/windowed_data") \
    .option("checkpointLocation", "output/checkpoint_windowed") \
    .start()

query_windowed.awaitTermination(timeout=60)
