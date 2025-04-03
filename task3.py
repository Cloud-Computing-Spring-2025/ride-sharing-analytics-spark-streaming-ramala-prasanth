from pyspark.sql.functions import window, to_timestamp

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

query_windowed.awaitTermination()
