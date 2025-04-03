from pyspark.sql.functions import sum, avg

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

query_agg.awaitTermination()
