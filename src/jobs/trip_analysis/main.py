from pyspark.sql.functions import month, avg, unix_timestamp, dayofmonth, hour, dayofweek, desc

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
 df = spark.read.format(format).load(gcs_input_path)
 df_enriched = df.withColumn(
 "duration_time_in_minutes",
 (
 unix_timestamp(df["tpep_dropoff_datetime"])
 - unix_timestamp(df["tpep_pickup_datetime"])
 )
 / 60,
 )

# Analysis by time of day
 df_hour = df_enriched.groupBy(
 hour("tpep_pickup_datetime").alias("hour")).agg(
 avg("duration_time_in_minutes").alias("average_trip_time_in_minutes"),
 avg("trip_distance").alias("average_trip_distance")
 ).orderBy("hour", ascending=True)

 df_hour.repartition(1) \
 .write \
 .mode("overwrite") \
 .format("csv") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/1_hour_analysis")

# Analysis by day of week
 df_dayofweek = df_enriched.groupBy(
 dayofweek("tpep_pickup_datetime").alias("dayofweek")).agg(
 avg("duration_time_in_minutes").alias("average_trip_time_in_minutes"),
 avg("trip_distance").alias("average_trip_distance")
 ).orderBy("dayofweek", ascending=True)

 df_dayofweek.repartition(1) \
 .write \
 .mode("overwrite") \
 .format("csv") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/1_dayofweek_analysis")

# Analysis by month of year
 df_month = df_enriched.groupBy(
 month("tpep_pickup_datetime").alias("month")).agg(
 avg("duration_time_in_minutes").alias("average_trip_time_in_minutes"),
 avg("trip_distance").alias("average_trip_distance")
 ).orderBy("month", ascending=True)

 df_month.repartition(1) \
 .write \
 .mode("overwrite") \
 .format("csv") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/1_month_analysis")

 # Identify the top 10 pickup locations
 df_pickup = df.groupBy("PULocationID").count().orderBy(desc("count")).limit(10)
 
 df_pickup.repartition(1) \
 .write \
 .mode("overwrite") \
 .format("csv") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/1_top_pickup_locations")

 # Identify the top 10 dropoff locations
 df_dropoff = df.groupBy("DOLocationID").count().orderBy(desc("count")).limit(10)
 
 df_dropoff.repartition(1) \
 .write \
 .mode("overwrite") \
 .format("csv") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/1_top_dropoff_locations")
