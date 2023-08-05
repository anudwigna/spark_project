from pyspark.sql.functions import desc, avg, hour, dayofweek, weekofyear, unix_timestamp

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
 df = spark.read.format(format).load(gcs_input_path)
 
 # Create a new column for trip duration in hours
 df = df.withColumn("trip_duration_hours", (unix_timestamp(df["tpep_dropoff_datetime"]) - unix_timestamp(df["tpep_pickup_datetime"])) / 3600)
 
 # Create a new column for average speed (distance/time)
 df = df.withColumn("average_speed", df["trip_distance"] / df["trip_duration_hours"])
 
 # Group the average speed by pickup and drop off location
 df_location_speed = df.groupBy("PULocationID", "DOLocationID").agg(avg("average_speed").alias("average_speed")).orderBy(desc("average_speed"))
 
 df_location_speed.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/4_location_speed_analysis")
 
 # Group the average speed by hour of the day
 df_hour_speed = df.groupBy(hour("tpep_pickup_datetime").alias("hour")).agg(avg("average_speed").alias("average_speed")).orderBy("hour")
 
 df_hour_speed.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/4_hour_speed_analysis")
 
 # Group the average speed by day of the week
 df_dayofweek_speed = df.groupBy(dayofweek("tpep_pickup_datetime").alias("dayofweek")).agg(avg("average_speed").alias("average_speed")).orderBy("dayofweek")
 
 df_dayofweek_speed.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/4_dayofweek_speed_analysis")
 
 # Group the average speed by week of the year
 df_weekofyear_speed = df.groupBy(weekofyear("tpep_pickup_datetime").alias("weekofyear")).agg(avg("average_speed").alias("average_speed")).orderBy("weekofyear")
 
 df_weekofyear_speed.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/4_weekofyear_speed_analysis")
