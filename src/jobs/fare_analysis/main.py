from pyspark.sql.functions import desc, avg

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
  df = spark.read.format(format).load(gcs_input_path)
  
  # Average fare by pickup and drop off location
  df_location_fare = df.groupBy("PULocationID", "DOLocationID").agg(avg("fare_amount").alias("average_fare")).orderBy(desc("average_fare"))
  
  df_location_fare.repartition(1) \
  .write.format("csv") \
  .mode("overwrite") \
  .option("header", "true") \
  .save(f"{gcs_output_path}/3_location_fare_analysis")
  
  # Average fare by passenger count
  df_passenger_fare = df.groupBy("passenger_count").agg(avg("fare_amount").alias("average_fare")).orderBy("passenger_count")
  
  df_passenger_fare.repartition(1) \
  .write.format("csv") \
  .mode("overwrite") \
  .option("header", "true") \
  .save(f"{gcs_output_path}/3_passenger_fare_analysis")

  # Correlation between trip distance and fare
  df_distance_fare_corr_value = df.stat.corr("trip_distance", "fare_amount")
  df_distance_fare_corr = spark.createDataFrame([(df_distance_fare_corr_value,)], ["correlation"])

  # Save the correlation result
  df_distance_fare_corr.repartition(1) \
  .write.format("csv") \
  .mode("overwrite") \
  .option("header", "true") \
  .save(f"{gcs_output_path}/3_distance_fare_correlation")
