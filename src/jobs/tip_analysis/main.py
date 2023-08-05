from pyspark.sql.functions import desc, avg, dayofyear, hour, year, month, dayofweek

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
 df = spark.read.format(format).load(gcs_input_path)
 
 # Add a new column for tip percentage
 df = df.withColumn("tip_percentage", (df["tip_amount"] / df["fare_amount"]) * 100)
 
 # Tip percentage by location
 df_location_tip = df.groupBy("PULocationID").agg(avg("tip_percentage").alias("average_tip_percentage")).orderBy(desc("average_tip_percentage"))
 
 df_location_tip.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/2_location_tip_analysis")

 # Tips by time (hour of day)
 df_hour_tip = df.groupBy(hour("tpep_pickup_datetime").alias("hour")).agg(avg("tip_percentage").alias("average_tip_percentage")).orderBy("hour")
 
 df_hour_tip.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/2_hour_tip_analysis")
 
 # Tips by day of week
 df_dayofweek_tip = df.groupBy(dayofweek("tpep_pickup_datetime").alias("dayofweek")).agg(avg("tip_percentage").alias("average_tip_percentage")).orderBy("dayofweek")
 
 df_dayofweek_tip.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/2_dayofweek_tip_analysis")

 # Tips by month of year
 df_month_tip = df.groupBy(month("tpep_pickup_datetime").alias("month")).agg(avg("tip_percentage").alias("average_tip_percentage")).orderBy("month")
 
 df_month_tip.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/2_month_tip_analysis")

 # Tip analysis by payment type
 df_payment_tip = df.groupBy("payment_type").agg(avg("tip_percentage").alias("average_tip_percentage")).orderBy(desc("average_tip_percentage"))
 
 df_payment_tip.repartition(1) \
 .write.format("csv") \
 .mode("overwrite") \
 .option("header", "true") \
 .save(f"{gcs_output_path}/2_payment_tip_analysis")
