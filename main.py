from pyspark.sql.functions import hour, dayofweek, month, count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName('test_program').getOrCreate()
df = spark.read.format("parquet").load("data/NYC/*")

print(df.head())
    
# # Add time features to the dataframe and count the number of pickups per hour
# df = df.withColumn("hour", hour(df["tpep_pickup_datetime"]))
# df = df.withColumn("dayofweek", dayofweek(df["tpep_pickup_datetime"]))
# df = df.withColumn("month", month(df["tpep_pickup_datetime"]))
# df = df.groupBy("hour", "dayofweek", "month", "trip_distance", "PULocationID", "DOLocationID", "passenger_count").agg(count("*").alias("pickup_count"))

# # Use additional features for the model
# assembler = VectorAssembler(
#     inputCols=["hour", "dayofweek", "month", "trip_distance", "PULocationID", "DOLocationID", "passenger_count"],
#     outputCol="features")
# output = assembler.transform(df)

# # Split the data into training and test sets
# train_data, test_data = output.randomSplit([0.8, 0.2])

# # Define the model
# lr = LinearRegression(featuresCol='features', labelCol='pickup_count')

# # Fit the model
# lr_model = lr.fit(train_data)

# # Get predictions
# predictions = lr_model.transform(test_data)

# # Calculate residuals
# predictions = predictions.withColumn('residuals', predictions['pickup_count'] - predictions['prediction'])

# # Save the predictions, actual values, and residuals to a csv file
# predictions.select("prediction", "pickup_count", "residuals").write.mode('overwrite').csv(f"{gcs_output_path}/5_predictions.csv")

# # Print the coefficients and intercept for linear regression
# print("Coefficients: " + str(lr_model.coefficients))
# print("Intercept: " + str(lr_model.intercept))

# # Summarize the model over the training set and print out some metrics
# trainingSummary = lr_model.summary
# print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
# print("r2: %f" % trainingSummary.r2)

# # Save the model
# lr_model.write().overwrite().save(f"{gcs_output_path}/5_lr_model")