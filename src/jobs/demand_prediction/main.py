from pyspark.sql.functions import hour, dayofweek, month, count
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import StructType, StructField, DoubleType

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    # Data cleaning
    df = df.dropna(how="any")
    df = df.filter((df["fare_amount"] > 0) & (df["passenger_count"] > 0))

    # Add time features to the dataframe and count the number of pickups per hour
    df = df.withColumn("hour", hour(df["tpep_pickup_datetime"]))
    df = df.withColumn("dayofweek", dayofweek(df["tpep_pickup_datetime"]))
    df = df.withColumn("month", month(df["tpep_pickup_datetime"]))
    df = df.groupBy("hour", "dayofweek", "month", "trip_distance", "PULocationID", "DOLocationID", "passenger_count").agg(count("*").alias("pickup_count"))

    # Use additional features for the model
    assembler = VectorAssembler(
        inputCols=["hour", "dayofweek", "month", "trip_distance", "PULocationID", "DOLocationID", "passenger_count"],
        outputCol="features")
    output = assembler.transform(df)

    # Split the data into training and test sets
    train_data, test_data = output.randomSplit([0.8, 0.2])

    # Define the model with non-zero regParam
    lr = LinearRegression(featuresCol='features', labelCol='pickup_count', regParam=0.1)

    # Fit the model
    lr_model = lr.fit(train_data)

    # Get predictions
    predictions = lr_model.transform(test_data)

    # Evaluate the model and get metrics
    evaluator = RegressionEvaluator(labelCol="pickup_count", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})

    # Create a dataframe with the metrics
    schema = StructType([
        StructField("rmse", DoubleType(), True),
        StructField("r2", DoubleType(), True),
        StructField("mae", DoubleType(), True),
        StructField("mse", DoubleType(), True)
    ])
    metrics_df = spark.createDataFrame([(rmse, r2, mae, mse)], schema=schema)

    # Save the metrics dataframe to a csv file
    metrics_df.write.mode('overwrite').option("header", "true").csv(f"{gcs_output_path}/5_metrics.csv")

    # Save the model
    lr_model.write().overwrite().save(f"{gcs_output_path}/5_lr_model")
