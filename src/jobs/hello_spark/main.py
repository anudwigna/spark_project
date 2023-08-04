from utils import shared_function

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    shared_function()
    df = spark.read.format(format).load(gcs_input_path)
    df.printSchema()
    
    # Save DataFrame in parquet format
    df.write.format('parquet').mode('overwrite') \
     .save(f"{gcs_output_path}/0_df_sample")
    
    spark.stop()
