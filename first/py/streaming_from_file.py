from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("streaming_from_file").getOrCreate()

schema = "DEST_COUNTRY_NAME string,ORIGIN_COUNTRY_NAME string,count string"

df = (spark.readStream.format("csv")
 .option("header", "true")
 .schema(schema)
 .load("../../databricks-datasets/learning-spark-v2/flights/summary-data/csv/*.csv"))

query = (df.writeStream.format("json").option("path", "../data/streaming-data")
 .option("checkpointLocation", "../data/checkpoint")
 .trigger(processingTime="1 second")
 .outputMode("append")
 .start())

query.awaitTermination()