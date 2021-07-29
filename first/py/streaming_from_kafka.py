from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("streaming_from_kafka").getOrCreate()

schema = "name string, age int"

df = (spark.readStream.format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("subscribe", "first_topic")
 .load())


query = (df.select("value").writeStream.format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "first_topic_output")
.outputMode("update")
.option("checkpointLocation", "../data/kafka-checkpoint")
 .start())

query.awaitTermination()