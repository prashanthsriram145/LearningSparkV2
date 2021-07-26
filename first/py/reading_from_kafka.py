from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, from_json

if __name__ == "__main__":

    spark = SparkSession.builder.appName("reading_from_kafka").getOrCreate()

    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "input-to-spark")
          .option("startingOffsets", "earliest")
          .load())

    schema = StructType([StructField("name", StringType(), False), StructField("age", IntegerType(), False)])

    df.printSchema()
    # print(df.select("value").select("value").take(2))
    df.select(col("value").cast("string"), from_json(col("value").cast("string"), schema)).show(10, False)