from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import col

if __name__ == "__main__":

    spark = SparkSession.builder.appName("using_schema").getOrCreate()

    schema = "Id INT, First string, Last string, url string, published string, hits INT, campaigns array<string>"

    data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "linkedIn"]],
            [2, "Nathan", "Ratan", "https://tinyurl.2", "2/5/2017", 3434, ["facebook", "twitter"]],
            [3, "James", "Hoggin", "https://tinyurl.3", "1/5/2018", 2342, ["facebook", "linkedin"]]]

    df = spark.createDataFrame(data, schema)

    df.printSchema()
    df.select(col("First")).show(10, False)

    rows = [Row("Prashanth", 40), Row("Sriram", 41)]
    # rows_df = spark.createDataFrame(rows, ["name", "age"])
    rows_schema = "name STRING, age INT"
    rows_df = spark.createDataFrame(rows, rows_schema)
    rows_df.printSchema()
    rows_df.select("*").show(2, False)

    rows_df.foreach(lambda row: print(row[1]))

