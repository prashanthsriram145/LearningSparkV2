from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col

if __name__ == "__main__":
    if(len(sys.argv) < 2):
        print("Usage: find_aggregates_mnm file.csv", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("find_aggregates_mnm").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(sys.argv[1])

    df.select("*").groupBy("State", "Color").sum("Count").orderBy("sum(Count)", ascending=False).show(10)

    df.select("*").where(col("State") == "CA").groupBy("State", "Color").sum("Count").orderBy("sum(Count)", ascending=False).show(10)

    df.select("*").groupBy("State", "Color").avg("Count").orderBy("avg(Count)").show(10)