from pyspark.sql import SparkSession
import sys, os
from pyspark.sql.functions import col, concat, lit

if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print("Usage: readavro_createnewavro avrofile", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("readavro_createnewavro").getOrCreate()

    df = spark.read.format("avro").load(sys.argv[1])
    print(df.count())
    df = df.withColumn("newlocationId", df.locationId).drop("locationId")\
            .withColumnRenamed("newlocationId", "locationId")

    combineddf = df
    for x in range(1, 2):
        newdf = df.withColumn("newlocationId", concat(df.locationId, lit(x))).drop("locationId")\
            .withColumnRenamed("newlocationId", "locationId")
        combineddf = combineddf.union(newdf)

    print(combineddf.count())
    combineddf.repartition(1).write.format("avro").mode("overwrite").save("../data/new.avro")

