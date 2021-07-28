from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

spark = SparkSession.builder.appName("creating_udf").getOrCreate()

def cubed(x):
    return x*x*x

spark.udf.register("cubed", cubed, LongType())
df = spark.range(1,9).createOrReplaceTempView("range_df")
spark.sql("select id, cubed(id) from range_df").show(10, False)

