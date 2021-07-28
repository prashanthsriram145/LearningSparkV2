from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("using_flight_delays").getOrCreate()

df = (spark.read.format("csv")
 .option("inferschema", "true")
.option("header", "true")
 .load("../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv"))

# df.select("date", "delay", "distance", "origin", "destination").show(10, False)
#
# (df.select("*").where(col("distance") > 1000)
# .orderBy("distance", ascending=False)
#  .show())

df.registerTempTable("flight_delays")

# spark.sql("""
# select * from flight_delays where distance > 1000 order by distance desc
# """).show(10, False)

# spark.sql("""
# select * from flight_delays where origin='SFO' and destination='ORD' and delay > 120 order by delay desc
# """).show(10)

# spark.sql("""
# select *, case
#  when delay > 360 then 'very long delay'
#  when delay <= 360 and delay >120 then 'long delay'
#  when delay <=120 and delay > 30 then 'short delay'
#  when delay == 0 then 'no delay'
#  else 'Early' end as flight_delay
#  from flight_delays
#  order by origin, delay desc
# """).show(10, False)

print(spark.catalog.listTables())

df_copy = spark.table("flight_delays")
df_copy.show(10, False)