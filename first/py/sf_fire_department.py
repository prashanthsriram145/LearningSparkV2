from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, countDistinct, to_timestamp, year

spark = SparkSession.builder.appName("sf_fire_department").getOrCreate()

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                StructField('UnitID', StringType(), True),
                StructField('IncidentNumber', IntegerType(), True),
                StructField('CallType', StringType(), True),
                StructField('CallDate', StringType(), True),
                StructField('WatchDate', StringType(), True),
                StructField('CallFinalDisposition', StringType(), True),
                StructField('AvailableDtTm', StringType(), True),
                StructField('Address', StringType(), True),
                StructField('City', StringType(), True),
                StructField('Zipcode', IntegerType(), True),
                StructField('Battalion', StringType(), True),
                StructField('StationArea', StringType(), True),
                StructField('Box', StringType(), True),
                StructField('OriginalPriority', StringType(), True),
                StructField('Priority', StringType(), True),
                StructField('FinalPriority', IntegerType(), True),
                StructField('ALSUnit', BooleanType(), True),
                StructField('CallTypeGroup', StringType(), True),
                StructField('NumAlarms', IntegerType(), True),
                StructField('UnitType', StringType(), True),
                StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                StructField('FirePreventionDistrict', StringType(), True),
                StructField('SupervisorDistrict', StringType(), True),
                StructField('Neighborhood', StringType(), True),
                StructField('Location', StringType(), True),
                StructField('RowID', StringType(), True),
                StructField('Delay', FloatType(), True)])

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "../../databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
# fire_df.select("UnitID", "IncidentNumber").show(10, False)
# fire_df.repartition(1).write.format("parquet").mode("overwrite").save("../data/parquet-sf-fire-dept")
#
# fire_df.write.format("parquet").saveAsTable("sf_fire_dept")
#
# spark.sql("select * from sf_fire_dept limit 10").show(10, False)

# fire_df.select("UnitID", "IncidentNumber", "CallType").where(col("CallType") == "Medical Incident").show(5, False)

# (fire_df.select("CallType", "UnitID", "IncidentNumber").where(col("CallType").isNotNull())
# .agg(countDistinct("CallType").alias("distinct_calltype")).show())
#
# (fire_df.select("CallType", "UnitID", "IncidentNumber").where(col("CallType").isNotNull()).distinct().show(100, False))

new_fire_df = (fire_df.withColumn("IncidentDate", to_timestamp("CallDate", "MM/dd/yyyy"))
               .drop("CallDate"))

# new_fire_df.show(10, False)
# (new_fire_df.select(year("IncidentDate"))
#  .distinct().orderBy(year("IncidentDate"))
#  .show())

(fire_df.select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(10, False))