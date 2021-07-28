from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

tripdelaysFilePath = "../../databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "../../databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

spark = SparkSession.builder.appName("unions_joins").getOrCreate()

# Obtain airports data set
airportsna = (spark.read
              .format("csv")
              .options(header="true", inferSchema="true", sep="\t")
              .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
                   .format("csv")
                   .options(header="true")
                   .load(tripdelaysFilePath))

departureDelays = (departureDelays
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))
                   .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = (departureDelays
       .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
    date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")
foo.show(10, False)

bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

foo.join(
  airportsna,
  airportsna.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()
