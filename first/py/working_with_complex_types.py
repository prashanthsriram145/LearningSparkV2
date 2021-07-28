from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType

spark = SparkSession.builder.appName("working_with_complex_types").getOrCreate()

# spark.sql("SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'))").show()
#
# spark.sql("SELECT element_at(map(1, 'a', 2, 'b'), 2)").show()
#
# spark.sql("""
# SELECT array_distinct(array(1, 2, 3, null, 3))
# """).show()
#
# spark.sql("SELECT array_join(array('hello', 'world'), ' ')").show()

schema = StructType([StructField("Celsius", ArrayType(IntegerType()))])

t_list = [[35,36,32,30,40,42,38]], [[31,32,34,55,56]]

t_c = spark.createDataFrame(t_list, schema)

t_c.createOrReplaceTempView("tc")

# t_c.show(10, False)

spark.sql("""
select Celsius, transform(Celsius, t -> ((t * 9) div 5) +32) as farhanheit from tc
""").show(10, False)

spark.sql("""
select Celsius, filter(Celsius, t -> t > 35) as high from tc
""").show(10, False)

spark.sql("""
select Celsius, exists(Celsius, t -> t == 38) as exists from tc
""").show(10, False)

# spark.sql("""
# select Celsius, reduce(Celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(Celsius) * 9 div 5) + 32)
# as avg_farhenheit from tc
# """).show(10, False)