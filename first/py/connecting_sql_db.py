from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("connecting_sql_db").getOrCreate()

df = (spark.read.format("jdbc")
.option("url", "jdbc:mysql://localhost:3306/emp")
.option("driver", "com.mysql.jdbc.Driver")
.option("dbtable", "employee")
.option("user", "root")
 .option("password", "mysql")
 .load()
)

df.show(10, False)

(df.write.format("jdbc")
.option("url", "jdbc:mysql://localhost:3306/emp")
.option("driver", "com.mysql.jdbc.Driver")
.option("dbtable", "employee_copy")
.option("user", "root")
 .option("password", "mysql")
 .save()
)