from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    (1, "Alice", 28, "New York"),
    (2, "Bob", 35, "Los Angeles"),
    (3, "Charlie", 25, "Chicago"),
    (4, "David", 40, "San Francisco"),
    (5, "Eva", 30, "Seattle")
]

bronze_path = "<target path>"

df = spark.createDataFrame(data, schema=schema)

(
  df
  .write
  .mode("overwrite")
  .format("delta")
  .option("path", bronze_path)
  .saveAsTable("aiq_dev.bronze.mock_data")
)
