from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# .schema(schema) \
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Stream JSON File") \
    .getOrCreate()

# Define the schema for parsing JSON data
# schema = StructType([
#     StructField("_id", StructType([
#         StructField("$oid", StringType(), nullable=True)
#     ]), nullable=True),
#     StructField("name", StringType(), nullable=True),
#     StructField("age", StringType(), nullable=True)
# ])
static_df = spark.read.json("/opt/bitnami/spark/jobs/AB.AB.json")
static_df.printSchema()
# schema = static_df.schema
schema = StructType() \
    .add("name", StringType()) \
    .add("age", IntegerType())

try:
    # Read JSON data as a streaming DataFrame
    streaming_df = spark.readStream \
        .schema(schema) \
        .format("json") \
        .option("maxFilesPerTrigger", 1) \
        .load("/opt/bitnami/spark/jobs/")

    # Apply from_json function to parse JSON data
    # parsed_df = streaming_df.select(from_json(streaming_df.name, schema).alias("data")).select("data.*")
    parsed_df = streaming_df.select("name", "age")

    # Define any further transformations or processing here

    # Start the streaming query
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='5 seconds') \
        .start()

    # Await termination of the query
    query.awaitTermination()

except Exception as e:
    print("An error occurred:", e)
finally:
    spark.stop()
