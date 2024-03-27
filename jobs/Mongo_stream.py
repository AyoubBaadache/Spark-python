import os
import time

from pyspark import SQLContext
from pyspark.sql import SparkSession

os.environ['HADOOP_HOME'] = 'C:/hadoop'

# Create SparkSession
spark = SparkSession.builder.appName("Mongo_APP") \
    .config("spark.mongodb.input.uri", "mongodb+srv://Ayoub:ABB2212023@spark-test.zdf0xk9.mongodb.net/sample_mflix"
                                       ".users") \
    .getOrCreate()


# Define a function to read data from MongoDB
def read_data_from_mongodb():
    return spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .load()


# Read data from MongoDB
df = read_data_from_mongodb()

# Cache the DataFrame for better performance
df.cache()
# Testing Sql queries
sqlc = SQLContext(spark)
df.createOrReplaceTempView("temp")

# Our function for processing logic
def process_data(df):

    df.show()
    # Register DataFrame as a temporary view
    D = sqlc.sql("Select email from temp")
    D.show()


# Define a function to periodically process the data
def process_data_periodically():
    while True:
        # Read new data from MongoDB
        new_data = read_data_from_mongodb()
        process_data(new_data)
        # Sleep for a specified interval before reading again
        time.sleep(10)  # Sleep for 10 seconds


# Start processing the data periodically
process_data_periodically()
