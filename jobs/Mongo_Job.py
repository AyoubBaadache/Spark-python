import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

os.environ['HADOOP_HOME'] = 'C:/hadoop'
# .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
# Create SparkSession
spark = SparkSession.builder.appName("Mongo_APP") \
    .config("spark.mongodb.input.uri", "mongodb+srv://Ayoub:ABB2212023@spark-test.zdf0xk9.mongodb.net/sample_mflix"
                                       ".users") \
    .getOrCreate()
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
df.cache()
df.show()
# Create SQLContext
sqlc = SQLContext(spark)
# Register DataFrame as a temporary view
df.createOrReplaceTempView("temp")
D = sqlc.sql("Select email from temp")
D.show()
spark.stop()
