import os
from pyspark.sql import SparkSession

os.environ['HADOOP_HOME'] = 'C:/hadoop'

spark = SparkSession.builder.appName('Spark.com') \
    .master('spark://spark-master:7077') \
    .getOrCreate()
dfFromTxt = spark.read.option("multiline", "true").json("/opt/bitnami/spark/jobs/AB.AB.json")
dfFromTxt.printSchema()
dfFromTxt.cache()
dfFromTxt.show()
