import os
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

os.environ['HADOOP_HOME'] = 'C:/hadoop'
######### Reading from json file #########
#this
# #read json from text file
#dfFromTxt=spark.read.json("C:\\Users\me\PycharmProjects\Spark-python\AB.AB.json")
#dfFromTxt=spark.read.format('json').load("C:\\Users\me\PycharmProjects\Spark-python\AB.AB.json")
spark = SparkSession.builder.appName('SparkByExamples.com').master('spark://spark-master:7077')\
    .getOrCreate()
dfFromTxt=spark.read.option("multiline", "true").json("C:\\Users\me\PycharmProjects\Spark-python\AB.AB.json")
dfFromTxt.printSchema()
dfFromTxt.cache()
dfFromTxt.show()
###################################################

##############Reading from mongo DB collection ###################



# conf =  pyspark.SparkConf().set("spark.jars.packages",
#                                    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#     .setMaster("local")\
#     .setAppName("mongo")\
#     .setAll([("spark.driver.memory","40g"),("spark.executor.memory","50g")])
#
# sc=SparkContext(conf=conf)
# sqlc = SQLContext(sc)
# mongo_ip="mongodb+srv://Ayoub:ABB2212023@spark-test.zdf0xk9.mongodb.net/"
# Data = sqlc.read.format("com.mongodb.spark.DefaultSource").option("uri",mongo_ip).load()



# # Configure SparkSession

# os.environ['HADOOP_HOME'] = 'C:/hadoop'
# spark = SparkSession.builder \
#     .appName("mongo") \
#     .master("local[*]") \
#     .config("spark.driver.memory", "40g") \
#     .config("spark.executor.memory", "50g") \
#     .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#     .getOrCreate()
#
# # Read data from MongoDB
# mongo_ip = "mongodb+srv://Ayoub:ABB2212023@spark-test.zdf0xk9.mongodb.net/sample_mflix.users"
# Data = spark.read.format("com.mongodb.spark.DefaultSource").option("uri", mongo_ip).load()
# Data.createOrReplaceTempView("users")
# D = sqlC.sql("Select * from users")


# .config("spark.driver.extraJavaOptions", "-DHADOOP_HOME=C:\\hadoop") \
# # .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:\\hadoop") \


#################################
# Create SparkSession
# spark = SparkSession.builder.appName("app")\
#     .master("spark-master://spark-master:7077").config("spark.executor.memory", "1g")\
#     .config("spark.mongodb.input.uri","mongodb+srv://Ayoub:ABB2212023@spark-test.zdf0xk9.mongodb.net/AB.AB")\
#     .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#     .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
#     .config("spark.network.maxRetries", "10")\
#     .config("spark.network.timeout", "600s")\
#     .getOrCreate()
# df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# df.printSchema()
# df.cache()
# df.show()
# # Create SQLContext
# sqlc = SQLContext(spark)
# # Register DataFrame as a temporary view
# df.createOrReplaceTempView("temp")
# D = sqlc.sql("Select email from temp")
# D.show()
