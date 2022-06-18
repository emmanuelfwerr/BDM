from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.tree import RandomForestModel
from pyspark import SparkContext
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors, VectorUDT

from time import sleep
from IPython.display import clear_output

spark = SparkSession \
    .builder \
    .master(f"local[*]") \
    .appName("myApp") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
    .getOrCreate()

pipeline = PipelineModel.load("exploitation/ModelRF2")

df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', "venomoth.fib.upc.edu:9092") \
    .option('subscribe', 'bdm_p2') \
    .load() \
    .selectExpr("CAST(value AS STRING)")

parsed_message = df.withColumn('Neighborhood_ID', split(df['value'], ',').getItem(1)) \
    .withColumn('Price', split(df['value'], ',').getItem(2)) \
    .select('Neighborhood_ID', 'Price')
parsed_message = parsed_message.withColumn('Price', parsed_message['Price'].cast('double'))
predict = pipeline.transform(parsed_message).select("Neighborhood_ID", "Price", "prediction")


query = predict \
    .writeStream \
    .format("console") \
    .start()
query.awaitTermination()
