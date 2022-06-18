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

def process(row):
        value = row.value
        neigh_id = value.split(',')[1]
        cost = value.split(',')[2]
        print('neighID=' + neigh_id + ' and cost=' + cost)

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2') \
        .getOrCreate()

    pipeline = PipelineModel.load("exploitation/modelRF")

    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', "venomoth.fib.upc.edu:9092") \
        .option('subscribe', 'bdm_p2') \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    parsed_message = df.withColumn('neighborhood_id', split(df['value'], ',').getItem(1)) \
        .withColumn('price', split(df['value'], ',').getItem(2)) \
        .select('neighborhood_id', 'price')

    parsed_message = parsed_message.withColumn('price', parsed_message['price'].cast('double'))

    predict = pipeline.transform(parsed_message).select("neighborhood_id", "price", "prediction")

    query = predict \
        .writeStream \
        .format("console") \
        .start()
    
    '''
    query.awaitTermination()

    query = df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreach(process) \
        .start()
   
    query.awaitTermination(timeout=30)
    '''