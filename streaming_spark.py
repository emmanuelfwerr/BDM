from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import *


def streaming_prediction():
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
        .getOrCreate()

    pipeline = PipelineModel.load("exploitation/ModelRF")

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

streaming_prediction()
