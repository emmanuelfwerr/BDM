from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import *


def streaming_prediction(val):
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

    data = df.withColumn('Neighborhood_ID', split(df['value'], ',').getItem(1)) \
        .withColumn('Price', split(df['value'], ',').getItem(2)) \
        .select('Neighborhood_ID', 'Price')
    data = data.withColumn('Price', data['Price'].cast('double'))
    prediction = pipeline.transform(data).select("Neighborhood_ID", "Price", "prediction")


    query = prediction \
        .writeStream \
        .format("console") \
        .start()
    query.awaitTermination(timeout=val)

val = int(input("Enter the number of seconds that you want to be receiving values by stream and displaying the prediction, e.g. 30: "))
streaming_prediction(val)
