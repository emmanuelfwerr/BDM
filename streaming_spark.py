from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "venomoth.fib.upc.edu:9092") \
        .option("subscribe", "bdm_p2") \
        .load()

    def process(row):
        print(row)
    query = df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreach(process) \
        .start()
    # .format("console") \
    #print(df.select('value').map(lambda x: str(x)))
    query.awaitTermination()




