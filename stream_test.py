from pyspark.sql import SparkSession

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

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "venomoth.fib.upc.edu:9092") \
        .option("subscribe", "bdm_p2") \
        .load()

    query = df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreach(process) \
        .start()
   
    query.awaitTermination(timeout=30)