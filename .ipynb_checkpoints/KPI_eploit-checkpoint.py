from pyspark.sql import SparkSession


def loadMongoRDD(spark):
    '''
    Download data from mongodb and store it in RDD format
    '''

    dataRDD = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/project3.new_data") \
        .load() \
        .rdd \
        .cache()

    return dataRDD


def mean(x, var):
    suma = 0
    num = len(x)
    for i in range(0,num):
        suma = suma + x[i][var]
    mean = suma/num

    return float("{:.2f}".format(mean))


def main():
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    rdd = loadMongoRDD(spark)

    # kpi 1: to know the average of price asked for a flat/apartment per neighborhood
    rdd1 = rdd.map(lambda x: (x['Neighborhood'], mean(x['Info Idealista'], 'price')))
    rdd1.foreach(lambda r: print(r))





if __name__ == '__main__':
    main()
