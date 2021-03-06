import os
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, DoubleType, IntegerType


def loadMongoRDD(collection: str, spark):
    '''
    Download data from mongodb and store it in RDD format
    '''

    dataRDD = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/opendata.{collection}") \
        .load() \
        .rdd \
        .cache()

    return dataRDD


def uploadRDDtoMongo(df, spark):
    '''
    Upload the final transformed data to mongodb
    '''

    struct_schema = ArrayType(StructType([
        StructField("date", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("rooms", IntegerType()),
        StructField("bedrooms", IntegerType())
    ]))

    df = df.select(col("Neighborhood"),
                   col("District"),
                   col("RFD most recent"),
                   col("RFD Increased"),
                   col("Population recent"),
                   col("Population Increase"),
                   col("Surface Price (€/m2)"),
                   col("Monthly Price (€/month)"),
                   col("Surface Price Increase"),
                   col("Monthly Price Increase"),
                   col("Info Idealista")
                   .cast(struct_schema))

    df.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .option('uri', f"mongodb://10.4.41.48/project3.new_data") \
        .save()


def mostrecent(x, var):
    '''
    we return the var most recent
    '''
    x.sort(reverse=True, key=lambda x: x['year'])
    return x[0][var]


def increase(x, var):
    '''
    it returns the increase difference of the var between the most recent one and the second most previous one
    '''
    x.sort(reverse=True, key=lambda x: x['year'])
    diff = x[0][var] - x[1][var] #difference between the var of the last year and the year before
    return float("{:.2f}".format(diff))


def unroll(x: Tuple[str, Tuple[float, str]]):
    '''
    it returns the reconciliate name from the lookup table as the key of the rdd
    '''
    (_, (price, ne_re)) = x
    return (ne_re, price)


def maxdate(a, b):
    '''
    It returns the rows with the gratest value in the x[1][1] field.
    In this case is used to keep only those repeated properties from idealista that appears in the most recent list.
    '''
    if a[1][1] > b[1][1]:
        return a
    else:
        return b


def generateIncomeRDD(incomeRDD, lookup_income_neighborhood_RDD):
    """
    RDD generated has the following structure:
    - Key: neighborhood
    - Values: last year of RFD (family income index), last year of population, increase of RFD, increase od population
    """
    rdd = incomeRDD \
        .map(lambda x: (x['neigh_name '], (x['district_name'], float(mostrecent(x['info'], 'RFD')), increase(x['info'], 'RFD'), float(mostrecent(x['info'], 'pop')), increase(x['info'], 'pop')))) \
        .join(lookup_income_neighborhood_RDD) \
        .distinct() \
        .map(unroll) \
        .cache()
    return rdd


def generatePreuRDD(preuRDD, lookup_income_neighborhood_RDD):
    '''
    RDD generated has the following structure:
    - Key: neighborhood
    - Values: district, surface price of the most recent year, monthly price of the most recent year, increase surface price, increase monthly price
    '''

    # we remove the missing values and keep only the data from the last two years
    preuRDD = preuRDD \
        .filter(lambda x: x['Preu'] != '--') \
        .filter(lambda x: x['Preu'] != None) \
        .filter(lambda x: 2020 <= x['Any']) \
        .cache()

    #here we create a RDD filtering by surface and do the mean of all the price per year
    rdd2_sup = preuRDD \
        .filter(lambda x: 'superfície' in x['Lloguer_mitja']) \
        .map(lambda x: ((x['Nom_Barri'], x['Any'], x['Nom_Districte']), float(x['Preu']))) \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
        .mapValues(lambda x: float("{:.2f}".format(x[0]/x[1])) ) \
        .distinct() \
        .cache()

    #here we create a RDD filtering by surface filtering by monthly price and do the mean of all the price per year
    rdd2_men = preuRDD \
        .filter(lambda x: 'mensual' in x['Lloguer_mitja']) \
        .map(lambda x: ((x['Nom_Barri'], x['Any'], x['Nom_Districte']), float(x['Preu']))) \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
        .mapValues(lambda x: float("{:.2f}".format(x[0]/x[1])) ) \
        .distinct() \
        .cache()

    #join them
    rdd2_join = rdd2_sup.join(rdd2_men) \
        .cache()

    #do the mapping of the interesting values
    rdd2_all = rdd2_join \
        .map(lambda x: (x[0][0], (x[0][2], x[0][1], x[1][0], x[1][1]))) \
        .cache()

    # we do a join of the values in order to have the last two years in the same row
    rdd2_join2 = rdd2_all \
        .filter(lambda x: 2021 == x[1][1]) \
        .join(rdd2_all.filter(lambda x: 2020 == x[1][1])) \
        .cache()

    # generating the last rdd
    rdd = rdd2_join2 \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][2], x[1][0][3], float("{:.2f}".format(x[1][0][2]-x[1][1][2])), float("{:.2f}".format(x[1][0][3]-x[1][1][3]))))) \
        .join(lookup_income_neighborhood_RDD) \
        .map(unroll) \
        .cache()

    return rdd


def remove_duplicate_properties_idealista(rdd_in):
    '''
    It removes duplicated properties, and keep the most recent one
    '''

    rdd_out = rdd_in \
        .map(lambda x: (x[1][0], (x[0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5], x[1][6]))) \
        .reduceByKey(maxdate) \
        .map(lambda x: (x[1][0], (x[1][1], x[1][3], x[1][4], x[1][5], x[1][6]))) \
        .cache()

    return rdd_out


def transform_idealista(rdd_in, lookup_rent_neighborhood_RDD):
    '''
    It transforms the idealista RDD to keep only the information from Barcelona, and selecting the interesting fields
    in the mapping. Also the neighborhood value is changed by the reconciled value from the lookup rent data
    '''

    transform_rdd = rdd_in \
        .filter (lambda x: x['province'] == 'Barcelona') \
        .map(lambda x: (x['neighborhood'], (x['propertyCode'], x['date'], x['district'], x['price'], x['size'], x['rooms'], x['bathrooms']))) \
        .join(lookup_rent_neighborhood_RDD) \
        .map(unroll) \
        .cache()

    return transform_rdd


def generateIdealistaRDD(directory, lookup_rent_neighborhood_RDD, spark):

    parq_files = {}  # List which will store all of the full filepaths.
    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            if filename[-7:] == 'parquet':
                parq_files[root[29:39]] = (root+'/'+filename)


    i = 0 # special loop counter
    for key in parq_files:
        # read spark df from parquet file
        df = spark.read.parquet(parq_files[key])
        if 'neighborhood' not in df.columns:
            continue
        rdd_addDate = df \
            .withColumn("date", lit(key)) \
            .filter(col('neighborhood').isNotNull()) \
            .rdd # add 'date' attribute, filter by Barcelona and transform into rdd
        transform_rdd = transform_idealista(rdd_addDate, lookup_rent_neighborhood_RDD) # remove duplicates and select attributes
        if i == 0:
            union_idealista_rdd = transform_rdd
        else:
            union_idealista_rdd = union_idealista_rdd.union(transform_rdd)
        #print(transform_rdd.count())
        i += 1

    rdd_idealista_clean = remove_duplicate_properties_idealista(union_idealista_rdd)
    rdd_idealista_1 = rdd_idealista_clean.groupByKey().map(lambda x : (x[0], list(x[1])))

    return rdd_idealista_1



def main():

    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    collections = ['income', 'preu', 'income_lookup_neighborhood', 'rent_lookup_neighborhood']

    incomeRDD = loadMongoRDD(collections[0], spark).cache()
    preuRDD = loadMongoRDD(collections[1], spark).cache()
    lookup_income_neighborhood_RDD = loadMongoRDD(collections[2], spark).map(lambda x: (x['neighborhood'], x['neighborhood_reconciled'])).cache()
    lookup_rent_neighborhood_RDD = loadMongoRDD(collections[3], spark).map(lambda x: (x['ne'], x['ne_re'])).cache()

    rdd1 = generateIncomeRDD(incomeRDD, lookup_income_neighborhood_RDD)

    rdd2 = generatePreuRDD(preuRDD, lookup_income_neighborhood_RDD)

    rdd3 = rdd1 \
        .join(rdd2) \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4]))) \
        .cache()

    directory = "landing/persistent/idealista"
    idealista_rdd = generateIdealistaRDD(directory, lookup_rent_neighborhood_RDD, spark)
    rdd_all = idealista_rdd.join(rdd3)


    # print('####################')
    # print('****** RDD ******')
    # rdd_all.foreach(lambda r: print(r))
    # print(rdd_all.count())

    df = rdd_all \
         .map(lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3], x[1][1][4], x[1][1][5], x[1][1][6], x[1][1][7], x[1][1][8], x[1][0])) \
        .toDF(['Neighborhood', 'District', 'RFD most recent', 'RFD Increased', 'Population recent', 'Population Increase', 'Surface Price (€/m2)', 'Monthly Price (€/month)', 'Surface Price Increase', 'Monthly Price Increase', 'Info Idealista'])

    uploadRDDtoMongo(df, spark)


if __name__ == '__main__':
    main()
