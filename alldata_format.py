import os
import operator
from typing import Tuple
import pandas as pd
import numpy as np
from pyspark import RDD
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode, col


def loadMongoRDD(collection: str):
    '''
    
    '''
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    dataRDD = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/opendata.{collection}") \
        .load() \
        .rdd

    return dataRDD


def mean(x, var):
    '''
    
    '''
    suma = 0
    num = len(x)
    for i in range(0,num):
        suma = suma + x[i][var]
    mean = suma/num
    return mean


def mostrecent(x, var):
    '''
    
    '''
    x.sort(reverse=True, key=lambda x: x['year'])
    return x[0][var]


def increase(x, var):
    '''
    
    '''
    x.sort(reverse=True, key=lambda x: x['year'])
    diff = x[0][var] - x[1][var] #difference between the var of the last year and the year before
    return float("{:.2f}".format(diff))


def unroll(x: Tuple[str, Tuple[float, str]]):
    '''
    
    '''
    (_, (price, ne_re)) = x
    return (ne_re, price)


def generateIncomeRDD(incomeRDD):
    """
    RDD generated has the following structure:
    - Key: neighborhood
    - Values: last year of RFD (family income index), last year of population, increase of RFD, increase od population
    """
    rdd = incomeRDD \
        .map(lambda x: (x['neigh_name '], (x['district_name'], mostrecent(x['info'], 'RFD'), increase(x['info'], 'RFD'), mostrecent(x['info'], 'pop'), increase(x['info'], 'pop')))) \
        .join(lookup_income_neighborhood_RDD) \
        .distinct() \
        .map(unroll) \
        .cache()
    return rdd


def generatePreuRDD(preuRDD):
    '''
    
    '''
    # we remove the missing values
    preuRDD = preuRDD \
        .filter(lambda x: x['Preu'] != '--') \
        .filter(lambda x: x['Preu'] != None) \
        .filter(lambda x: 2020 <= x['Any']) \
        .cache()

    #here we create a RDD only with the "superficie" values and do the mean of all the price per trimester in the last two years
    rdd2_sup = preuRDD \
        .filter(lambda x: 'superfície' in x['Lloguer_mitja']) \
        .map(lambda x: ((x['Nom_Barri'], x['Any'], x['Nom_Districte']), float(x['Preu']))) \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
        .mapValues(lambda x: float("{:.2f}".format(x[0]/x[1])) ) \
        .distinct() \
        .cache()

    #here we create a RDD only with the "mensual" values and do the mean of all the price per trimester in the last two years
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

    # we do a join of the values in order to have both last years in the same row
    rdd2_join2 = rdd2_all \
        .filter(lambda x: 2021 == x[1][1]) \
        .join(rdd2_all.filter(lambda x: 2020 == x[1][1])) \
        .cache()

    # neigh (key), district, precio superficie ultimo año, precio mensual ultimo año, incremento precio superficie, incremento precio mensual
    rdd = rdd2_join2 \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][2], x[1][0][3], float("{:.2f}".format(x[1][0][2]-x[1][1][2])), float("{:.2f}".format(x[1][0][3]-x[1][1][3]))))) \
        .join(lookup_income_neighborhood_RDD) \
        .map(unroll) \
        .cache()

    return rdd


def validate_idealista(rdd_in):
    '''
    
    '''


def transform_idealista(rdd_in):
    '''
    
    '''
    transform_rdd = rdd_in \
        .map(lambda x: (x['propertyCode'], 
                        x['propertyType'],
                        x['operation'],
                        x['country'],
                        x['municipality'],
                        x['province'],
                        x['district'], 
                        x['neighborhood'],
                        x['price'],
                        x['priceByArea'],
                        x['rooms'],
                        x['bathrooms'],
                        x['size'],
                        x['status'],
                        x['floor'],
                        x['hasLift'],
                        x['parkingSpace'],
                        x['newDevelopment'],
                        x['numPhotos'],
                        x['distance'], 
                        x['exterior'])) \
        .distinct()
    
    return transform_rdd
    

def merge_all():
    '''
    
    '''


def main():
    '''
    
    '''
    # main idealista-parquet code
    directory = "landing/persistent/idealista"
    parq_files = {}  # List which will store all of the full filepaths.
    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            if filename[-7:] == 'parquet':
                parq_files[root[29:39]] = (root+'/'+filename)
                
    # main opendatabcn-mongodb code
    collections = ['income', 
                   'preu', 
                   'income_lookup_district', 
                   'income_lookup_neighborhood', 
                   'rent_lookup_district', 
                   'rent_lookup_neighborhood']

    incomeRDD = loadMongoRDD(collections[0]).cache() # load opendata_income from mongodb
    preuRDD = loadMongoRDD(collections[1]).cache() # load opendata_preu from mongodb
    lookup_income_neighborhood_RDD = loadMongoRDD(collections[3]) \ # load opendata_lookup from mongodb
        .map(lambda x: (x['neighborhood'], 
                        x['neighborhood_reconciled']))
        .cache()
        
    # generate and preprocess opendata_income RDD
    rdd1 = generateIncomeRDD(incomeRDD)

    # print('####################')
    # print('****** RDD1 ******')
    # rdd1.foreach(lambda r: print(r))

    # generate and preprocess opendata_preu RDD
    rdd2 = generatePreuRDD(preuRDD)

    # merge income and preu RDDs
    rdd3 = rdd1 \
        .join(rdd2) \
        .map(lambda x: (x[0], (x[1][0][0], 
                               x[1][0][1], 
                               x[1][0][2], 
                               x[1][0][3], 
                               x[1][0][4], 
                               x[1][1][1], 
                               x[1][1][2], 
                               x[1][1][3], 
                               x[1][1][4]))) \
        .cache()

    print('####################')
    print('****** RDD3 ******')
    rdd3.foreach(lambda r: print(r))
    print(rdd3.count())
    
    # spark transformations in sequence for each parquet file
    i = 0 # special loop counter
    for key in parq_files:
        # read spark df from parquet file
        df = spark.read.parquet(parq_files[key])
        rdd_addDate = df.withColumn("date", lit(key)).rdd # add 'date' attribute and transform into rdd
        transform_rdd = transform_idealista(rdd_addDate) # remove duplicates and select attributes
        if i == 0:
            union_idealista_rdd = transform_rdd
        else:
            union_idealista_rdd = union_idealista_rdd.union(transform_rdd)
        i += 1

    
if __name__ == '__main__':
    main()
        
        
       