from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, Bucketizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, OneHotEncoder

def loadMongoDF(db, collection):
    '''
    Download data from mongodb and store it in DF format
    '''
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    dataDF = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/{db}.{collection}") \
        .load()

    return dataDF, spark

## --------------- KPI 3 DF --------------- 
## --> predict number of rooms given a price and neigbirhood_id
dataDF, spark = loadMongoDF(db='formatted', collection='data')
subsetDF = dataDF.select('Neighborhood Id', 'Price', 'Rooms') \
                .withColumnRenamed("Neighborhood Id","Neighborhood_ID")

# Indexing 'Neighborhood_ID' (necessary for one-hot encoding)
indexers = [StringIndexer(inputCol=column, outputCol=column+"_INDEX").fit(subsetDF) for column in ['Neighborhood_ID']]
pipeline = Pipeline(stages=indexers) # bulky... can change to be more streamlined in final pipeline
modelDF = pipeline.fit(subsetDF).transform(subsetDF)

# One-Hot Encoding 'Neighborhood_ID'
single_col_ohe = OneHotEncoder(inputCol="Neighborhood_ID_INDEX", outputCol="Neighborhood_ID_OneHot")
modelDF_OneHot = single_col_ohe.fit(modelDF).transform(modelDF)

# creating label index from 'Rooms' and Feature Vector from 'features'
modelDF = StringIndexer(inputCol="Rooms", outputCol="indexedRooms") \
                .fit(modelDF_OneHot) \
                .transform(modelDF_OneHot)

# assembling feature vector for model
modelDF = VectorAssembler(inputCols=['Neighborhood_ID_OneHot', 'Price'], outputCol="indexedFeatures") \
                .transform(modelDF)

# removing clutter coluns from DF
modelDF = modelDF.select('Price', 'Rooms', 'Neighborhood_ID_OneHot', 'indexedRooms', 'indexedFeatures')

# Split the data into training and test sets (30% held out for testing)
(trainingDataDF, testDataDF) = modelDF.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedRooms", 
                            featuresCol="indexedFeatures", 
                            numTrees=3, 
                            maxDepth=4, 
                            maxBins=32)

# Convert indexed labels back to original labels.
#labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=modelDF.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[rf])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingDataDF)

# Make predictions.
predictions = model.transform(testDataDF)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedRooms", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages
print(rfModel)  # summary only