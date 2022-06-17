from pyspark.sql import SparkSession
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

    print(row)


# fields = [StructField("neigh_id", IntegerType(),True),
#           StructField("cost", IntegerType(),True)]
# schema = StructType(fields)
# data = [[neigh_id,cost]]
# df = spark.createDataFrame(data, schema)
#
# #prediction = model.transform(df)


if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
        .getOrCreate()

    sc = spark.sparkContext

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "venomoth.fib.upc.edu:9092") \
        .option("subscribe", "bdm_p2") \
        .load()

    df1 = (df
           .withColumn("key", df["key"].cast(StringType()))
           .withColumn("value", df["value"].cast(StringType())))

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType

    # schema_wiki = StructType(
    #     [StructField("neigh",StringType(),True),
    #      StructField("cost",IntegerType(),True)])

    schema_wiki = StructType(
        [StructField("$schema",StringType(),True),
         StructField("bot",BooleanType(),True),
         StructField("comment",StringType(),True),
         StructField("id",StringType(),True),
         StructField("length",
                     StructType(
                         [StructField("new",IntegerType(),True),
                          StructField("old",IntegerType(),True)]),True),
         StructField("meta",
                     StructType(
                         [StructField("domain",StringType(),True),
                          StructField("dt",StringType(),True),
                          StructField("id",StringType(),True),
                          StructField("offset",LongType(),True),
                          StructField("partition",LongType(),True),
                          StructField("request_id",StringType(),True),
                          StructField("stream",StringType(),True),
                          StructField("topic",StringType(),True),
                          StructField("uri",StringType(),True)]),True),
         StructField("minor",BooleanType(),True),
         StructField("namespace",IntegerType(),True),
         StructField("parsedcomment",StringType(),True),
         StructField("patrolled",BooleanType(),True),
         StructField("revision",
                     StructType(
                         [StructField("new",IntegerType(),True),
                          StructField("old",IntegerType(),True)]),True),
         StructField("server_name",StringType(),True),
         StructField("server_script_path",StringType(),True),
         StructField("server_url",StringType(),True),
         StructField("timestamp",StringType(),True),
         StructField("title",StringType(),True),
         StructField("type",StringType(),True),
         StructField("user",StringType(),True),
         StructField("wiki",StringType(),True)])


    df_wiki = (df1.withColumn("value", from_json("value", schema_wiki)))

    #df_wiki_formatted = (df_wiki.select("value.neigh", "value.cost"))

    df_wiki_formatted = (df_wiki.select(
        col("key").alias("event_key")
        ,col("topic").alias("event_topic")
        ,col("timestamp").alias("event_timestamp")
        ,col("value.$schema").alias("schema")
        ,"value.bot"
        ,"value.comment"
        ,"value.id"
        ,col("value.length.new").alias("length_new")
        ,col("value.length.old").alias("length_old")
        ,"value.minor"
        ,"value.namespace"
        ,"value.parsedcomment"
        ,"value.patrolled"
        ,col("value.revision.new").alias("revision_new")
        ,col("value.revision.old").alias("revision_old")
        ,"value.server_name"
        ,"value.server_script_path"
        ,"value.server_url"
        ,to_timestamp(from_unixtime(col("value.timestamp"))).alias("change_timestamp")
        ,to_date(from_unixtime(col("value.timestamp"))).alias("change_timestamp_date")
        ,"value.title"
        ,"value.type"
        ,"value.user"
        ,"value.wiki"
        ,col("value.meta.domain").alias("meta_domain")
        ,col("value.meta.dt").alias("meta_dt")
        ,col("value.meta.id").alias("meta_id")
        ,col("value.meta.offset").alias("meta_offset")
        ,col("value.meta.partition").alias("meta_partition")
        ,col("value.meta.request_id").alias("meta_request_id")
        ,col("value.meta.stream").alias("meta_stream")
        ,col("value.meta.topic").alias("meta_topic")
        ,col("value.meta.uri").alias("meta_uri")
    ))

    raw_path = "exploitation/wiki-changes"
    checkpoint_path = "exploitation/wiki-changes-checkpoint"

    queryStream = df_wiki_formatted \
        .writeStream.format("parquet") \
        .queryName('wiki_changes_ingestion') \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", raw_path) \
        .outputMode("append") \
        .partitionBy("change_timestamp_date", "server_name") \
        .start()

    df_wiki_changes = (
        spark
            .readStream
            .format("parquet")
            .schema(df_wiki_formatted.schema)
            .load(raw_path)
    )

    queryStreamMem = (df_wiki_changes
                      .writeStream
                      .foreach(process)
                      .format("memory")
                      .queryName("wiki_changes_count")
                      .outputMode("update")
                      .start())

    try:
        i=1
        # While stream is active, print count
        while len(spark.streams.active) > 0:

            # Clear output
            clear_output(wait=True)
            print("Run:{}".format(i))

            lst_queries = []
            for s in spark.streams.active:
                lst_queries.append(s.name)

            # Verify if wiki_changes_count query is active before count
            if "wiki_changes_count" in lst_queries:
                # Count number of events
                spark.sql("select count(1) as qty from wiki_changes_count").show()
            else:
                print("'wiki_changes_count' query not found.")

            sleep(5)
            i=i+1

    except KeyboardInterrupt:
        # Stop Query Stream
        queryStreamMem.stop()

        print("stream process interrupted")

    for s in spark.streams.active:
        print("ID:{} | NAME:{}".format(s.id, s.name))

    #queryStream.stop()
    queryStream.awaitTermination(timeout=30)

    # df2 = df.selectExpr("CAST(value AS STRING)").writeStream.foreach(process).start()
    # df2.awaitTermination(timeout=10)
    #
    # fields = [StructField("neigh_id", IntegerType(),True),
    #           StructField("cost", IntegerType(),True)]
    # schema = StructType(fields)
    # data = [[neigh_id,cost]]
    # df = spark.createDataFrame(data, schema)
    # print('hi')
    # values = df2.select(df2.value.split(",").cast("array<Double>").alias("inputs"))
    # print(values.printSchema)
    #
    # conv_vec = udf(lambda vs: Vectors.dense([float(i) for i in vs]), VectorUDT())
    #
    # def predict_mw(sdf):
    #     mw = model.transform(sdf)
    #     return mw
    #
    # df_out = predict_mw(conv_vec)



    # query = df.selectExpr("CAST(value AS STRING)") \
    #     .writeStream \
    #     .foreach(process) \
    #     .start()
    #
    # query.awaitTermination(timeout=30)
