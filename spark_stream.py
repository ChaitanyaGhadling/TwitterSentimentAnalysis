from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.types import *
import functools as ft
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType




def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string, StringType())

spark = SparkSession \
    .builder \
    .appName("Sentiment Analysis") \
    .getOrCreate()

socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
        .option("port", 5518) \
    .load()

socketDF.printSchema()   
oldColumns = socketDF.schema.names
newColumns = ["_c5"]

df = ft.reduce(lambda socketDF, id: socketDF.withColumnRenamed(oldColumns[id],
            newColumns[id]), range(len(oldColumns)), socketDF)

rf = PipelineModel.load("/home/chaitanya/spark-3.1.2-bin-hadoop2.7/python/Sentiment_Analysis_model")
prediction = rf.transform(df)
prediction = prediction.select("_c5","prediction")

"""prediction = prediction.withColumn('words_string', array_to_string_udf(prediction["words"]))
prediction = prediction.withColumn('filtered_as_str', array_to_string_udf(prediction["filtered"]))
prediction = prediction.withColumn('pred_as_str', array_to_string_udf(prediction["rawPrediction"]))
prediction = prediction.withColumn('probability_as_str', array_to_string_udf(prediction["probability"]))
prediction = prediction.withColumn('c5_as_str', array_to_string_udf(prediction["_c5"]))
prediction = prediction.drop("_c5")
prediction = prediction.drop("words")
prediction = prediction.drop("filtered")
prediction = prediction.drop("rawPrediction")
prediction = prediction.drop("probability")"""
File_save2 = prediction.writeStream.format("csv").trigger(processingTime='30 seconds').option("path","tweet_output").option("checkpointLocation","ckpt_output").outputMode("append").start()
#File_save2 = prediction.writeStream.format("console").outputMode("append").start()
File_save2.awaitTermination()
