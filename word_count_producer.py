from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("word_counter") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()    

inputPath = "./text_directory/"

userSchema = StructType([StructField('line', StringType(), True)])   

lines_df = spark \
    .readStream \
    .option("sep", "/t") \
    .schema(userSchema) \
    .csv(inputPath)

lines_df.selectExpr(" 'line' AS key", "to_json(struct(*)) AS value") \
   .writeStream \
   .format("kafka") \
   .outputMode("append") \
   .option("kafka.bootstrap.servers", "0.0.0.0:9092") \
   .option("topic", "word-count_2") \
   .option("checkpointLocation", "./checkpoint_dir") \
   .start() \
   .awaitTermination()

query = lines_df.writeStream.format("console").start()   
