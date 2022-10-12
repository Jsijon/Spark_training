from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("word_counter") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()    

userSchema = StructType([StructField('line', StringType(), True)])   

lines_df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe","word-count_2")  \
  .load()


new_lines_df = lines_df.select(from_json(col("value").cast('string'), userSchema).alias('line'))

new_lines_df = new_lines_df.select(
   explode(
       split(new_lines_df.line, " ")
   ).alias("word")
)

wordCounts = new_lines_df.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
