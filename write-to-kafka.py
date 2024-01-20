from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("date", StringType()) \
                     .add("location", StringType()) \
                     .add("confirmed_cases", IntegerType()) \
                     .add("deaths", IntegerType()) \
                     .add("recovered_cases", IntegerType()) \
                     .add("active_cases", IntegerType())

# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "D:\covid\data") \
    .load()

# Select specific columns and write to Kafka
df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "covid") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
