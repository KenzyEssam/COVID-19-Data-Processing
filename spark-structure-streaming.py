from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql

def insert_into_spread_tracking(row):
    host = "localhost"
    port = 3306
    database = "covid"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    date = row.date
    total_confirmed_cases = row.total_confirmed_cases 
    total_deaths = row.total_deaths  
    total_recovered_cases = row.total_recovered_cases  
    total_active_cases = row.total_active_cases  

    sql_query_a = f"INSERT INTO spread_tracking (date, total_confirmed_cases, total_deaths, total_recovered_cases, total_active_cases) " \
                  f"VALUES ('{date}', {total_confirmed_cases}, {total_deaths}, {total_recovered_cases}, {total_active_cases}) " \
                  f"ON DUPLICATE KEY UPDATE " \
                  f"total_confirmed_cases={total_confirmed_cases}, total_deaths={total_deaths}, " \
                  f"total_recovered_cases={total_recovered_cases}, total_active_cases={total_active_cases}"

    cursor.execute(sql_query_a)
    conn.commit()
    conn.close()

def insert_into_hotspots(row):
    host = "localhost"
    port = 3306
    database = "covid"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    location = row.location
    total_confirmed_cases = row.total_confirmed_cases
    total_deaths = row.total_deaths
    total_recovered_cases = row.total_recovered_cases
    total_active_cases = row.total_active_cases

    # Calculate case_fatality_rate and recovered_cases_rate
    case_fatality_rate = (row.total_deaths / row.total_confirmed_cases) * 100 if row.total_confirmed_cases > 0 else 0
    recovered_cases_rate = (row.total_recovered_cases / row.total_confirmed_cases) * 100 if row.total_confirmed_cases > 0 else 0


    sql_query_b = f"INSERT INTO hotspots (location, total_confirmed_cases, total_deaths, total_recovered_cases, total_active_cases, " \
              f"case_fatality_rate, recovered_cases_rate) " \
              f"VALUES ('{location}', {total_confirmed_cases}, {total_deaths}, {total_recovered_cases}, {total_active_cases}, " \
              f"CONCAT('{case_fatality_rate:.2f}', '%'), CONCAT('{recovered_cases_rate:.2f}', '%')) " \
              f"ON DUPLICATE KEY UPDATE " \
              f"total_confirmed_cases={total_confirmed_cases}, total_deaths={total_deaths}, " \
              f"total_recovered_cases={total_recovered_cases}, total_active_cases={total_active_cases}, " \
              f"case_fatality_rate=CONCAT('{case_fatality_rate:.2f}', '%'), recovered_cases_rate=CONCAT('{recovered_cases_rate:.2f}', '%')"

    
    cursor.execute(sql_query_b)
    conn.commit()
    conn.close()

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

schema = StructType().add("date", StringType()) \
                     .add("location", StringType()) \
                     .add("confirmed_cases", IntegerType()) \
                     .add("deaths", IntegerType()) \
                     .add("recovered_cases", IntegerType()) \
                     .add("active_cases", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# Group by date, then aggregate the sum of cases
aggregated_date_df = df.groupBy("data.date") \
    .agg(
        sum("data.confirmed_cases").alias("total_confirmed_cases"),
        sum("data.deaths").alias("total_deaths"),
        sum("data.recovered_cases").alias("total_recovered_cases"),
        sum("data.active_cases").alias("total_active_cases")
    )

aggregated_location_df = df.groupBy("data.location") \
    .agg(
        sum("data.confirmed_cases").alias("total_confirmed_cases"),
        sum("data.deaths").alias("total_deaths"),
        sum("data.recovered_cases").alias("total_recovered_cases"),
        sum("data.active_cases").alias("total_active_cases")
    )

filtered_hotspots_df = aggregated_location_df.filter(col("total_confirmed_cases") > 40)


# Write to spread_tracking table
spread_tracking_query = aggregated_date_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .foreach(insert_into_spread_tracking) \
    .start()

hotspots_query = filtered_hotspots_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .foreach(insert_into_hotspots) \
    .start()


spread_tracking_query.awaitTermination()
hotspots_query.awaitTermination()