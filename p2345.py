import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, round, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from tools import view_sensor_topic_messages


df = view_sensor_topic_messages("building_sensors_MZ")

# Агрегація даних:
windowed_avg = df.withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(round(avg("temperature"), 1).alias("t_avg"), round(avg("humidity"), 1).alias("h_avg"))

def print_avg_to_console(df, epoch_id): 
    if df.count() > 0:
        print("windowed_avg:")
        sorted_df = df.orderBy(col("window").asc()) 
        total_count = sorted_df.count() 
        print(f"Total number of rows: {total_count}")
        sorted_df.show(truncate=False, n=100)

windowed_avg.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_avg_to_console) \
    .start() \
    .awaitTermination()

# Завантаження параметрів алертів
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .config("spark.default.parallelism", "1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    .getOrCreate()

alerts_parameters_df = pd.read_csv("alerts_conditions.csv")
alerts_parameters_spark_df = spark.createDataFrame(alerts_parameters_df)

# Вибір умов та побудова алертів
alerts_crossed = windowed_avg.crossJoin(alerts_parameters_spark_df)
alerts_cracced_and_filtered = alerts_crossed.filter(
    ( (alerts_crossed['humidity_min'] == -999) | (alerts_crossed['h_avg'] >= alerts_crossed['humidity_min']) ) &
    ( (alerts_crossed['humidity_max'] == -999) | (alerts_crossed['h_avg'] <= alerts_crossed['humidity_max']) ) &
    ( (alerts_crossed['temperature_min'] == -999) | (alerts_crossed['t_avg'] >= alerts_crossed['temperature_min']) ) &
    ( (alerts_crossed['temperature_max'] == -999) | (alerts_crossed['t_avg'] <= alerts_crossed['temperature_max']) ) 
)

target_data_for_alerts = alerts_cracced_and_filtered \
    .select(col("window"), col("t_avg"), col("h_avg"), col("code"), col("message"))

# target_data_for_alerts.printSchema()
target_data_for_alerts = target_data_for_alerts.withColumn("code", col("code").cast("string"))
target_data_for_alerts.printSchema()

# Вивід у консоль таблиці алертів та запис даних у Kafka топік
json_df = target_data_for_alerts.select(to_json(struct("window", "t_avg", "h_avg", "code", "message")).alias("value"))

def print_JSON_alerts_to_console_and_kafka(df, epoch_id):
    if df.count() > 0:
        print("JSON:")
        df.show(truncate=False, n=100)
        df.write.format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
            .option("topic", "building_sensors_alerts_MZ") \
            .save()
        
query = json_df.writeStream \
    .trigger(availableNow=True) \
    .format("console") \
    .outputMode("append") \
    .foreachBatch(print_JSON_alerts_to_console_and_kafka) \
    .start()
    
query.awaitTermination()