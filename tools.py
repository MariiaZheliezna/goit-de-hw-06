from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window, avg, round, to_json, struct


def create_kafka_topic(topic):
    admin_client = AdminClient(kafka_config)
    kafla_topic = NewTopic(topic, num_partitions=2, replication_factor=1)
    try: 
        admin_client.create_topics([kafla_topic], validate_only=False)
    except Exception as e: 
        print(f"An error occurred: {e}")
    view_kafka_topics("_MZ")

def view_kafka_topics(mask):
    admin_client = AdminClient(kafka_config)
    metadata = admin_client.list_topics(timeout=10)
    topics = metadata.topics
    print(f"List of kafka topiks with mask '{mask}':")
    [print(topic) for topic in topics if mask in topic]

def del_kafka_topic(topic):
    admin_client = AdminClient(kafka_config)
    fs = admin_client.delete_topics([topic], operation_timeout=30)
    # Очікування результату операції видалення
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Топік {topic} успішно видалено")
        except Exception as e:
            print(f"Не вдалося видалити топік {topic}: {e}")

def print_to_console_s(df, epoch_id): 
    sorted_df = df.orderBy(col("timestamp").asc())
    total_count = sorted_df.count() 
    print(f"Total number of rows: {total_count}")
    sorted_df.show(truncate=False, n=150)

def print_to_console_a(df, epoch_id): 
    # Сортування даних всередині функції foreachBatch перед виведенням 
    sorted_df = df.orderBy(col("window").asc())
    total_count = sorted_df.count() 
    print(f"Total number of rows: {total_count}")
    sorted_df.show(truncate=False, n=20)

def view_sensor_topic_messages(topic):
    # Створення сесії Spark
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .getOrCreate()
    schema = StructType([
        StructField("sensor_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType())
        ])
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "5") \
        .load()   
    df = df.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema).alias("data")) \
        .select("data.*")
    query = df.writeStream \
        .trigger(availableNow=True) \
        .outputMode("append") \
        .format("console") \
        .foreachBatch(print_to_console_s) \
        .start() \
        .awaitTermination()

def view_alert_topic_messages(topic):
    # Створення сесії Spark
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .getOrCreate()
    schema = StructType([
        StructField("window", StructType([
            StructField("start", TimestampType()),
            StructField("end", TimestampType())
        ])),
        StructField("t_avg", DoubleType()),
        StructField("h_avg", DoubleType()),
        StructField("code", StringType()),
        StructField("message", StringType())
    ])
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "5") \
        .load()   
    df = df.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema).alias("data")) \
        .select("data.*")
    query = df.writeStream \
        .trigger(availableNow=True) \
        .outputMode("append") \
        .format("console") \
        .foreachBatch(print_to_console_a) \
        .start() \
        .awaitTermination()
    


# topic_name="building_sensors_alerts_MZ"
topic_name="building_sensors_MZ"
# view_kafka_topics("_MZ")
view_sensor_topic_messages(topic_name)
# view_alert_topic_messages(topic_name)
# create_kafka_topic(topic_name)