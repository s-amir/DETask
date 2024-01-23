import json

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import uuid
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer, cimpl


spark = SparkSession.builder.config("spark.jars", "/home/amirhosseindarvishi/Downloads/clickhouse-integration-spark_2"
                                                  ".12-2.7.1.jar") \
    .master("local").appName("PySpark_clickhouse").getOrCreate()
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://localhost:8123") \
    .option("dbtable", "bama_ads") \
    .load()
jdbcDF.show()


# Function to create a random dictionary with ad_id, timestamp, and uuid
def create_random_dictionary():
    events = []
    now = datetime.now()
    thirty_minutes_ago = now - timedelta(minutes=30)
    uuid_list = [uuid.uuid4() for _ in range(1, 201)]

    for i in range(1, 100):
        random_seconds = random.randint(0, 30 * 60)  # 30 minutes * 60 seconds

        # Create a random timestamp within the last 30 minutes
        random_timestamp = thirty_minutes_ago + timedelta(seconds=random_seconds)
        index=random.randint(1, 200)
        print(index)
        event = {
            "ad_id": random.randint(100, 500),  # Random ad_id between 100 and 500
            "timestamp": random_timestamp.isoformat(),  # Current timestamp
            "uuid": str(uuid_list[index])
        }
        events.append(event)
    return events


# conf = {'bootstrap.servers': 'localhost:9092'}
# producer = Producer(conf)
# events = create_random_dictionary()
# count = 0
# for event in events:
#     producer.produce('click', json.dumps(event))
#     producer.produce('view', json.dumps(event))
#     print(f'done {count}')
#     count += 1
# producer.flush()



