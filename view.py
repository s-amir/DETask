from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

"""
stream message consuming from kafka topic=view and insert them to mysql with pyspark
"""

schema = StructType([
    StructField("ad_id", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("uuid", StringType())
])

spark2 = SparkSession.builder.appName("kafka1").config("spark.jars.packages",
                                                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").config(
    "spark.jars", "/home/amirhosseindarvishi/Downloads/mysql-connector-java-8.0.13.jar ").getOrCreate()

view = spark2.readStream.format("kafka").option("forceDeleteTempCheckpointLocation", "true").option(
    "kafka.bootstrap.servers", "localhost:9092").option("subscribe", "view").option("startingOffsets",
                                                                                    "earliest").load()

view_value = view.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data"))
final_view = view_value.select("data.ad_id", "data.timestamp", "data.uuid")


def write_to_mysql(batch_df, epoch_id, table_name):
    print(epoch_id)
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/db") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()


final_view.writeStream \
    .foreachBatch(lambda batch_df, epoch_id: write_to_mysql(batch_df, epoch_id, "bama_view")) \
    .outputMode("append") \
    .start().awaitTermination()
