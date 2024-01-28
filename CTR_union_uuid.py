from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, countDistinct

'''
calculate CTR_unique_uuid from mysql data that are related to 6 hours ago with pyspark 
note:change the path of driver
'''

spark1 = SparkSession.builder.appName("CTR_Mysql").config(
    "spark.jars", "/home/amirhosseindarvishi/Downloads/mysql-connector-java-8.0.13.jar ").getOrCreate()

click = spark1.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/db") \
    .option("dbtable", 'bama_click') \
    .option("user", "root") \
    .option("password", "root") \
    .load()

view = spark1.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/db") \
    .option("dbtable", 'bama_view') \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Calculate the timestamp 6 hours ago
current_timestamp = datetime.now()
six_hours_ago_timestamp = current_timestamp - timedelta(hours=48)
formatted_timestamp = six_hours_ago_timestamp.strftime("%Y-%m-%d %H:%M:%S")

click = click.filter(col('timestamp') >= formatted_timestamp).groupBy('ad_id').agg(countDistinct('uuid')).withColumnRenamed('count(DISTINCT uuid)','count_unique_uuid_click')
# click.show(truncate=False)
view = view.filter(col('timestamp') >= formatted_timestamp).groupBy('ad_id').agg(countDistinct('uuid')).withColumnRenamed('count(DISTINCT uuid)','count_unique_uuid_view')
# view.show(truncate=False)

CTR = view.join(click, on='ad_id', how='left')
CTR = CTR.withColumn("CTR_unique_uuid", col('count_unique_uuid_click') / col('count_unique_uuid_view')).na.fill(0, subset=["CTR_unique_uuid"]).sort(desc('CTR_unique_uuid'))
CTR.show(500,truncate=False)
