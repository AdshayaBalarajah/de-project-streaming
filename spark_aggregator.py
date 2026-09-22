from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("OrdersStreamingAggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_name", StringType()) \
    .add("product", StringType()) \
    .add("amount", DoubleType()) \
    .add("region", StringType()) \
    .add("timestamp", DoubleType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

orders = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

windowed_totals = orders \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("region")
    ) \
    .agg(
        spark_sum("amount").alias("total_amount"),
        count("*").alias("order_count")
    )

query = windowed_totals.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()