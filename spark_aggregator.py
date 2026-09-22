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

def write_to_duckdb(batch_df, batch_id):
    import duckdb
    pandas_df = batch_df.toPandas()
    if pandas_df.empty:
        return
    con = duckdb.connect("streaming_results.duckdb")
    con.execute("""
        CREATE TABLE IF NOT EXISTS windowed_orders (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            region VARCHAR,
            total_amount DOUBLE,
            order_count BIGINT
        )
    """)
    pandas_df["window_start"] = pandas_df["window"].apply(lambda w: w["start"])
    pandas_df["window_end"] = pandas_df["window"].apply(lambda w: w["end"])
    con.executemany(
        "INSERT INTO windowed_orders VALUES (?, ?, ?, ?, ?)",
        pandas_df[["window_start", "window_end", "region", "total_amount", "order_count"]].values.tolist()
    )
    con.close()
    print(f"Batch {batch_id}: wrote {len(pandas_df)} rows to streaming_results.duckdb")

query = windowed_totals.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_duckdb) \
    .start()

query.awaitTermination()