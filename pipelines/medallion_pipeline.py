import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, DateType, IntegerType
)


# --- Spark Session ---
def create_spark_session():
    spark = SparkSession.builder \
        .appName("SmartDQAssistant") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --- Schema ---
ORDER_SCHEMA = StructType([
    StructField("order_id",         StringType(),  True),
    StructField("customer_id",      StringType(),  True),
    StructField("order_amount",     DoubleType(),  True),
    StructField("order_date",       DateType(),    True),
    StructField("product_category", StringType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("delivery_status",  StringType(),  True),
    StructField("customer_age",     IntegerType(), True),
])


# --- Bronze Layer ---
def write_bronze(spark, input_path="data/raw/orders_dataset.csv",
                 output_path="data/bronze/orders"):
    print("Writing Bronze layer...")
    df = spark.read.csv(input_path, header=True, schema=ORDER_SCHEMA)
    df = df.withColumn("ingested_at", F.current_timestamp())
    df.write.mode("overwrite").parquet(output_path)
    count = spark.read.parquet(output_path).count()
    print(f"Bronze rows: {count:,}")
    return df


# --- Silver Transformation ---
def transform_to_silver(df, output_path="data/silver/orders"):
    print("Writing Silver layer...")

    VALID_CATEGORIES = ["Electronics", "Clothing", "Books", "Food"]
    VALID_PAYMENTS   = ["UPI", "Card", "COD"]
    VALID_STATUSES   = ["Delivered", "Pending", "Cancelled"]

    silver_df = df \
        .dropDuplicates(["order_id"]) \
        .filter(F.col("order_amount") > 0) \
        .filter(F.col("order_date") <= F.current_date()) \
        .filter(F.col("product_category").isin(VALID_CATEGORIES)) \
        .filter(F.col("payment_method").isin(VALID_PAYMENTS)) \
        .filter(F.col("delivery_status").isin(VALID_STATUSES)) \
        .filter(F.col("customer_age").between(18, 100)) \
        .withColumn("order_amount", F.round(F.col("order_amount"), 2)) \
        .withColumn("transformed_at", F.current_timestamp())

    silver_df.write.mode("overwrite").parquet(output_path)

    spark = silver_df.sparkSession
    count = spark.read.parquet(output_path).count()
    print(f"Silver rows: {count:,}")
    return silver_df


# --- Gold Aggregates ---
def write_gold(silver_df):
    print("Writing Gold layer...")
    spark = silver_df.sparkSession

    # Gold 1 — Revenue by category
    revenue_by_category = silver_df.groupBy("product_category") \
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("order_amount").alias("avg_order_value")
        )
    revenue_by_category.write.mode("overwrite") \
        .parquet("data/gold/revenue_by_category")

    # Gold 2 — Daily order counts
    daily_orders = silver_df.groupBy("order_date") \
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_amount").alias("daily_revenue")
        ).orderBy("order_date")
    daily_orders.write.mode("overwrite") \
        .parquet("data/gold/daily_orders")

    print(f"Gold - Revenue by category rows: {revenue_by_category.count()}")
    print(f"Gold - Daily orders rows: {daily_orders.count()}")


# --- Data Profiling ---
def profile_dataframe(df, layer_name="silver"):
    print(f"\n--- Data Profile: {layer_name} ---")
    profile = {}
    total_rows = df.count()

    for col_name in df.columns:
        if col_name in ("ingested_at", "transformed_at"):
            continue

        col_stats = {"column": col_name, "total_rows": total_rows}

        # Null rate
        null_count = df.filter(F.col(col_name).isNull()).count()
        col_stats["null_rate"]  = round(null_count / total_rows, 4)
        col_stats["null_count"] = null_count

        # Distinct count
        col_stats["distinct_count"] = df.select(col_name).distinct().count()

        # Numeric stats
        if df.schema[col_name].dataType in (DoubleType(), IntegerType()):
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.avg(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev")
            ).collect()[0]
            col_stats["min"]    = round(float(stats["min"]), 2) if stats["min"] else None
            col_stats["max"]    = round(float(stats["max"]), 2) if stats["max"] else None
            col_stats["mean"]   = round(float(stats["mean"]), 2) if stats["mean"] else None
            col_stats["stddev"] = round(float(stats["stddev"]), 2) if stats["stddev"] else None

        # Top values for string columns
        if df.schema[col_name].dataType == StringType():
            top_values = df.groupBy(col_name).count() \
                .orderBy(F.desc("count")).limit(5) \
                .rdd.map(lambda r: r[0]).collect()
            col_stats["top_values"] = top_values

        profile[col_name] = col_stats
        print(f"  {col_name}: nulls={col_stats['null_rate']:.2%}, "
              f"distinct={col_stats['distinct_count']}")

    return profile


# --- Main ---
if __name__ == "__main__":
    spark = create_spark_session()

    bronze_df = write_bronze(spark)
    silver_df = transform_to_silver(bronze_df)
    write_gold(silver_df)
    profile   = profile_dataframe(silver_df, "silver")

    print("\nPipeline complete.")
    spark.stop()