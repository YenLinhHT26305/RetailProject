from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
)

# =====================================================
# CONFIG
# =====================================================

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"

BRONZE_PATH = "s3a://bronze/delta/online_retail_raw"
SILVER_PATH = "s3a://silver/sales/sales_clean"

# =====================================================
# SPARK
# =====================================================

spark = (
    SparkSession.builder
    .appName("Silver-Sales")
    .master("spark://spark-master:7077")

    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# READ BRONZE
# =====================================================

df = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)

# =====================================================
# SELECT
# =====================================================

df = df.select(
    col("Invoice").alias("invoice"),
    col("Quantity").cast("int").alias("quantity"),
    col("Price").cast("double").alias("price"),
    col("InvoiceDate").cast("timestamp").alias("invoice_date"),
    col("Country").alias("country")
)

# =====================================================
# BUSINESS
# =====================================================

df = (
    df
    .withColumn(
        "is_cancelled",
        when(col("invoice").startswith("C"), True).otherwise(False)
    )
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)

# =====================================================
# WRITE
# =====================================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)

print("Silver Sales Created")

spark.stop()