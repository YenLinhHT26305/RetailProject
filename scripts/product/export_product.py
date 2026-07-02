import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. CAU HINH
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")

TOP_PRODUCTS_PATH = "s3a://gold/product/top_products"
CANCELLATION_RATE_PATH = (
    "s3a://gold/product/product_cancellation_rate"
)

# E:\RetailProject\exports  ->  /opt/spark-apps/exports
LOCAL_EXPORT_BASE = "/opt/spark-apps/exports/product"

TOP_PRODUCTS_EXPORT_PATH = (
    f"{LOCAL_EXPORT_BASE}/top_products"
)

CANCELLATION_RATE_EXPORT_PATH = (
    f"{LOCAL_EXPORT_BASE}/product_cancellation_rate"
)

TOP_PRODUCTS_LIMIT = 1000
MIN_SOLD_QUANTITY = 1
MIN_TOTAL_QUANTITY_RELATED = 20


# 2. TAO SPARK SESSION

spark = (
    SparkSession.builder
    .appName("Retail-Export-Product-For-PowerBI")
    .master("spark://spark-master:7077")

    # Cau hinh Delta Lake
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    # Cau hinh MinIO / Hadoop S3A
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# 3. TAO THU MUC EXPORT

os.makedirs(LOCAL_EXPORT_BASE, exist_ok=True)


# 4. DOC CAC BANG GOLD

df_top_products = (
    spark.read
    .format("delta")
    .load(TOP_PRODUCTS_PATH)
)

df_cancellation_rate = (
    spark.read
    .format("delta")
    .load(CANCELLATION_RATE_PATH)
)


# 5. CHUAN BI TOP PRODUCTS CHO POWER BI

df_top_products_export = (
    df_top_products
    .filter(F.col("revenue_rank") <= TOP_PRODUCTS_LIMIT)
    .orderBy(
        F.asc("revenue_rank"),
        F.asc("quantity_rank"),
        F.asc("stock_code"),
    )
)


# 6. CHUAN BI CANCELLATION RATE CHO POWER BI

df_cancellation_rate_export = (
    df_cancellation_rate
    .filter(F.col("sold_quantity") >= MIN_SOLD_QUANTITY)
    .filter(
        F.col("total_quantity_related")
        >= MIN_TOTAL_QUANTITY_RELATED
    )
    .orderBy(
        F.desc("cancellation_rate"),
        F.desc("cancelled_quantity"),
        F.desc("total_quantity_related"),
        F.asc("stock_code"),
    )
)


# 7. XUAT TOP PRODUCTS CSV

(
    df_top_products_export
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", "")
    .option("emptyValue", "")
    .csv(TOP_PRODUCTS_EXPORT_PATH)
)


# 8. XUAT CANCELLATION RATE CSV

(
    df_cancellation_rate_export
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", "")
    .option("emptyValue", "")
    .csv(CANCELLATION_RATE_EXPORT_PATH)
)

spark.stop()