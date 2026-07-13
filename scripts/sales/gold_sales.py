from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    month,
    sum,
    countDistinct,
)

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"

SILVER_PATH = "s3a://silver/sales/sales_clean"

MONTHLY_PATH = "s3a://gold/sales/monthly_sales"
COUNTRY_PATH = "s3a://gold/sales/country_sales"
MONTHLY_COUNTRY_PATH = "s3a://gold/sales/monthly_country_sales"

EXPORT_MONTH = "s3a://gold/sales_export/monthly_sales_csv"
EXPORT_COUNTRY = "s3a://gold/sales_export/country_sales_csv"
EXPORT_MONTHLY_COUNTRY = "s3a://gold/sales_export/monthly_country_sales_csv"

spark = (
    SparkSession.builder
    .appName("Gold-Sales")
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

df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

# =====================================================
# MONTHLY SALES
# =====================================================

monthly = (
    df
    .withColumn("year", year("invoice_date"))
    .withColumn("month", month("invoice_date"))
    .groupBy("year", "month")
    .agg(
        sum("total_amount").alias("net_sales"),
        countDistinct("invoice").alias("total_invoice"),
        sum("quantity").alias("total_quantity")
    )
    .orderBy("year", "month")
)

(
    monthly.write
    .format("delta")
    .mode("overwrite")
    .save(MONTHLY_PATH)
)

(
    monthly.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(EXPORT_MONTH)
)

# =====================================================
# COUNTRY SALES
# =====================================================

country = (
    df
    .groupBy("country")
    .agg(
        sum("total_amount").alias("net_sales"),
        countDistinct("invoice").alias("total_invoice"),
        sum("quantity").alias("total_quantity")
    )
    .orderBy("net_sales", ascending=False)
)

(
    country.write
    .format("delta")
    .mode("overwrite")
    .save(COUNTRY_PATH)
)

(
    country.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(EXPORT_COUNTRY)
)

# =====================================================
# MONTHLY COUNTRY SALES
# =====================================================

monthly_country = (
    df
    .withColumn("year", year("invoice_date"))
    .withColumn("month", month("invoice_date"))
    .groupBy(
        "year",
        "month",
        "country"
    )
    .agg(
        sum("total_amount").alias("net_sales"),
        countDistinct("invoice").alias("total_invoice"),
        sum("quantity").alias("total_quantity")
    )
    .orderBy(
        "year",
        "month",
        "country"
    )
)

(
    monthly_country.write
    .format("delta")
    .mode("overwrite")
    .save(MONTHLY_COUNTRY_PATH)
)

(
    monthly_country
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(EXPORT_MONTHLY_COUNTRY)
)

print("Monthly Country Sales Created")

print("Gold Sales Created")

spark.stop()
