from pyspark.sql import SparkSession


# ============================================================
# 1. MINIO + DATA PATH CONFIGURATION
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

RAW_CSV_PATH = "s3a://bronze/online_retail_II.csv"
BRONZE_DELTA_PATH = "s3a://bronze/delta/online_retail_raw"


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Retail-Bronze-Layer")
    .master("spark://spark-master:7077")

    # Delta Lake
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    # MinIO through Hadoop S3A
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


# ============================================================
# 3. READ RAW CSV
# BRONZE RULE:
# - no filtering
# - no null removal
# - no calculated columns
# - no business transformations
# ============================================================

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "ISO-8859-1")
    .csv(RAW_CSV_PATH)
)

print("\n========== RAW DATA PREVIEW ==========")
df_raw.show(5, truncate=False)

print("\n========== RAW SCHEMA ==========")
df_raw.printSchema()

raw_count = df_raw.count()

print("\n========== RAW ROW COUNT ==========")
print(raw_count)


# ============================================================
# 4. WRITE RAW DATA AS DELTA TABLE
# ============================================================

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .save(BRONZE_DELTA_PATH)
)

print("\n========== BRONZE DELTA CREATED ==========")
print(f"Path: {BRONZE_DELTA_PATH}")


# ============================================================
# 5. VALIDATE DELTA TABLE
# ============================================================

df_bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_DELTA_PATH)
)

bronze_count = df_bronze.count()

print("\n========== DELTA VALIDATION ==========")
print("Raw row count:  ", raw_count)
print("Delta row count:", bronze_count)

if raw_count == bronze_count:
    print("SUCCESS: Bronze Delta Table created correctly.")
else:
    print("WARNING: Raw and Bronze row counts are different.")

spark.stop()