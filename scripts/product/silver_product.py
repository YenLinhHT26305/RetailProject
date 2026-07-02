from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. CAU HINH

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

DUONG_DAN_BRONZE_DELTA = "s3a://bronze/delta/online_retail_raw"
DUONG_DAN_SILVER_SAN_PHAM = "s3a://silver/product/product_clean"

MA_KHONG_PHAI_SAN_PHAM = [
    "POST",          # Phi gui buu dien
    "M",             # Dieu chinh thu cong
    "BANK CHARGES",  # Phi ngan hang
    "DOT",
    "C2",
]


# 2. TAO SPARK SESSION

spark = (
    SparkSession.builder
    .appName("Retail-Silver-Product-Layer")
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


# 3. DOC BANG DELTA O BRONZE

df_bronze = (
    spark.read
    .format("delta")
    .load(DUONG_DAN_BRONZE_DELTA)
)


# 4. CHON CAC COT LIEN QUAN DEN SAN PHAM

df_san_pham = df_bronze.select(
    F.col("Invoice").cast("string").alias("invoice"),

    F.upper(
        F.trim(F.col("StockCode").cast("string"))
    ).alias("stock_code"),

    F.col("Description").cast("string").alias("description"),

    F.col("Quantity").cast("integer").alias("quantity"),

    F.col("Price").cast("double").alias("price"),
)


# 5. XU LY SILVER PRODUCT
#
# - Description null/rong -> Unknown Product
# - Invoice bat dau bang C -> giao dich huy hoac tra hang
# - total_amount giu gia tri am de the hien hang tra
# - is_product_code danh dau ma san pham vat ly

df_silver_san_pham = (
    df_san_pham

    # Xu ly mo ta san pham bi thieu hoac rong
    .withColumn(
        "description",
        F.when(
            F.col("description").isNull()
            | (F.trim(F.col("description")) == ""),
            F.lit("Unknown Product"),
        ).otherwise(F.trim(F.col("description"))),
    )

    # Xac dinh giao dich huy hoac tra hang
    .withColumn(
        "is_cancelled",
        F.upper(F.col("invoice")).startswith("C"),
    )

    # Tinh tong tien cua tung dong giao dich
    .withColumn(
        "total_amount",
        F.round(F.col("quantity") * F.col("price"), 2),
    )

    # Danh dau ma co phai la ma san pham vat ly hay khong
    .withColumn(
        "is_product_code",
        F.when(
            F.col("stock_code").isNull()
            | (F.trim(F.col("stock_code")) == "")
            | F.col("stock_code").isin(MA_KHONG_PHAI_SAN_PHAM),
            F.lit(False),
        ).otherwise(F.lit(True)),
    )
)


# 6. GHI BANG DELTA VAO SILVER

(
    df_silver_san_pham.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DUONG_DAN_SILVER_SAN_PHAM)
)


# 7. KET THUC

spark.stop()