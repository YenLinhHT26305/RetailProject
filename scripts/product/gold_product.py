from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. CAU HINH

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

DUONG_DAN_SILVER_SAN_PHAM = "s3a://silver/product/product_clean"

DUONG_DAN_TOP_SAN_PHAM = "s3a://gold/product/top_products"
DUONG_DAN_TY_LE_HUY_SAN_PHAM = (
    "s3a://gold/product/product_cancellation_rate"
)


# 2. TAO SPARK SESSION

spark = (
    SparkSession.builder
    .appName("Retail-Gold-Product-Layer")
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


# 3. DOC BANG DELTA SILVER PRODUCT

df_silver = (
    spark.read
    .format("delta")
    .load(DUONG_DAN_SILVER_SAN_PHAM)
)


# 4. CHI GIU CAC MA SAN PHAM THAT
# Loai bo cac ma dac biet khong phai san pham vat ly nhu:
# POST, M, BANK CHARGES, DOT, C2.

df_chi_san_pham = (
    df_silver
    .filter(F.col("is_product_code") == True)
)


# 5. TAO BANG TOP SAN PHAM
# Chi tinh cac giao dich ban thanh cong:
# - is_cancelled = false
# - quantity > 0
# Cac dong huy/tra hang khong duoc tinh trong bang nay
# vi muc tieu la do hieu qua ban hang thanh cong.

df_ban_hang_thanh_cong = (
    df_chi_san_pham
    .filter(
        (F.col("is_cancelled") == False)
        & (F.col("quantity") > 0)
    )
)

df_top_san_pham = (
    df_ban_hang_thanh_cong
    .groupBy("stock_code", "description")
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.countDistinct("invoice").alias("total_invoices"),
    )
)

# Tao thu hang theo doanh thu va so luong ban
cua_so_doanh_thu = Window.orderBy(
    F.desc("total_revenue"),
    F.desc("total_quantity_sold")
)

cua_so_so_luong = Window.orderBy(
    F.desc("total_quantity_sold"),
    F.desc("total_revenue")
)

df_top_san_pham = (
    df_top_san_pham
    .withColumn(
        "revenue_rank",
        F.dense_rank().over(cua_so_doanh_thu)
    )
    .withColumn(
        "quantity_rank",
        F.dense_rank().over(cua_so_so_luong)
    )
    .orderBy(F.col("revenue_rank"))
)


# 6. TAO BANG TY LE HUY / TRA HANG THEO SAN PHAM
# So luong ban thanh cong:
# Tong quantity khi giao dich khong bi huy va quantity > 0.
# So luong huy/tra:
# Tong gia tri tuyet doi cua quantity khi giao dich bi huy.
# Ty le huy:
# cancelled_quantity /
# (sold_quantity + cancelled_quantity)

df_ty_le_huy_san_pham = (
    df_chi_san_pham
    .groupBy("stock_code", "description")
    .agg(
        F.sum(
            F.when(
                (F.col("is_cancelled") == False)
                & (F.col("quantity") > 0),
                F.col("quantity")
            ).otherwise(F.lit(0))
        ).alias("sold_quantity"),

        F.sum(
            F.when(
                F.col("is_cancelled") == True,
                F.abs(F.col("quantity"))
            ).otherwise(F.lit(0))
        ).alias("cancelled_quantity"),

        F.countDistinct(
            F.when(
                F.col("is_cancelled") == False,
                F.col("invoice")
            )
        ).alias("successful_invoices"),

        F.countDistinct(
            F.when(
                F.col("is_cancelled") == True,
                F.col("invoice")
            )
        ).alias("cancelled_invoices"),
    )
    .withColumn(
        "total_quantity_related",
        F.col("sold_quantity") + F.col("cancelled_quantity")
    )
    .withColumn(
        "cancellation_rate",
        F.when(
            F.col("total_quantity_related") > 0,
            F.round(
                F.col("cancelled_quantity")
                / F.col("total_quantity_related"),
                4
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "cancellation_rate_percent",
        F.round(F.col("cancellation_rate") * 100, 2)
    )
    .orderBy(
        F.desc("cancellation_rate"),
        F.desc("cancelled_quantity")
    )
)


# 7. GHI BANG TOP SAN PHAM VAO GOLD

(
    df_top_san_pham.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DUONG_DAN_TOP_SAN_PHAM)
)


# 8. GHI BANG TY LE HUY VAO GOLD

(
    df_ty_le_huy_san_pham.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DUONG_DAN_TY_LE_HUY_SAN_PHAM)
)


# 9. KET THUC

spark.stop()