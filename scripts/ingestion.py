from minio import Minio
import pandas as pd
import os

FILE_PATH = "/home/jovyan/data/raw/online_retail_II.csv"

BUCKET_NAME = "bronze"
OBJECT_NAME = "raw/online_retail_II.csv"

client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False,
)

# Kiem tra xem file CSV nguon co ton tai hay khong
if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"Khong tim thay file: {FILE_PATH}")

# Doc thu de kiem tra file co hop le, khong thay doi file nguon
df = pd.read_csv(FILE_PATH, encoding="ISO-8859-1")
print(df.head())
print(f"So dong: {len(df)}")
print(f"Cac cot: {list(df.columns)}")

# Tao bucket neu bucket chua ton tai tren MinIO
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# Day file tu thu muc local len MinIO bucket
client.fput_object(
    bucket_name=BUCKET_NAME,
    object_name=OBJECT_NAME,
    file_path=FILE_PATH,
)

print(f"Upload thanh cong: s3://{BUCKET_NAME}/{OBJECT_NAME}")