from minio import Minio
import pandas as pd

FILE_PATH = "/home/jovyan/data/online_retail_II.csv"

client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False,
)

# Kiểm tra file có đọc được hay không
df = pd.read_csv(FILE_PATH)
print(df.head())

# Upload nguyên bản lên Bronze
client.fput_object(
    "bronze",
    "online_retail_II.csv",
    FILE_PATH
)

print("Upload thành công.")