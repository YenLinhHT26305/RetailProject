from minio import Minio
import pandas as pd

FILE_PATH = "/home/jovyan/data/Online Retail.xlsx"

client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False,
)

# Kiểm tra file có đọc được hay không
df = pd.read_excel(FILE_PATH)
print(df.head())

# Upload nguyên bản lên Bronze
client.fput_object(
    "bronze",
    "Online Retail.xlsx",
    FILE_PATH
)

print("Upload thành công.")