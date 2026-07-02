# Retail Lakehouse Project

Dự án xây dựng hệ thống **Data Lakehouse** để phân tích bộ dữ liệu **Online Retail II** bằng **MinIO, Apache Spark và Delta Lake**.

Mục tiêu của dự án là chuyển dữ liệu giao dịch bán lẻ thô thành các bảng dữ liệu đã được làm sạch, chuẩn hóa và tổng hợp nhằm hỗ trợ doanh nghiệp theo dõi hiệu quả kinh doanh, đánh giá sản phẩm, phân tích thị trường và hiểu hành vi khách hàng.

Hệ thống tập trung vào ba hướng phân tích chính:

- **Product & Inventory Analytics:** xác định sản phẩm bán chạy, sản phẩm tạo doanh thu cao, các mã giao dịch không phải sản phẩm vật lý và tỷ lệ hủy/trả hàng theo sản phẩm.
- **Sales & Market Analytics:** theo dõi doanh thu ròng theo thời gian, số lượng hóa đơn, số lượng bán và hiệu quả kinh doanh theo quốc gia.
- **Customer Analytics & RFM:** đánh giá giá trị khách hàng dựa trên mức độ mua gần đây, tần suất mua và tổng chi tiêu; từ đó hỗ trợ phân khúc khách hàng và xây dựng chiến lược chăm sóc phù hợp.

Dữ liệu được xử lý theo kiến trúc **Medallion Architecture** gồm ba tầng:

- **Bronze:** lưu trữ dữ liệu gốc từ nguồn đầu vào, đảm bảo khả năng truy vết và chạy lại pipeline.
- **Silver:** làm sạch, chuẩn hóa kiểu dữ liệu và tạo các trường cần thiết cho từng hướng phân tích.
- **Gold:** tổng hợp các bảng KPI phục vụ báo cáo, dashboard và hỗ trợ ra quyết định.

---

## Bài toán cần giải quyết

Dữ liệu giao dịch bán lẻ thường chứa nhiều vấn đề như giao dịch hủy, hàng trả, mã giao dịch không phải sản phẩm, dữ liệu thiếu thông tin khách hàng và dữ liệu phân tán theo thời gian.

Dự án xây dựng Data Lakehouse nhằm giải quyết các bài toán phân tích sau:

### 1. Phân tích sản phẩm và hoàn trả

- Sản phẩm nào bán chạy nhất theo số lượng và doanh thu?
- Sản phẩm nào có tỷ lệ hủy hoặc trả hàng cao?
- Những mã giao dịch nào không phải sản phẩm vật lý và cần được tách khỏi phân tích sản phẩm?
- Kết quả phân tích hỗ trợ doanh nghiệp ưu tiên các sản phẩm hiệu quả và xem xét nguyên nhân của các sản phẩm có tỷ lệ hoàn trả cao.

### 2. Phân tích doanh số và thị trường

- Doanh thu ròng thay đổi như thế nào theo tháng?
- Quốc gia nào có doanh thu, số hóa đơn và số lượng bán cao nhất?
- Hàng trả và giao dịch hủy ảnh hưởng như thế nào đến doanh thu thực tế?
- Kết quả phân tích hỗ trợ doanh nghiệp theo dõi xu hướng bán hàng, nhận diện thị trường tiềm năng và đánh giá hiệu quả kinh doanh theo thời gian.

### 3. Phân tích khách hàng

- Khách hàng nào có giá trị chi tiêu cao?
- Khách hàng nào mua thường xuyên hoặc đã lâu không mua?
- Có thể phân khúc khách hàng thành VIP, Loyal Customers, Potential Loyalists, At Risk hoặc Lost Customers bằng mô hình RFM hay không?
- Kết quả phân tích hỗ trợ doanh nghiệp xây dựng chiến lược giữ chân khách hàng, chăm sóc khách hàng trung thành và kích hoạt lại nhóm khách hàng có nguy cơ rời bỏ.

---

## Nguồn dữ liệu

Dự án sử dụng bộ dữ liệu **Online Retail II**, chứa thông tin về các giao dịch bán lẻ trực tuyến.

Dữ liệu đầu vào bao gồm các trường chính:

| Cột | Ý nghĩa |
|---|---|
| `Invoice` | Mã hóa đơn |
| `StockCode` | Mã sản phẩm hoặc mã giao dịch |
| `Description` | Mô tả sản phẩm |
| `Quantity` | Số lượng sản phẩm mua hoặc trả |
| `InvoiceDate` | Ngày và thời gian giao dịch |
| `Price` | Đơn giá |
| `Customer ID` | Mã khách hàng |
| `Country` | Quốc gia của khách hàng |

File dữ liệu gốc được đặt tại:

```text
data/raw/online_retail_II.csv
```

---

## Công nghệ sử dụng

- Docker Desktop
- Docker Compose
- MinIO
- Apache Spark
- Delta Lake
- Python / PySpark
- Jupyter Notebook
- Power BI

---

## Kiến trúc hệ thống

```text
Online Retail II Dataset
        │
        ▼
Python Ingestion
        │
        ▼
MinIO Object Storage
        │
        ▼
Bronze Layer
Raw CSV được lưu thành Delta Table
        │
        ▼
Silver Layer
Dữ liệu được làm sạch và chuẩn hóa
        │
        ▼
Gold Layer
Các bảng KPI phục vụ phân tích
        │
        ▼
Power BI Dashboard
```

---

## Cấu trúc thư mục

```text
RetailProject/
│
├── data/
│   └── raw/
│       └── online_retail_II.csv
│
├── exports/
│   ├── customer/
│   ├── powerbi/
│   ├── product/
│   └── sales/
│
├── ivy-cache/
│
├── minio-data/
│   ├── .minio.sys/
│   ├── bronze/
│   ├── gold/
│   └── silver/
│
├── notebooks/
│
├── scripts/
│   ├── common/
│   ├── customer/
│   ├── exports/
│   ├── product/
│   ├── sales/
│   ├── .ipynb_checkpoints/
│   ├── bronze.py
│   └── ingestion.py
│
├── spark-work/
│
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt

```

---

## Medallion Architecture

### Bronze Layer

Bronze Layer lưu trữ dữ liệu gốc với thay đổi tối thiểu.

```text
CSV gốc
→ MinIO bucket bronze
→ Delta table online_retail_raw
```

Quy tắc xử lý ở Bronze:

* Giữ nguyên toàn bộ dữ liệu nguồn.
* Không xóa giá trị null.
* Không lọc giao dịch hủy.
* Không lọc số lượng âm.
* Không áp dụng quy tắc nghiệp vụ.
* Không tạo cột tính toán mới.

Đường dẫn Delta Bronze:

```text
s3a://bronze/delta/online_retail_raw
```

---

## Silver Layer

Silver Layer làm sạch và chuẩn hóa dữ liệu từ Bronze theo từng hướng phân tích.

### 1. Product & Inventory Analytics

**Phụ trách:** Linh
**Branch:** `feature/product-inventory`

File:

```text
scripts/product/silver_product.py
```

Đọc dữ liệu từ:

```text
s3a://bronze/delta/online_retail_raw/
```

Các cột sử dụng:

```text
Invoice, StockCode, Description, Quantity, Price
```

Xử lý chính:

* Đổi tên cột sang `snake_case`.
* Thay `Description` null hoặc rỗng bằng `Unknown Product`.
* Tạo `is_cancelled` khi mã hóa đơn bắt đầu bằng `C`.
* Tạo `total_amount = quantity × price`.
* Tạo `is_product_code` để phân biệt sản phẩm vật lý với các mã phí hoặc điều chỉnh.
* Gắn cờ các mã đặc biệt: `POST`, `M`, `BANK CHARGES`, `DOT`, `C2`.
* Giữ giao dịch hoàn trả hoặc hủy để phục vụ phân tích tỷ lệ hoàn trả.

Output:

```text
s3a://silver/product/product_clean/
```

---

### 2. Sales & Market Analytics

**Phụ trách:** Thùy
**Branch:** `feature/sales-market`

File:

```text
scripts/sales/silver_sales.py
```

Các cột sử dụng:

```text
Invoice, Quantity, Price, InvoiceDate, Country
```

Xử lý chính:

* Đổi tên cột sang `snake_case`.
* Ép `invoice_date` sang kiểu Timestamp.
* Ép `quantity` sang Integer và `price` sang Double.
* Tạo `is_cancelled` cho các hóa đơn bắt đầu bằng `C`.
* Tạo `total_amount = quantity × price`.
* Giữ giá trị âm của giao dịch hoàn trả để tính doanh thu ròng.

Output:

```text
s3a://silver/sales/sales_clean/
```

---

### 3. Customer Analytics & RFM

**Phụ trách:** Luân
**Branch:** `feature/customer-rfm`

File:

```text
scripts/customer/silver_customer.py
```

Các cột sử dụng:

```text
Invoice, Customer ID, InvoiceDate, Quantity, Price
```

Xử lý chính:

* Đổi tên cột sang `snake_case`.
* Loại bỏ các dòng thiếu `customer_id`.
* Ép `invoice_date` sang Timestamp.
* Ép `quantity` sang Integer và `price` sang Double.
* Tạo `is_cancelled` khi hóa đơn bắt đầu bằng `C`.
* Tạo `total_amount = quantity × price`.
* Giữ các giá trị âm của giao dịch hoàn trả để tổng chi tiêu của khách hàng chính xác.

Output:

```text
s3a://silver/customer/customer_clean/
```

---

## Gold Layer

Gold Layer tổng hợp dữ liệu Silver thành các bảng KPI phục vụ Power BI.

### 1. Product & Inventory Analytics

**Phụ trách:** Linh
**Branch:** `feature/product-inventory`

File:

```text
scripts/product/gold_product.py
```

Các bảng Gold:

| Bảng                      | Nội dung                                                               | Output                                          |
| ------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------- |
| Top Products              | Tổng số lượng bán, tổng doanh thu, xếp hạng theo số lượng và doanh thu | `s3a://gold/product/top_products/`              |
| Product Cancellation Rate | Số lượng bán thành công, số lượng hủy/trả hàng, tỷ lệ hủy/trả hàng     | `s3a://gold/product/product_cancellation_rate/` |

Dữ liệu được nhóm theo:

```text
stock_code, description
```

CSV export cho Power BI:

```text
exports/product/top_products.csv
exports/product/product_cancellation_rate.csv
```

Dashboard Power BI dự kiến:

* Top 10 sản phẩm theo doanh thu.
* Top 10 sản phẩm theo số lượng bán.
* Top sản phẩm có tỷ lệ hoàn trả cao.
* KPI: tổng sản phẩm, tổng số lượng bán, tổng doanh thu.
* Bộ lọc theo `StockCode` hoặc `Description`.

---

### 2. Sales & Market Analytics

**Phụ trách:** Thùy
**Branch:** `feature/sales-market`

File:

```text
scripts/sales/gold_sales.py
```

Các bảng Gold:

| Bảng          | Nội dung                                                          | Output                            |
| ------------- | ----------------------------------------------------------------- | --------------------------------- |
| Monthly Sales | Doanh thu ròng, số hóa đơn, số lượng sản phẩm theo tháng          | `s3a://gold/sales/monthly_sales/` |
| Country Sales | Doanh thu ròng, số hóa đơn duy nhất và số lượng bán theo quốc gia | `s3a://gold/sales/country_sales/` |

CSV export cho Power BI:

```text
exports/sales/monthly_sales.csv
exports/sales/country_sales.csv
```

Dashboard Power BI dự kiến:

* Xu hướng doanh thu ròng theo tháng.
* Top 10 quốc gia theo doanh thu.
* Bản đồ doanh thu theo quốc gia.
* KPI: tổng doanh thu ròng, tổng số hóa đơn, quốc gia có doanh thu cao nhất.
* Bộ lọc theo năm, tháng và quốc gia.

---

### 3. Customer Analytics & RFM

**Phụ trách:** Luân
**Branch:** `feature/customer-rfm`

File:

```text
scripts/customer/gold_customer_rfm.py
```

Các bảng Gold:

| Bảng              | Nội dung                                           | Output                                   |
| ----------------- | -------------------------------------------------- | ---------------------------------------- |
| Customer RFM      | Recency, Frequency và Monetary cho từng khách hàng | `s3a://gold/customer/customer_rfm/`      |
| Customer Segments | Phân khúc khách hàng theo RFM                      | `s3a://gold/customer/customer_segments/` |

Quy tắc RFM:

* **Recency:** Số ngày từ lần mua gần nhất đến ngày lớn nhất trong dataset.
* **Frequency:** Số hóa đơn duy nhất của khách hàng.
* **Monetary:** Tổng `total_amount` của khách hàng.

Các phân khúc dự kiến:

```text
VIP
Loyal Customers
Potential Loyalists
New Customers
At Risk
Lost Customers
```

CSV export cho Power BI:

```text
exports/customer/customer_rfm.csv
exports/customer/customer_segments.csv
```

Dashboard Power BI dự kiến:

* Scatter chart giữa Frequency và Monetary.
* Số lượng khách hàng theo phân khúc RFM.
* KPI: tổng khách hàng, số khách hàng VIP, chi tiêu trung bình mỗi khách hàng.
* Bảng top khách hàng theo Monetary.
* Bộ lọc theo phân khúc khách hàng.

---

## Thứ tự chạy pipeline

```text
1. docker compose up -d
2. ingestion.py
3. bronze.py
4. silver_product.py / silver_sales.py / silver_customer.py
5. gold_product.py / gold_sales.py / gold_customer_rfm.py
6. Export CSV
7. Power BI Dashboard
```

---

## Phân công nhóm

| Thành viên | Branch                      | Phạm vi phụ trách                                                |
| ---------- | --------------------------- | ---------------------------------------------------------------- |
| Linh       | `feature/product-inventory` | Infrastructure, Ingestion, Bronze, Product & Inventory Analytics |
| Thùy       | `feature/sales-market`      | Sales & Market Analytics                                         |
| Luân       | `feature/customer-rfm`      | Customer Analytics và RFM Segmentation                           |

---

## Power BI Dashboard

Dữ liệu Gold được export thành CSV để sử dụng trong Power BI.

Các nhóm dashboard gồm:

* **Product & Inventory Dashboard**
* **Sales & Market Dashboard**
* **Customer Analytics Dashboard**

Mỗi dashboard sử dụng các bảng Gold tương ứng để hiển thị KPI, biểu đồ xu hướng, top sản phẩm/quốc gia/khách hàng và các bộ lọc phân tích.
