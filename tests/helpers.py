"""Dữ liệu dựng tay dùng chung cho các test Spark."""
from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.config import EXPECTED_COLUMNS

# Một dòng Bronze hợp lệ; test đổi vài ô để tạo từng tình huống
VALID = {
    "ma_nghiep_vu": "070104", "nhom_xe": "KKD1-1", "hang_xe": "", "ma_dong_xe": "TOY01001",
    "ma_kieu_dang": "", "so_cho_ngoi": "5", "nam_san_xuat": "2020", "gia_tri_dinh_gia": "500000000",
    "ngay_duyet": "2026-06-10", "hieu_luc_tu": "2026-06-12", "hieu_luc_den": "2027-06-12",
    "ky_bao_cao": "2026-06", "ma_dinh_gia": "BV-1", "don_vi_tinh": "VND", "loai_dinh_gia": "used",
    "as_of_month": "2026-06",
}

REF_MODEL_SCHEMA = "ma_dong_xe string, ma_hieu string, hang_xe string, hieu_xe string"
REF_TYPE_SCHEMA = "VEH_TYPE string, mo_ta string, trong_pham_vi string"


def local_df(spark, data, schema):
    """createDataFrame từ list Python, rồi đưa hẳn vào JVM MỘT lần.

    DataFrame dựng từ list Python mặc định chia 8 partition; MỖI action trên nó phải khởi động
    Python worker cho từng partition — trên Windows mất ~6 giây/action. localCheckpoint tính một
    lần, lưu vào bộ nhớ JVM, các action sau không cần Python worker nữa (17 giây -> 0,6 giây).
    """
    return spark.createDataFrame(data, schema).coalesce(1).localCheckpoint()


def bronze_df(spark, rows: list[dict]):
    """Danh sách dict -> DataFrame giống Bronze: 16 cột string + cột kỹ thuật."""
    cols = EXPECTED_COLUMNS + ["_batch_id", "_source_file"]
    schema = StructType([StructField(c, StringType()) for c in cols]
                        + [StructField("_ingest_ts", TimestampType())])
    ts = datetime(2026, 9, 28, 10, 0)
    return local_df(spark, [[r.get(c, "") for c in cols] + [ts] for r in rows], schema)
