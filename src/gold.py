"""Silver (star schema) -> Gold (data mart) — README §4, §5.3.

Gold KHÔNG làm sạch thêm: mọi quy tắc làm sạch nằm tại Silver. Gold chỉ JOIN fact với dimension
rồi GROUP BY để trả lời câu hỏi nghiệp vụ. Viết bằng Spark SQL để thể hiện rõ dạng truy vấn star schema:

    SELECT <thuộc tính dimension>, <hàm tổng hợp (aggregate) trên measure>
    FROM   fact JOIN dim_a ON fact.a_key = dim_a.a_key JOIN dim_b ...
    GROUP BY <thuộc tính dimension>

Bốn data mart:
    mart_vehicle_price   giá trung vị (median) xe mới / xe cũ theo (hãng, dòng, đời, số chỗ) + tỷ lệ giá trị còn lại
    mart_monthly_trend   số hợp đồng và giá trung vị theo tháng, hãng, mục đích sử dụng
    mart_data_quality    mỗi tháng: bao nhiêu record vào fact, bao nhiêu vào quarantine, do vi phạm rule nào
    mart_ml_training     feature table cho mô hình ML: grain 1 record = 1 hợp đồng, KHÔNG tổng hợp (README §5.3)

Ba mart phân tích được ghi đè mỗi lần chạy. mart_ml_training thì KHÔNG: mỗi cặp (phiên bản ref, mốc chia)
là một BẢN CHỤP riêng, để mô hình đã huấn luyện luôn tìm lại được đúng tập dữ liệu của nó:

    gold/mart_ml_training/ref_version=2026-07/train_end_month=2026-03/part-*.parquet + _run_info.json

Chạy:  python -m src.gold
"""
import json
import logging
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import GOLD_DIR, ML_TRAIN_END_MONTH, REF_MODEL_MAP_NAME, ROOT, RUN_INFO_FILE, SILVER_DIR
from src.silver import RULES

log = logging.getLogger("gold")

SILVER_TABLES = ["fact_valuation", "dim_model", "dim_vehicle_type", "dim_date", "quarantine"]

MART_VEHICLE_PRICE = """
WITH g AS (
    SELECT m.hang_xe, m.hieu_xe, f.nam_san_xuat, f.so_cho_ngoi,
           COUNT(*)                                                             AS so_hop_dong,
           COUNT_IF(f.loai_dinh_gia = 'new')                                    AS so_hd_xe_moi,
           COUNT_IF(f.loai_dinh_gia = 'used')                                   AS so_hd_xe_cu,
           MEDIAN(CASE WHEN f.loai_dinh_gia = 'new'  THEN f.gia_tri_trieu_vnd END) AS gia_xe_moi_trung_vi,
           MEDIAN(CASE WHEN f.loai_dinh_gia = 'used' THEN f.gia_tri_trieu_vnd END) AS gia_xe_cu_trung_vi,
           MIN(d.nam_thang)                                                     AS thang_dau_tien,
           MAX(d.nam_thang)                                                     AS thang_gan_nhat
    FROM fact_valuation f
    JOIN dim_model m ON f.model_key      = m.model_key
    JOIN dim_date  d ON f.ngay_duyet_key = d.date_key
    GROUP BY m.hang_xe, m.hieu_xe, f.nam_san_xuat, f.so_cho_ngoi
)
SELECT hang_xe, hieu_xe, nam_san_xuat, so_cho_ngoi,
       so_hop_dong, so_hd_xe_moi, so_hd_xe_cu,
       ROUND(gia_xe_moi_trung_vi, 1)                          AS gia_xe_moi_trung_vi,
       ROUND(gia_xe_cu_trung_vi, 1)                           AS gia_xe_cu_trung_vi,
       ROUND(gia_xe_cu_trung_vi / gia_xe_moi_trung_vi, 3)     AS ty_le_con_lai,   -- null nếu thiếu 1 phía
       thang_dau_tien, thang_gan_nhat
FROM g
ORDER BY hang_xe, hieu_xe, nam_san_xuat, so_cho_ngoi
"""

MART_MONTHLY_TREND = """
SELECT d.nam_thang, m.hang_xe, v.muc_dich_su_dung, f.loai_dinh_gia,
       COUNT(*)                              AS so_hop_dong,
       ROUND(MEDIAN(f.gia_tri_trieu_vnd), 1) AS gia_trung_vi,
       ROUND(AVG(f.tuoi_xe), 1)              AS tuoi_xe_trung_binh
FROM fact_valuation f
JOIN dim_date         d ON f.ngay_duyet_key   = d.date_key
JOIN dim_model        m ON f.model_key        = m.model_key
JOIN dim_vehicle_type v ON f.vehicle_type_key = v.vehicle_type_key
GROUP BY d.nam_thang, m.hang_xe, v.muc_dich_su_dung, f.loai_dinh_gia
ORDER BY d.nam_thang, m.hang_xe, v.muc_dich_su_dung, f.loai_dinh_gia
"""


def mart_data_quality_sql() -> str:
    """Một cột đếm cho mỗi rule_id — danh sách rule lấy từ silver.RULES (single source of truth),
    không khai báo thủ công ở hai nơi."""
    per_rule = ",\n       ".join(f"COUNT_IF(rule_id = '{r}') AS {r.lower()}" for r in RULES)
    return f"""
WITH all_rows AS (
    SELECT as_of_month, CAST(NULL AS STRING) AS rule_id FROM fact_valuation
    UNION ALL
    SELECT as_of_month, rule_id FROM quarantine
)
SELECT as_of_month,
       COUNT(*)                                                     AS so_dong_bronze,
       COUNT_IF(rule_id IS NULL)                                    AS so_dong_fact,
       COUNT_IF(rule_id IS NOT NULL)                                AS so_dong_quarantine,
       ROUND(100 * COUNT_IF(rule_id IS NOT NULL) / COUNT(*), 2)     AS ty_le_quarantine_pct,
       {per_rule}
FROM all_rows
GROUP BY as_of_month
ORDER BY as_of_month
"""


# ---- mart_ml_training: output data contract (README §5.3) ------------------------------
# (cột, kiểu Spark, vai trò). Thứ tự cột là một phần của contract.
ML_TRAINING_CONTRACT = [
    ("ma_dinh_gia",       "string", "id"),        # truy vết (lineage) về fact — KHÔNG đưa vào mô hình
    ("hang_xe",           "string", "feature"),
    ("hieu_xe",           "string", "feature"),
    ("nam_san_xuat",      "int",    "feature"),
    ("tuoi_xe",           "int",    "feature"),
    ("so_cho_ngoi",       "int",    "feature"),
    ("muc_dich_su_dung",  "string", "feature"),
    ("loai_dinh_gia",     "string", "feature"),   # derived attribute, thông tin gần trùng với tuoi_xe
    ("thang_duyet",       "string", "feature"),
    ("gia_tri_trieu_vnd", "double", "target"),
    ("split",             "string", "split"),
]


def mart_ml_training_sql(train_end_month: str = ML_TRAIN_END_MONTH) -> str:
    """Grain = 1 hợp đồng, cùng grain với fact — KHÔNG GROUP BY. Mô hình học từ từng quan sát,
    không từ giá trị trung vị.

    Chỉ lấy các cột có sẵn trên CHÍNH record đó (không dùng aggregate feature kiểu "giá trung vị của
    dòng xe"): tính tổng hợp trên toàn bộ dữ liệu sẽ khiến giá của tập test lọt vào feature của tập
    train (data leakage).
    Chia split theo THÁNG DUYỆT, không ngẫu nhiên: mô hình phải được đánh giá trên các tháng chưa
    từng thấy, đúng như khi triển khai thực tế; và re-run bao nhiêu lần cũng cho cùng một cách chia.
    INNER JOIN ở đây an toàn: Silver bảo đảm mọi khoá ngoại (foreign key) của fact đều tồn tại trong
    dimension — test_one_row_per_contract kiểm chứng lại điều đó bằng số record.
    """
    return f"""
SELECT f.ma_dinh_gia,
       m.hang_xe,
       m.hieu_xe,
       f.nam_san_xuat,
       f.tuoi_xe,
       f.so_cho_ngoi,
       v.muc_dich_su_dung,
       f.loai_dinh_gia,
       d.nam_thang                                                     AS thang_duyet,
       f.gia_tri_trieu_vnd,
       CASE WHEN d.nam_thang <= '{train_end_month}' THEN 'train' ELSE 'test' END AS split
FROM fact_valuation f
JOIN dim_model        m ON f.model_key        = m.model_key
JOIN dim_vehicle_type v ON f.vehicle_type_key = v.vehicle_type_key
JOIN dim_date         d ON f.ngay_duyet_key   = d.date_key
ORDER BY f.ma_dinh_gia
"""


def check_ml_contract(df: DataFrame) -> None:
    """Output phải khớp contract: đúng cột, đúng thứ tự, đúng kiểu, không null. Vi phạm thì DỪNG
    (fail fast) — thà không ghi bảng còn hơn ghi một bảng mà code huấn luyện phía sau (downstream)
    đọc sai một cách âm thầm."""
    expected = [(c, t) for c, t, _ in ML_TRAINING_CONTRACT]
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    if actual != expected:
        raise ValueError(f"mart_ml_training sai schema:\n  cần {expected}\n  có  {actual}")
    nulls = df.select([F.count_if(F.col(c).isNull()).alias(c) for c, _ in expected]).first().asDict()
    bad = {c: n for c, n in nulls.items() if n}
    if bad:
        raise ValueError(f"mart_ml_training có null: {bad}")


def register_silver(spark: SparkSession, silver_dir: Path) -> None:
    """Đăng ký mỗi bảng Silver thành một temp view để truy vấn bằng Spark SQL."""
    for name in SILVER_TABLES:
        path = silver_dir / name
        if not path.exists():
            raise FileNotFoundError(f"Chưa có {path} — chạy python -m src.silver trước")
        spark.read.parquet(path.as_posix()).createOrReplaceTempView(name)


def read_silver_run_info(silver_dir: Path) -> dict:
    """_run_info.json do silver.run ghi: Silver hiện tại được dựng từ phiên bản ref nào, batch nào.
    Silver cũ (trước khi có file này) -> refs rỗng, bản chụp ML mang nhãn 'unknown'."""
    path = silver_dir / RUN_INFO_FILE
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"refs": {}}


def ml_snapshot_dir(gold_dir: Path, ref_version: str, train_end_month: str) -> Path:
    return gold_dir / "mart_ml_training" / f"ref_version={ref_version}" / f"train_end_month={train_end_month}"


def run(spark: SparkSession, silver_dir: Path = SILVER_DIR, gold_dir: Path = GOLD_DIR,
        csv_dir: Path | None = ROOT / "output", train_end_month: str = ML_TRAIN_END_MONTH) -> dict:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", train_end_month):
        raise ValueError(f"train_end_month phải có dạng YYYY-MM, nhận được {train_end_month!r}")
    register_silver(spark, silver_dir)
    silver_info = read_silver_run_info(silver_dir)
    ref_version = silver_info["refs"].get(REF_MODEL_MAP_NAME, {}).get("version", "unknown")
    snapshot = ml_snapshot_dir(gold_dir, ref_version, train_end_month)

    marts: dict[str, DataFrame] = {
        "mart_vehicle_price": spark.sql(MART_VEHICLE_PRICE),
        "mart_monthly_trend": spark.sql(MART_MONTHLY_TREND),
        "mart_data_quality": spark.sql(mart_data_quality_sql()),
        "mart_ml_training": spark.sql(mart_ml_training_sql(train_end_month)),
    }
    counts: dict = {}
    for name, df in marts.items():
        df = df.coalesce(1).cache()
        if name == "mart_ml_training":
            check_ml_contract(df)                 # kiểm tra contract TRƯỚC khi ghi
            # Ghi đè CHỈ bản chụp cùng (ref, mốc chia) — chạy lại vẫn idempotent; bản chụp khác giữ nguyên
            df.write.mode("overwrite").parquet(snapshot.as_posix())
        else:
            df.write.mode("overwrite").parquet((gold_dir / name).as_posix())
        counts[name] = df.count()
        if csv_dir is not None:                   # bản CSV để mở bằng Excel khi viết báo cáo
            csv_dir.mkdir(parents=True, exist_ok=True)
            df.toPandas().to_csv(csv_dir / f"{name}.csv", index=False, encoding="utf-8-sig")
        log.info("Gold: %-20s %6d record", name, counts[name])
        if name == "mart_ml_training":
            by_split = {r.split: r["count"] for r in df.groupBy("split").count().collect()}
            counts["ml_train"], counts["ml_test"] = by_split.get("train", 0), by_split.get("test", 0)
            log.info("      %-20s train %d · test %d (mốc chia %s)", "", counts["ml_train"],
                     counts["ml_test"], train_end_month)
            # Thẻ của bản chụp: dựng từ ref nào, batch nào, mốc chia nào — đọc được mà không cần Spark
            (snapshot / RUN_INFO_FILE).write_text(json.dumps(
                {**silver_info, "train_end_month": train_end_month,
                 "ml_rows": {"total": counts[name], "train": counts["ml_train"], "test": counts["ml_test"]}},
                ensure_ascii=False, indent=2), encoding="utf-8")
            log.info("      %-20s bản chụp: %s", "", snapshot.relative_to(gold_dir).as_posix())
    counts["ml_snapshot"] = snapshot.relative_to(gold_dir).as_posix()
    return counts


def main() -> int:
    from src.run_pipeline import setup_logging
    from src.spark import get_spark
    setup_logging()
    spark = get_spark("gold")
    try:
        run(spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
