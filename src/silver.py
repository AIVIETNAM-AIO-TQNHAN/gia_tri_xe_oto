"""Bronze -> Silver: làm sạch + dựng STAR SCHEMA (docs/ARCHITECTURE.md §4).

                        dim_date (x2: ngày duyệt, ngày hiệu lực)
                            |
    dim_model ---- fact_valuation ---- dim_vehicle_type
                   (1 dòng = 1 hợp đồng định giá)

Các bước:
  1. Chọn lô ACTIVE trong Bronze: mỗi tháng lấy bản nạp mới nhất (_ingest_ts lớn nhất).
  2. Dựng dim_model, dim_vehicle_type từ bảng ref_*.
  3. Parse cột thô (string) sang đúng kiểu, tra dimension lấy khoá thay thế.
  4. Gán rule_id cho dòng vi phạm -> tách: dòng sạch vào fact, dòng lỗi vào quarantine.
  5. Dựng dim_date phủ đủ khoảng ngày của fact.
  6. Đối soát: fact + quarantine = Bronze active. Lệch là dừng job.

Silver ghi FULL REFRESH (mode overwrite) mỗi lần chạy: dữ liệu nhỏ, và chạy lại bao nhiêu lần
cũng ra đúng một kết quả — idempotent mà không cần logic merge.

Chạy:  python -m src.silver
"""
import csv
import logging
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.config import (BRONZE_DIR, MAX_APPROVAL_GAP_DAYS, PARTITION_COL, PRICE_VND_MAX,
                        PRICE_VND_MIN, REF_MODEL_MAP, REF_VEH_TYPE, SEATS_MAX, SEATS_MIN,
                        SILVER_DIR, YEAR_MIN)

log = logging.getLogger("silver")

CORRUPT_COL = "_corrupt_record"

# Thứ tự ưu tiên: một dòng dính nhiều luật chỉ ghi luật ĐẦU TIÊN, để mỗi dòng Bronze ra đúng một
# dòng quarantine và đếm theo rule_id cộng lại bằng tổng số dòng bị tách.
RULES = {
    "CORRUPT_RECORD":     "dòng vỡ cột, không đọc được",
    "DUP_KEY":            "ma_dinh_gia rỗng hoặc trùng (giữ 1 dòng, các dòng thua vào đây)",
    "MISSING_FIELD":      "rỗng ở ma_dong_xe / nam_san_xuat / so_cho_ngoi",
    "DATE_INVALID":       "ngày sai dạng · hieu_luc_den < hieu_luc_tu · hieu_luc_tu lệch ngay_duyet quá 1 năm",
    "OUT_OF_SCOPE":       "nhom_xe không có trong dim_vehicle_type hoặc ngoài phạm vi ô tô con",
    "MODEL_CODE_UNKNOWN": "ma_dong_xe không có trong ref_model_map",
    "VALUE_INVALID":      "nam_san_xuat / so_cho_ngoi / loai_dinh_gia ngoài miền hợp lệ",
    "PRICE_INVALID":      "gia_tri_dinh_gia sai dạng hoặc ngoài [50 triệu, 50 tỷ] VND",
}


# ---- Công cụ nhỏ ------------------------------------------------------------------------
def surrogate_key(*cols: str) -> Column:
    """Khoá thay thế TẤT ĐỊNH = băm khoá tự nhiên.

    Không dùng row_number() hay monotonically_increasing_id(): chúng đánh số theo thứ tự dữ liệu,
    thêm một mã xe mới là cả dãy số đổi -> fact cũ trỏ sai dimension. Băm thì cùng khoá tự nhiên
    luôn ra cùng một số, chạy lại bao nhiêu lần cũng vậy.
    """
    return F.xxhash64(*[F.col(c) for c in cols])


def date_key(col: Column) -> Column:
    """date -> số nguyên yyyymmdd (khoá chuẩn của dim_date). null giữ null."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def blank(col: str) -> Column:
    """true nếu null hoặc chỉ toàn khoảng trắng. Không bao giờ trả null."""
    return F.coalesce(F.trim(F.col(col)) == "", F.lit(True))


def ok(cond: Column) -> Column:
    """Điều kiện hợp lệ; null (do đầu vào null) coi là KHÔNG hợp lệ."""
    return F.coalesce(cond, F.lit(False))


def read_ref_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Bảng ref_* nhỏ -> đọc bằng Python (utf-8-sig bỏ BOM), trim mọi giá trị, mọi cột string."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    cols = list(rows[0].keys())
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    df = spark.createDataFrame([[(r[c] or "").strip() for c in cols] for r in rows], schema)
    # DataFrame dựng từ list Python: mỗi action phải khởi động Python worker cho từng partition
    # (chậm trên Windows). Đưa vào JVM một lần — các join/count sau không cần Python nữa.
    return df.coalesce(1).localCheckpoint()


def assert_unique(df: DataFrame, key: str, name: str) -> None:
    """Khoá dimension phải duy nhất, nếu không LEFT JOIN sẽ NHÂN BẢN dòng fact một cách im lặng."""
    dup = df.groupBy(key).count().filter("count > 1").count()
    if dup:
        raise ValueError(f"{name}: {dup} giá trị {key} bị trùng")


# ---- 1. Lô active -----------------------------------------------------------------------
def active_bronze(bronze: DataFrame) -> DataFrame:
    """Mỗi as_of_month chỉ giữ bản nạp mới nhất.

    Bronze là append-only: bên cung cấp gửi lại lô tháng 6 (batch_id mới) thì Bronze có HAI bản
    tháng 6. Silver phải chọn một — bản mới nhất thắng; bản cũ vẫn nằm ở Bronze để truy vết.
    "Mới nhất" = _ingest_ts lớn nhất; bằng nhau (nạp trong cùng một giây) -> _batch_id lớn hơn.
    """
    w = Window.partitionBy(PARTITION_COL)
    latest = F.max(F.struct("_ingest_ts", "_batch_id")).over(w)
    return (bronze.withColumn("_latest", latest)
                  .filter((F.col("_ingest_ts") == F.col("_latest._ingest_ts"))
                          & (F.col("_batch_id") == F.col("_latest._batch_id")))
                  .drop("_latest"))


# ---- 2. Dimension từ ref_* --------------------------------------------------------------
def build_dim_model(ref_model_map: DataFrame) -> DataFrame:
    """Grain: 1 dòng = 1 mã dòng xe nội bộ. Phân cấp: hang_xe > hieu_xe > ma_hieu > ma_dong_xe."""
    assert_unique(ref_model_map, "ma_dong_xe", "ref_model_map")
    return ref_model_map.select(surrogate_key("ma_dong_xe").alias("model_key"),
                                "ma_dong_xe", "ma_hieu", "hang_xe", "hieu_xe")


def build_dim_vehicle_type(ref_veh_type: DataFrame) -> DataFrame:
    """Grain: 1 dòng = 1 mã loại xe. Thêm muc_dich_su_dung để roll-up (gộp nhóm) khi phân tích."""
    assert_unique(ref_veh_type, "VEH_TYPE", "ref_veh_type_scope")
    code = F.col("VEH_TYPE")
    muc_dich = (F.when(code.startswith("KKD"), "Không kinh doanh")
                 .when(code.startswith("XEKD"), "Kinh doanh vận tải")
                 .when(code.startswith("TAXI"), "Taxi")
                 .when(code.startswith("XTTL"), "Cho thuê tự lái")
                 .otherwise("Khác"))
    return ref_veh_type.select(surrogate_key("VEH_TYPE").alias("vehicle_type_key"),
                               code.alias("nhom_xe"), "mo_ta",
                               muc_dich.alias("muc_dich_su_dung"),
                               (F.col("trong_pham_vi") == "1").alias("trong_pham_vi"))


def build_dim_date(spark: SparkSession, fact: DataFrame) -> DataFrame:
    """Grain: 1 dòng = 1 ngày. Phủ trọn các NĂM có trong fact (suy từ dữ liệu, không từ ngày chạy)."""
    bounds = fact.select(
        F.least(F.min("ngay_duyet"), F.min("hieu_luc_tu")).alias("lo"),
        F.greatest(F.max("ngay_duyet"), F.max("hieu_luc_tu")).alias("hi")).first()
    start, end = f"{bounds.lo.year}-01-01", f"{bounds.hi.year}-12-31"
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS ngay")
    return days.select(
        date_key(F.col("ngay")).alias("date_key"),
        "ngay",
        F.year("ngay").alias("nam"),
        F.quarter("ngay").alias("quy"),
        F.month("ngay").alias("thang"),
        F.date_format("ngay", "yyyy-MM").alias("nam_thang"),
        F.dayofweek("ngay").alias("thu_trong_tuan"),              # 1 = Chủ nhật ... 7 = Thứ bảy
        F.dayofweek("ngay").isin(1, 7).alias("la_cuoi_tuan"))


# ---- 3 + 4. Parse, tra dimension, gán luật ----------------------------------------------
def parse_and_lookup(bronze: DataFrame, dim_model: DataFrame, dim_vehicle_type: DataFrame) -> DataFrame:
    """Thêm cột đã parse (tiền tố p_) và khoá dimension. Cột thô GIỮ NGUYÊN cho quarantine.

    Tra dimension luôn là LEFT JOIN: dòng không tra được vẫn còn (khoá null), để luật phía sau
    đưa nó vào quarantine. INNER JOIN sẽ làm dòng đó biến mất không dấu vết.
    """
    t = lambda c: F.trim(F.col(c))
    df = (bronze
          .withColumn("p_nam_san_xuat", t("nam_san_xuat").cast("int"))
          .withColumn("p_so_cho_ngoi", t("so_cho_ngoi").cast("int"))
          .withColumn("p_gia_vnd", t("gia_tri_dinh_gia").cast("double"))
          .withColumn("p_ngay_duyet", F.to_date(t("ngay_duyet"), "yyyy-MM-dd"))
          .withColumn("p_hieu_luc_tu", F.to_date(t("hieu_luc_tu"), "yyyy-MM-dd"))
          .withColumn("p_hieu_luc_den", F.to_date(t("hieu_luc_den"), "yyyy-MM-dd"))
          .withColumn("_ma_dong_xe", t("ma_dong_xe"))
          .withColumn("_nhom_xe", t("nhom_xe")))

    models = dim_model.select(F.col("ma_dong_xe").alias("_ma_dong_xe"), "model_key")
    types = dim_vehicle_type.select(F.col("nhom_xe").alias("_nhom_xe"), "vehicle_type_key",
                                    F.col("trong_pham_vi").alias("_trong_pham_vi"))
    return (df.join(F.broadcast(models), "_ma_dong_xe", "left")
              .join(F.broadcast(types), "_nhom_xe", "left")
              .drop("_ma_dong_xe", "_nhom_xe"))


def rule_conditions(df: DataFrame) -> dict[str, Column]:
    """{rule_id: Column boolean, true = VI PHẠM}. Mỗi điều kiện không bao giờ null."""
    # DUP_KEY: xếp hạng trong nhóm cùng ma_dinh_gia, dòng hạng 1 thắng. Thứ tự tất định (không phụ
    # thuộc thứ tự đọc file): hiệu lực muộn nhất -> giá lớn hơn -> batch_id lớn hơn.
    w = Window.partitionBy("ma_dinh_gia").orderBy(
        F.col("p_hieu_luc_tu").desc_nulls_last(), F.col("p_gia_vnd").desc_nulls_last(),
        F.col("_batch_id").desc())
    rank = F.row_number().over(w)
    year_max = F.year("p_ngay_duyet") + 1
    return {
        "CORRUPT_RECORD": F.col(CORRUPT_COL).isNotNull() if CORRUPT_COL in df.columns else F.lit(False),
        "DUP_KEY": blank("ma_dinh_gia") | (rank > 1),
        "MISSING_FIELD": blank("ma_dong_xe") | blank("nam_san_xuat") | blank("so_cho_ngoi"),
        "DATE_INVALID": (F.col("p_ngay_duyet").isNull() | F.col("p_hieu_luc_tu").isNull()
                         | (~blank("hieu_luc_den") & F.col("p_hieu_luc_den").isNull())
                         | ok(F.col("p_hieu_luc_den") < F.col("p_hieu_luc_tu"))
                         | ok(F.abs(F.datediff("p_hieu_luc_tu", "p_ngay_duyet")) > MAX_APPROVAL_GAP_DAYS)),
        "OUT_OF_SCOPE": ~ok(F.col("_trong_pham_vi")),
        "MODEL_CODE_UNKNOWN": F.col("model_key").isNull(),
        "VALUE_INVALID": (~ok(F.col("p_nam_san_xuat").between(YEAR_MIN, year_max))
                          | ~ok(F.col("p_so_cho_ngoi").between(SEATS_MIN, SEATS_MAX))
                          | ~ok(F.trim("loai_dinh_gia").isin("new", "used"))),
        "PRICE_INVALID": ~ok(F.col("p_gia_vnd").between(PRICE_VND_MIN, PRICE_VND_MAX)),
    }


def assign_rule_id(df: DataFrame) -> DataFrame:
    """Cột rule_id = luật ĐẦU TIÊN (theo thứ tự RULES) mà dòng vi phạm; null = dòng sạch."""
    conds = rule_conditions(df)
    expr = F.lit(None).cast(StringType())
    for rule in reversed(list(RULES)):          # dựng từ cuối lên -> luật đầu danh sách thắng
        expr = F.when(conds[rule], F.lit(rule)).otherwise(expr)
    return df.withColumn("rule_id", expr)


def transform(bronze: DataFrame, dim_model: DataFrame,
              dim_vehicle_type: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze active -> (fact_valuation, quarantine). Hàm thuần: không đọc/ghi đĩa -> test được."""
    raw_cols = list(bronze.columns)
    staged = assign_rule_id(parse_and_lookup(bronze, dim_model, dim_vehicle_type))

    clean = staged.filter(F.col("rule_id").isNull())
    fact = clean.select(
        "ma_dinh_gia",                                                   # degenerate dimension
        "model_key",
        "vehicle_type_key",
        date_key(F.col("p_ngay_duyet")).alias("ngay_duyet_key"),         # role-playing: dim_date
        date_key(F.col("p_hieu_luc_tu")).alias("hieu_luc_tu_key"),       # role-playing: dim_date
        F.col("p_nam_san_xuat").alias("nam_san_xuat"),
        F.col("p_so_cho_ngoi").alias("so_cho_ngoi"),
        F.trim("loai_dinh_gia").alias("loai_dinh_gia"),
        (F.year("p_ngay_duyet") - F.col("p_nam_san_xuat")).alias("tuoi_xe"),
        F.round(F.col("p_gia_vnd") / 1e6, 3).alias("gia_tri_trieu_vnd"),  # MEASURE, đơn vị triệu VND
        F.col("p_ngay_duyet").alias("ngay_duyet"),
        F.col("p_hieu_luc_tu").alias("hieu_luc_tu"),
        F.col("p_hieu_luc_den").alias("hieu_luc_den"),
        PARTITION_COL,
        "_batch_id",                                                     # lineage về Bronze
    )
    quarantine = staged.filter(F.col("rule_id").isNotNull()).select(*raw_cols, "rule_id")
    return fact, quarantine


# ---- Chạy ---------------------------------------------------------------------------------
def run(spark: SparkSession, bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR,
        ref_model_map: Path = REF_MODEL_MAP, ref_veh_type: Path = REF_VEH_TYPE) -> dict[str, int]:
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Chưa có Bronze ở {bronze_dir} — chạy python -m src.bronze trước")

    bronze = active_bronze(spark.read.parquet(bronze_dir.as_posix())).cache()
    dim_model = build_dim_model(read_ref_csv(spark, ref_model_map)).cache()
    dim_vehicle_type = build_dim_vehicle_type(read_ref_csv(spark, ref_veh_type)).cache()

    fact, quarantine = transform(bronze, dim_model, dim_vehicle_type)
    fact, quarantine = fact.cache(), quarantine.cache()
    dim_date = build_dim_date(spark, fact)

    n_bronze, n_fact, n_quar = bronze.count(), fact.count(), quarantine.count()
    if n_fact + n_quar != n_bronze:                     # không dòng nào được phép biến mất
        raise RuntimeError(f"Đối soát lệch: fact {n_fact} + quarantine {n_quar} ≠ bronze {n_bronze}")

    def write(df: DataFrame, name: str, partitioned: bool = False) -> None:
        path = (silver_dir / name).as_posix()
        if partitioned:                                  # fact/quarantine: 1 thư mục / tháng
            # repartition trước: mỗi tháng nằm gọn trong 1 task -> 1 file/tháng, ít writer mở cùng lúc
            (df.repartition(PARTITION_COL)
               .write.mode("overwrite").partitionBy(PARTITION_COL).parquet(path))
        else:                                            # dimension nhỏ: 1 file
            df.coalesce(1).write.mode("overwrite").parquet(path)

    write(dim_model, "dim_model")
    write(dim_vehicle_type, "dim_vehicle_type")
    write(dim_date, "dim_date")
    write(fact, "fact_valuation", partitioned=True)
    write(quarantine, "quarantine", partitioned=True)

    log.info("Silver: bronze active %d = fact %d + quarantine %d (%.2f%%)",
             n_bronze, n_fact, n_quar, 100 * n_quar / max(n_bronze, 1))
    for row in quarantine.groupBy("rule_id").count().orderBy(F.desc("count")).collect():
        log.info("        %-20s %6d", row.rule_id, row["count"])
    log.info("        dim_model %d · dim_vehicle_type %d · dim_date %d",
             dim_model.count(), dim_vehicle_type.count(), dim_date.count())
    return {"bronze": n_bronze, "fact": n_fact, "quarantine": n_quar}


def main() -> int:
    from src.run_pipeline import setup_logging
    from src.spark import get_spark
    setup_logging()
    spark = get_spark("silver")
    try:
        run(spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
