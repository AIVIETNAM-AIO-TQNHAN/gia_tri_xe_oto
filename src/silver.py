"""Bronze -> Silver: làm sạch dữ liệu + xây dựng STAR SCHEMA (README §4, §5.2).

                        dim_date (role-playing x2: ngày duyệt, ngày hiệu lực)
                            |
    dim_model ---- fact_valuation ---- dim_vehicle_type
                   (grain: 1 record = 1 hợp đồng định giá)

Các bước:
  1. Chọn batch ACTIVE trong Bronze: mỗi tháng lấy lần ingest mới nhất (_ingest_ts lớn nhất).
  2. Xây dựng dim_model, dim_vehicle_type từ reference data ref_*.
  3. Parse cột thô (string) sang đúng kiểu, lookup dimension để lấy surrogate key.
  4. Gán rule_id cho record vi phạm -> phân tách: record hợp lệ vào fact, record lỗi vào quarantine.
  5. Xây dựng dim_date bao phủ toàn bộ khoảng ngày của fact.
  6. Đối soát (reconciliation): fact + quarantine = Bronze active. Lệch thì dừng job (fail fast).

Silver được ghi theo cơ chế FULL REFRESH (mode overwrite) ở mỗi lần thực thi: dữ liệu nhỏ, và re-run
bao nhiêu lần cũng cho cùng một kết quả — idempotent mà không cần logic merge.

Chạy:  python -m src.silver
"""
import csv
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src import reference
from src.config import (BRONZE_DIR, MAX_APPROVAL_GAP_DAYS, PARTITION_COL, PRICE_VND_MAX,
                        PRICE_VND_MIN, REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME, RUN_INFO_FILE,
                        SEATS_MAX, SEATS_MIN, SILVER_DIR, YEAR_MIN)

log = logging.getLogger("silver")

CORRUPT_COL = "_corrupt_record"

# Thứ tự ưu tiên: một record vi phạm nhiều rule chỉ ghi nhận rule ĐẦU TIÊN, để mỗi record Bronze sinh
# ra đúng một record quarantine và tổng số đếm theo rule_id bằng đúng tổng số record bị tách ra.
# Thứ tự: lỗi cấu trúc (structural) -> lỗi tham chiếu (referential) -> lỗi miền giá trị (domain).
RULES = {
    "CORRUPT_RECORD":     "record vỡ cột, không parse được",
    "DUP_KEY":            "ma_dinh_gia rỗng hoặc trùng lặp (giữ 1 record, các record còn lại vào đây)",
    "MISSING_FIELD":      "rỗng tại ma_dong_xe / nam_san_xuat / so_cho_ngoi",
    "DATE_INVALID":       "ngày sai định dạng · hieu_luc_den < hieu_luc_tu · hieu_luc_tu lệch ngay_duyet quá 1 năm",
    "OUT_OF_SCOPE":       "nhom_xe không tồn tại trong dim_vehicle_type hoặc ngoài phạm vi ô tô chở người",
    "MODEL_CODE_UNKNOWN": "ma_dong_xe không tồn tại trong ref_model_map",
    "VALUE_INVALID":      "nam_san_xuat / so_cho_ngoi / loai_dinh_gia ngoài miền giá trị (domain) hợp lệ",
    "PRICE_INVALID":      "gia_tri_dinh_gia sai định dạng hoặc ngoài [50 triệu, 50 tỷ] VND",
}


# ---- Hàm tiện ích (helper) --------------------------------------------------------------
def surrogate_key(*cols: str) -> Column:
    """Surrogate key TẤT ĐỊNH (deterministic) = băm natural key.

    Không dùng row_number() hay monotonically_increasing_id(): chúng đánh số theo thứ tự dữ liệu,
    nên chỉ cần bổ sung một mã xe mới là cả dãy khoá thay đổi -> fact cũ tham chiếu sai dimension.
    Hàm băm luôn cho cùng một giá trị với cùng một natural key, re-run bao nhiêu lần cũng vậy.
    """
    return F.xxhash64(*[F.col(c) for c in cols])


def date_key(col: Column) -> Column:
    """date -> số nguyên yyyymmdd (khoá chuẩn của dim_date). Giá trị null được giữ nguyên là null."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def blank(col: str) -> Column:
    """true nếu null hoặc chỉ toàn khoảng trắng. Không bao giờ trả null."""
    return F.coalesce(F.trim(F.col(col)) == "", F.lit(True))


def ok(cond: Column) -> Column:
    """Điều kiện hợp lệ; kết quả null (do đầu vào null) được xem là KHÔNG hợp lệ."""
    return F.coalesce(cond, F.lit(False))


def read_ref_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Reference data ref_* có kích thước nhỏ -> đọc bằng Python (utf-8-sig loại bỏ BOM), trim mọi
    giá trị, mọi cột kiểu string."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    cols = list(rows[0].keys())
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    df = spark.createDataFrame([[(r[c] or "").strip() for c in cols] for r in rows], schema)
    # DataFrame dựng từ list Python: mỗi action phải khởi động Python worker cho từng partition
    # (chậm trên Windows). Đưa vào JVM một lần — các join/count sau không cần Python nữa.
    return df.coalesce(1).localCheckpoint()


def assert_unique(df: DataFrame, key: str, name: str) -> None:
    """Khoá của dimension phải duy nhất (unique), nếu không LEFT JOIN sẽ âm thầm NHÂN BẢN record fact."""
    dup = df.groupBy(key).count().filter("count > 1").count()
    if dup:
        raise ValueError(f"{name}: {dup} giá trị {key} bị trùng")


# ---- 1. Batch active --------------------------------------------------------------------
def active_bronze(bronze: DataFrame) -> DataFrame:
    """Mỗi as_of_month chỉ giữ lại lần ingest mới nhất (Bronze active).

    Bronze là append-only: khi bên cung cấp gửi lại batch tháng 6 (batch_id mới), Bronze chứa HAI bản
    của tháng 6. Silver phải chọn một — bản mới nhất được sử dụng; bản cũ vẫn lưu tại Bronze để phục vụ
    truy vết (audit trail). "Mới nhất" = _ingest_ts lớn nhất; nếu bằng nhau (ingest trong cùng một
    giây) -> _batch_id lớn hơn.
    """
    w = Window.partitionBy(PARTITION_COL)
    latest = F.max(F.struct("_ingest_ts", "_batch_id")).over(w)
    return (bronze.withColumn("_latest", latest)
                  .filter((F.col("_ingest_ts") == F.col("_latest._ingest_ts"))
                          & (F.col("_batch_id") == F.col("_latest._batch_id")))
                  .drop("_latest"))


# ---- 2. Dimension từ reference data ref_* -----------------------------------------------
def build_dim_model(ref_model_map: DataFrame) -> DataFrame:
    """Grain: 1 record = 1 mã dòng xe nội bộ. Phân cấp (hierarchy): hang_xe > hieu_xe > ma_hieu > ma_dong_xe."""
    assert_unique(ref_model_map, "ma_dong_xe", "ref_model_map")
    return ref_model_map.select(surrogate_key("ma_dong_xe").alias("model_key"),
                                "ma_dong_xe", "ma_hieu", "hang_xe", "hieu_xe")


def build_dim_vehicle_type(ref_veh_type: DataFrame) -> DataFrame:
    """Grain: 1 record = 1 mã loại xe. Bổ sung muc_dich_su_dung để roll-up (gộp nhóm) khi phân tích."""
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
    """Grain: 1 record = 1 ngày. Bao phủ trọn các NĂM có trong fact (suy ra từ dữ liệu, không từ ngày
    thực thi job — bảo đảm reproducibility)."""
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


# ---- 3 + 4. Parse, lookup dimension, gán rule -------------------------------------------
def parse_and_lookup(bronze: DataFrame, dim_model: DataFrame, dim_vehicle_type: DataFrame) -> DataFrame:
    """Bổ sung các cột đã parse (tiền tố p_) và khoá dimension. Cột thô được GIỮ NGUYÊN cho quarantine.

    Lookup dimension luôn dùng LEFT JOIN: record không lookup được vẫn được giữ lại (khoá null), để
    rule phía sau đưa nó vào quarantine. INNER JOIN sẽ khiến record đó biến mất mà không để lại dấu vết.
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
    """{rule_id: Column boolean, true = VI PHẠM}. Không điều kiện nào trả về null."""
    # DUP_KEY: xếp hạng trong nhóm cùng ma_dinh_gia, record hạng 1 được giữ lại. Thứ tự tất định
    # (deterministic, không phụ thuộc thứ tự đọc tệp): hiệu lực muộn nhất -> giá lớn hơn -> batch_id lớn hơn.
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
    """Cột rule_id = rule ĐẦU TIÊN (theo thứ tự RULES) mà record vi phạm; null = record hợp lệ."""
    conds = rule_conditions(df)
    expr = F.lit(None).cast(StringType())
    for rule in reversed(list(RULES)):          # dựng từ cuối lên -> rule đứng đầu danh sách được ưu tiên
        expr = F.when(conds[rule], F.lit(rule)).otherwise(expr)
    return df.withColumn("rule_id", expr)


def transform(bronze: DataFrame, dim_model: DataFrame,
              dim_vehicle_type: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze active -> (fact_valuation, quarantine). Hàm thuần (pure function): không đọc/ghi đĩa
    -> kiểm thử được độc lập."""
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


# ---- Thực thi -----------------------------------------------------------------------------
def run(spark: SparkSession, bronze_dir: Path = BRONZE_DIR, silver_dir: Path = SILVER_DIR,
        ref_model_map: Path | None = None, ref_veh_type: Path | None = None,
        ref_as_of: str | None = None) -> dict:
    """ref_model_map / ref_veh_type: đường dẫn CSV cụ thể (sandbox, test). Bỏ trống -> chọn phiên bản
    trong landing: mới nhất, hoặc mới nhất có version <= ref_as_of (src.reference)."""
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Chưa có Bronze ở {bronze_dir} — chạy python -m src.bronze trước")

    refs = {
        REF_MODEL_MAP_NAME: reference.inspect(ref_model_map or reference.resolve(REF_MODEL_MAP_NAME, ref_as_of)),
        REF_VEH_TYPE_NAME: reference.inspect(ref_veh_type or reference.resolve(REF_VEH_TYPE_NAME, ref_as_of)),
    }
    for r in refs.values():
        log.info("Ref: %-20s version=%s · %d record · sha256 %s…", r.name, r.version, r.row_count, r.sha256[:12])

    bronze = active_bronze(spark.read.parquet(bronze_dir.as_posix())).cache()
    dim_model = build_dim_model(read_ref_csv(spark, refs[REF_MODEL_MAP_NAME].path)).cache()
    dim_vehicle_type = build_dim_vehicle_type(read_ref_csv(spark, refs[REF_VEH_TYPE_NAME].path)).cache()

    fact, quarantine = transform(bronze, dim_model, dim_vehicle_type)
    fact, quarantine = fact.cache(), quarantine.cache()
    dim_date = build_dim_date(spark, fact)

    n_bronze, n_fact, n_quar = bronze.count(), fact.count(), quarantine.count()
    if n_fact + n_quar != n_bronze:                     # invariant: không record nào được phép biến mất
        raise RuntimeError(f"Đối soát lệch: fact {n_fact} + quarantine {n_quar} ≠ bronze {n_bronze}")

    def write(df: DataFrame, name: str, partitioned: bool = False) -> None:
        path = (silver_dir / name).as_posix()
        if partitioned:                                  # fact/quarantine: 1 thư mục partition / tháng
            # repartition trước: mỗi tháng nằm gọn trong 1 task -> 1 tệp/tháng, ít writer mở cùng lúc
            (df.repartition(PARTITION_COL)
               .write.mode("overwrite").partitionBy(PARTITION_COL).parquet(path))
        else:                                            # dimension nhỏ: 1 tệp
            df.coalesce(1).write.mode("overwrite").parquet(path)

    write(dim_model, "dim_model")
    write(dim_vehicle_type, "dim_vehicle_type")
    write(dim_date, "dim_date")
    write(fact, "fact_valuation", partitioned=True)
    write(quarantine, "quarantine", partitioned=True)

    # Silver ghi đè toàn bộ -> phải ghi lại ĐÃ DỰNG TỪ ĐÂU, nếu không lần chạy sau xoá mất dấu vết.
    # Gold đọc file này để gắn nhãn bản chụp mart_ml_training theo đúng phiên bản ref Silver đã dùng.
    batches = sorted(r._batch_id for r in bronze.select("_batch_id").distinct().collect())
    counts = {"bronze": n_bronze, "fact": n_fact, "quarantine": n_quar}
    run_info = {"refs": {k: r.as_dict() for k, r in refs.items()}, "active_batches": batches, "counts": counts}
    (silver_dir / RUN_INFO_FILE).write_text(json.dumps(run_info, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("Silver: bronze active %d = fact %d + quarantine %d (%.2f%%)",
             n_bronze, n_fact, n_quar, 100 * n_quar / max(n_bronze, 1))
    for row in quarantine.groupBy("rule_id").count().orderBy(F.desc("count")).collect():
        log.info("        %-20s %6d", row.rule_id, row["count"])
    log.info("        dim_model %d · dim_vehicle_type %d · dim_date %d",
             dim_model.count(), dim_vehicle_type.count(), dim_date.count())
    return {**counts, "ref_versions": {k: r.version for k, r in refs.items()}}


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
