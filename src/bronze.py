"""Landing -> Bronze: batch ingestion theo kiểu incremental + idempotent (README §3).

Mỗi batch:  batch-level validation -> đã có trong sổ batch? bỏ qua -> đọc CSV (mọi cột kiểu string)
            -> bổ sung technical column -> GHI BRONZE -> rồi mới GHI SỔ BATCH loaded_batches.

Vì sao ghi Bronze TRƯỚC rồi mới ghi sổ batch:
  - Ghi sổ trước mà Bronze lỗi  -> batch bị coi là đã ingest, MẤT VĨNH VIỄN khỏi pipeline.
  - Ghi Bronze trước mà sổ lỗi  -> lần sau batch đó được ingest lại thành bản trùng. Không mất dữ
    liệu, và Silver vẫn chỉ đọc một bản (bản có _ingest_ts mới nhất của tháng) nên kết quả không đổi.

Vì sao đọc mọi cột dạng string: Bronze giữ NGUYÊN TRẠNG dữ liệu thô. Nếu cast kiểu tại đây, giá trị
sai kiểu sẽ thành null và mất luôn dấu vết giá trị gốc — việc parse thuộc về Silver, nơi record lỗi
được đưa vào quarantine.

Chạy:  python -m src.bronze
"""
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.config import BRONZE_DIR, LANDING_DIR, LOADED_BATCHES, PARTITION_COL, ROOT
from src.validate import list_batches, load_manifest, partition_value, validate_batch

log = logging.getLogger("bronze")

CORRUPT_COL = "_corrupt_record"      # record vỡ cột vẫn được ghi vào Bronze, không bị mất
CONTROL_COLUMNS = ["batch_id", "as_of_month", "row_count", "ingest_ts", "source_file"]


# ---- Sổ batch (batch log) đã ingest ------------------------------------------------
def read_loaded_batch_ids(control_file: Path) -> set[str]:
    if not control_file.exists():
        return set()
    with open(control_file, newline="", encoding="utf-8") as f:
        return {row["batch_id"] for row in csv.DictReader(f)}


def append_loaded_batch(control_file: Path, row: dict) -> None:
    control_file.parent.mkdir(parents=True, exist_ok=True)
    need_header = not control_file.exists() or control_file.stat().st_size == 0
    with open(control_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONTROL_COLUMNS)
        if need_header:
            writer.writeheader()
        writer.writerow(row)


# ---- Đọc một batch --------------------------------------------------------------------
def read_batch(spark: SparkSession, data_file: Path, manifest: dict) -> DataFrame:
    """CSV -> DataFrame với mọi cột kiểu string.

    Spark áp schema CSV THEO VỊ TRÍ cột, nên schema phải được dựng từ header THỰC TẾ của tệp (đọc
    bằng Python, encoding utf-8-sig tự loại bỏ BOM) chứ không lấy từ danh sách cột mong đợi.
    """
    with open(data_file, newline="", encoding=manifest["encoding"]) as f:
        header = next(csv.reader(f, delimiter=manifest["delimiter"]))
    schema = StructType([StructField(c, StringType(), True) for c in header + [CORRUPT_COL]])
    return (spark.read
            .schema(schema)
            .option("header", True)
            .option("sep", manifest["delimiter"])
            .option("encoding", "UTF-8")          # Spark không hỗ trợ 'utf-8-sig'; BOM nằm ở dòng header đã bị bỏ qua
            .option("quote", '"')
            .option("escape", '"')                # data contract: escape "" trong chuỗi; mặc định của Spark là \
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .csv(data_file.as_posix()))


def relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def ingest_batch(spark: SparkSession, partition_dir: Path, bronze_dir: Path, control_file: Path) -> int:
    manifest = load_manifest(partition_dir)
    data_file = partition_dir / manifest["file_name"]
    ingest_ts = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(microsecond=0)

    df = (read_batch(spark, data_file, manifest)
          # Cột partition lấy từ tên thư mục (batch-level validation đã bảo đảm khớp với cột trong tệp) —
          # để record vỡ cột (as_of_month null) không rơi vào partition __HIVE_DEFAULT_PARTITION__.
          .withColumn(PARTITION_COL, F.lit(partition_value(partition_dir)))
          .withColumn("_batch_id", F.lit(manifest["batch_id"]))
          .withColumn("_source_file", F.lit(relative(data_file)))
          .withColumn("_ingest_ts", F.lit(ingest_ts)))   # metadata, KHÔNG tham gia tính toán nghiệp vụ

    row_count = df.count()
    df.write.mode("append").partitionBy(PARTITION_COL).parquet(bronze_dir.as_posix())

    append_loaded_batch(control_file, {           # chỉ ghi sổ batch khi Bronze đã ghi xong
        "batch_id": manifest["batch_id"],
        "as_of_month": partition_value(partition_dir),
        "row_count": row_count,
        "ingest_ts": ingest_ts.isoformat(),
        "source_file": relative(data_file),
    })
    return row_count


def run(spark: SparkSession, landing_dir: Path = LANDING_DIR, bronze_dir: Path = BRONZE_DIR,
        control_file: Path = LOADED_BATCHES) -> dict[str, list[str]]:
    """Ingest mọi batch mới. Một batch bị reject/lỗi không ngăn cản việc ingest các batch khác."""
    result = {"loaded": [], "skipped": [], "rejected": [], "failed": []}
    loaded_ids = read_loaded_batch_ids(control_file)

    for partition_dir in list_batches(landing_dir):
        findings = validate_batch(partition_dir)
        if findings:
            log.error("REJECT  %s", partition_dir.name)
            for f in findings:
                log.error("        %-22s %s", f.rule_id, f.message)
            result["rejected"].append(partition_dir.name)
            continue

        batch_id = load_manifest(partition_dir)["batch_id"]
        if batch_id in loaded_ids:
            result["skipped"].append(batch_id)
            continue

        try:
            n = ingest_batch(spark, partition_dir, bronze_dir, control_file)
        except Exception as e:                      # batch này lỗi, các batch khác vẫn được ingest
            log.error("FAILED  %s: %s", partition_dir.name, e)
            result["failed"].append(partition_dir.name)
            continue
        loaded_ids.add(batch_id)
        result["loaded"].append(batch_id)
        log.info("INGEST  %s  %6d record  (batch_id=%s)", partition_dir.name, n, batch_id)

    log.info("Bronze: %d batch ingest mới, bỏ qua %d batch đã ingest, %d bị reject, %d lỗi",
             len(result["loaded"]), len(result["skipped"]), len(result["rejected"]), len(result["failed"]))
    return result


def main() -> int:
    from src.spark import get_spark
    from src.run_pipeline import setup_logging
    setup_logging()
    spark = get_spark("bronze")
    try:
        result = run(spark)
    finally:
        spark.stop()
    return 1 if result["rejected"] or result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
