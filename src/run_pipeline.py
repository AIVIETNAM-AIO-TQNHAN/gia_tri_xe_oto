"""Điều phối (orchestrate) toàn bộ pipeline: Landing -> Bronze -> Silver -> Gold, dùng chung một SparkSession.

Pipeline có thể re-run nhiều lần (idempotent):
  - Bronze chỉ ingest các batch chưa có trong sổ batch data/control/loaded_batches.csv (incremental).
  - Silver và 3 mart phân tích được ghi đè toàn bộ (full refresh) từ tầng ngay trước nó.
  - mart_ml_training: mỗi cặp (phiên bản ref, mốc chia) là một bản chụp riêng, không ghi đè bản khác.

Mỗi lần chạy thành công ghi THÊM một dòng vào data/control/run_log.csv: đã dùng phiên bản ref nào
(version + sha256 + số record), tham số nào, ra kết quả gì. Silver ghi đè, nhưng run_log thì không —
nhờ đó luôn trả lời được "kết quả tháng trước được tính với ref nào".

Chạy:
    python -m src.run_pipeline                                # ref mới nhất, mốc chia mặc định (config)
    python -m src.run_pipeline --ref-version 2026-06          # tái tạo kết quả với ref <= 2026-06
    python -m src.run_pipeline --train-end-month 2026-05      # bản chụp ML với mốc chia khác
Exit code: 0 = thành công · 1 = có batch bị reject hoặc lỗi khi ingest (Silver/Gold vẫn được thực thi
với các batch đã có).
"""
import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from src import bronze, gold, silver
from src.config import (ML_TRAIN_END_MONTH, REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME, RUN_LOG,
                        SILVER_DIR)
from src.spark import get_spark

log = logging.getLogger("pipeline")

RUN_LOG_COLUMNS = [
    "run_id", "run_ts", "ref_as_of",
    "ref_model_map_version", "ref_model_map_sha256", "ref_model_map_rows",
    "ref_veh_type_version", "ref_veh_type_sha256", "ref_veh_type_rows",
    "so_lo_active", "thang_cuoi", "train_end_month",
    "bronze", "fact", "quarantine", "ty_le_quarantine_pct",
    "ml_train", "ml_test", "ml_snapshot",
]


def setup_logging() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)-8s %(message)s",
                        datefmt="%H:%M:%S", stream=sys.stdout)
    logging.getLogger("py4j").setLevel(logging.WARNING)


def build_run_log_row(run_ts: datetime, ref_as_of: str | None, silver_info: dict,
                      train_end_month: str, gold_counts: dict) -> dict:
    """Một dòng run_log. silver_info = _run_info.json của Silver vừa ghi (src.gold.read_silver_run_info)."""
    ref_m = silver_info["refs"][REF_MODEL_MAP_NAME]
    ref_t = silver_info["refs"][REF_VEH_TYPE_NAME]
    c = silver_info["counts"]
    batches = silver_info["active_batches"]
    months = sorted(m.group(1) for b in batches if (m := re.search(r"_(\d{6})_", b)))  # bvv_202607_01
    return {
        "run_id": run_ts.strftime("%Y%m%dT%H%M%S"),
        "run_ts": run_ts.isoformat(timespec="seconds"),
        "ref_as_of": ref_as_of or "latest",
        "ref_model_map_version": ref_m["version"], "ref_model_map_sha256": ref_m["sha256"],
        "ref_model_map_rows": ref_m["row_count"],
        "ref_veh_type_version": ref_t["version"], "ref_veh_type_sha256": ref_t["sha256"],
        "ref_veh_type_rows": ref_t["row_count"],
        "so_lo_active": len(batches),
        "thang_cuoi": f"{months[-1][:4]}-{months[-1][4:]}" if months else "",
        "train_end_month": train_end_month,
        "bronze": c["bronze"], "fact": c["fact"], "quarantine": c["quarantine"],
        "ty_le_quarantine_pct": round(100 * c["quarantine"] / max(c["bronze"], 1), 2),
        "ml_train": gold_counts["ml_train"], "ml_test": gold_counts["ml_test"],
        "ml_snapshot": gold_counts["ml_snapshot"],
    }


def append_run_log(row: dict, path: Path = RUN_LOG) -> None:
    """Append-only, giống loaded_batches.csv. Header lệch (file của phiên bản code cũ) -> dừng, không
    ghi trộn hai định dạng vào một file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    if not new_file:
        with open(path, newline="", encoding="utf-8") as f:
            header = next(csv.reader(f), [])
        if header != RUN_LOG_COLUMNS:
            raise ValueError(f"{path}: header khác RUN_LOG_COLUMNS — đổi tên file cũ rồi chạy lại")
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RUN_LOG_COLUMNS)
        if new_file:
            w.writeheader()
        w.writerow(row)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Landing -> Bronze -> Silver -> Gold")
    ap.add_argument("--ref-version", default=None, metavar="YYYY-MM",
                    help="dùng phiên bản ref mới nhất có version <= giá trị này (mặc định: mới nhất)")
    ap.add_argument("--train-end-month", default=ML_TRAIN_END_MONTH, metavar="YYYY-MM",
                    help=f"mốc chia train/test của mart_ml_training (mặc định: {ML_TRAIN_END_MONTH})")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging()
    run_ts = datetime.now().astimezone()      # metadata của lần chạy, không dùng trong transform
    spark = get_spark("gia_tri_xe_oto")
    try:
        log.info("== 1/3 Landing -> Bronze")
        b = bronze.run(spark)
        log.info("== 2/3 Bronze -> Silver (star schema)")
        silver.run(spark, ref_as_of=args.ref_version)
        log.info("== 3/3 Silver -> Gold (data mart)")
        g = gold.run(spark, train_end_month=args.train_end_month)
    finally:
        spark.stop()

    row = build_run_log_row(run_ts, args.ref_version, gold.read_silver_run_info(SILVER_DIR),
                            args.train_end_month, g)
    append_run_log(row)
    log.info("Run log: %s · ref_model_map=%s · train_end_month=%s -> %s",
             row["run_id"], row["ref_model_map_version"], row["train_end_month"], RUN_LOG.name)
    return 1 if b["rejected"] or b["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
