"""Điều phối (orchestrate) toàn bộ pipeline: Landing -> Bronze -> Silver -> Gold, dùng chung một SparkSession.

Pipeline có thể re-run nhiều lần (idempotent):
  - Bronze chỉ ingest các batch chưa có trong sổ batch data/control/loaded_batches.csv (incremental).
  - Silver và Gold được ghi đè toàn bộ (full refresh) từ tầng ngay trước nó.

Chạy:  python -m src.run_pipeline
Exit code: 0 = thành công · 1 = có batch bị reject hoặc lỗi khi ingest (Silver/Gold vẫn được thực thi
với các batch đã có).
"""
import logging
import sys

from src import bronze, gold, silver
from src.spark import get_spark

log = logging.getLogger("pipeline")


def setup_logging() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)-8s %(message)s",
                        datefmt="%H:%M:%S", stream=sys.stdout)
    logging.getLogger("py4j").setLevel(logging.WARNING)


def main() -> int:
    setup_logging()
    spark = get_spark("gia_tri_xe_oto")
    try:
        log.info("== 1/3 Landing -> Bronze")
        b = bronze.run(spark)
        log.info("== 2/3 Bronze -> Silver (star schema)")
        silver.run(spark)
        log.info("== 3/3 Silver -> Gold (data mart)")
        gold.run(spark)
    finally:
        spark.stop()
    return 1 if b["rejected"] or b["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
