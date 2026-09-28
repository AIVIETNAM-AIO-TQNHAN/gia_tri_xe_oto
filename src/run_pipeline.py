"""Chạy cả pipeline: Landing -> Bronze -> Silver -> Gold, dùng chung một SparkSession.

Chạy lại bao nhiêu lần cũng được:
  - Bronze chỉ nạp lô chưa có trong data/control/loaded_batches.csv (incremental).
  - Silver và Gold ghi đè toàn bộ (full refresh) từ tầng ngay trước nó.

Chạy:  python -m src.run_pipeline
Mã thoát: 0 = ổn · 1 = có lô bị từ chối hoặc lỗi khi nạp (Silver/Gold vẫn chạy với các lô đã có).
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
        log.info("== 3/3 Silver -> Gold (mart)")
        gold.run(spark)
    finally:
        spark.stop()
    return 1 if b["rejected"] or b["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
