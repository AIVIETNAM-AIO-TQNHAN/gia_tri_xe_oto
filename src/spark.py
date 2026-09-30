"""SparkSession chế độ local, dùng chung cho mọi job.

Trên Windows cần: JAVA_HOME (Java 17 hoặc 11) và HADOOP_HOME có bin/winutils.exe + bin/hadoop.dll.
Thiếu winutils vẫn đọc được dữ liệu nhưng thao tác GHI Parquet sẽ lỗi — nên kiểm tra ngay từ đầu
(fail fast) để thông báo lỗi dễ hiểu.
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _check_env() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not (Path(java_home) / "bin").exists():
        raise RuntimeError(f"JAVA_HOME chưa đặt hoặc sai (hiện: {java_home!r}). Cần Java 17 hoặc 11.")
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME") or ""
        missing = [n for n in ("winutils.exe", "hadoop.dll") if not (Path(hadoop_home) / "bin" / n).exists()]
        if missing:
            raise RuntimeError(f"HADOOP_HOME={hadoop_home!r} thiếu bin/{', bin/'.join(missing)}.")


def get_spark(app_name: str = "gia_tri_xe_oto") -> SparkSession:
    """Gọi nhiều lần vẫn trả về CÙNG một session (getOrCreate)."""
    _check_env()
    # Python worker dùng đúng trình thông dịch đang chạy — tránh lỗi "Python worker failed to connect back"
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.shuffle.partitions", "8")      # dữ liệu nhỏ: 8 thay vì mặc định 200
        # Ngày sai định dạng -> null (để rule DATE_INVALID phát hiện), thay vì ném exception làm hỏng cả job
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # Giữ as_of_month=2026-06 ở kiểu string khi đọc lại thư mục partition, không để Spark tự suy luận kiểu
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
