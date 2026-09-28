"""Cổng kiểm tra lô — chạy TRƯỚC khi nạp Bronze (docs/DATA_CONTRACT.md §6).

Python thuần, không cần Spark: chỉ đọc manifest, header và đếm dòng.
Mọi luật ở đây đều là REJECT: vi phạm một luật là từ chối CẢ LÔ, file nằm nguyên ở landing,
bên cung cấp phải bàn giao lại. Pipeline không bao giờ tự sửa file landing.

    MANIFEST_INVALID       không có/không đọc được _manifest.json, thiếu field, hoặc không thấy file dữ liệu
    CHECKSUM_MISMATCH      sha256 của file khác file_sha256 trong manifest
    SCHEMA_COLUMN_MISSING  header thiếu cột bắt buộc
    COUNT_MISMATCH         số dòng dữ liệu khác record_count trong manifest
    PARTITION_MISMATCH     tên thư mục, as_of_month của manifest và cột as_of_month không trùng nhau

Chạy:  python -m src.validate          (mã thoát 1 nếu có lô bị từ chối)
"""
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from src.config import EXPECTED_COLUMNS, LANDING_DIR, PARTITION_COL

MANIFEST_FIELDS = ["batch_id", "as_of_month", "record_count", "file_name", "file_sha256",
                   "encoding", "delimiter"]


@dataclass
class Finding:
    rule_id: str
    message: str


def load_manifest(partition_dir: Path) -> dict:
    return json.loads((partition_dir / "_manifest.json").read_text(encoding="utf-8"))


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def partition_value(partition_dir: Path) -> str:
    """'as_of_month=2026-06' -> '2026-06'."""
    return partition_dir.name.split("=", 1)[1] if "=" in partition_dir.name else ""


def validate_batch(partition_dir: Path) -> list[Finding]:
    """Trả danh sách vi phạm của một lô; rỗng = lô đạt."""
    # 1. Manifest phải đọc được thì mới kiểm tiếp được — lỗi ở đây dừng ngay
    try:
        manifest = load_manifest(partition_dir)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return [Finding("MANIFEST_INVALID", f"không đọc được _manifest.json: {e}")]
    missing = [k for k in MANIFEST_FIELDS if k not in manifest]
    if missing:
        return [Finding("MANIFEST_INVALID", f"manifest thiếu field {missing}")]
    data_file = partition_dir / manifest["file_name"]
    if not data_file.exists():
        return [Finding("MANIFEST_INVALID", f"không thấy file dữ liệu {manifest['file_name']}")]

    findings: list[Finding] = []

    # 2. Checksum: phát hiện file hỏng / truyền thiếu / bị sửa tay sau khi bàn giao
    actual_sha = sha256_of(data_file)
    if actual_sha != manifest["file_sha256"]:
        findings.append(Finding("CHECKSUM_MISMATCH",
                                f"sha256 {actual_sha[:12]}… ≠ manifest {manifest['file_sha256'][:12]}…"))

    # 3. Đọc header + đếm dòng — tham số đọc LẤY TỪ MANIFEST, không hard-code
    with open(data_file, newline="", encoding=manifest["encoding"]) as f:
        reader = csv.DictReader(f, delimiter=manifest["delimiter"])
        header = reader.fieldnames or []
        rows = list(reader)

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in header]
    if missing_cols:
        findings.append(Finding("SCHEMA_COLUMN_MISSING", f"thiếu cột {missing_cols}"))

    if len(rows) != int(manifest["record_count"]):
        findings.append(Finding("COUNT_MISMATCH",
                                f"file có {len(rows)} dòng, manifest khai {manifest['record_count']}"))

    # 4. Ba nơi cùng nói về kỳ dữ liệu phải khớp nhau
    folder = partition_value(partition_dir)
    col_values = {r.get(PARTITION_COL) for r in rows} if PARTITION_COL in header else set()
    if manifest["as_of_month"] != folder or (col_values and col_values != {folder}):
        findings.append(Finding("PARTITION_MISMATCH",
                                f"thư mục={folder}, manifest={manifest['as_of_month']}, "
                                f"cột as_of_month={sorted(v or '' for v in col_values)[:3]}"))
    return findings


def list_batches(landing_dir: Path = LANDING_DIR) -> list[Path]:
    """Các thư mục lô, sort theo tên = tăng dần theo tháng."""
    return sorted(p for p in landing_dir.glob(f"{PARTITION_COL}=*") if p.is_dir())


def validate_all(landing_dir: Path = LANDING_DIR) -> dict[Path, list[Finding]]:
    return {p: validate_batch(p) for p in list_batches(landing_dir)}


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    results = validate_all()
    for p, findings in results.items():
        print(f"{'REJECT' if findings else 'OK    '}  {p.name}")
        for f in findings:
            print(f"        {f.rule_id:<22} {f.message}")
    n_bad = sum(1 for f in results.values() if f)
    print(f"\n{len(results)} lô: {len(results) - n_bad} đạt, {n_bad} bị từ chối")
    return 1 if n_bad else 0


if __name__ == "__main__":
    sys.exit(main())
