"""Cổng kiểm tra lô: mỗi test làm hỏng lô mẫu theo MỘT cách và kiểm validator bắt đúng luật."""
import json
from pathlib import Path

from src.validate import load_manifest, validate_batch


def rules(partition_dir: Path) -> set[str]:
    return {f.rule_id for f in validate_batch(partition_dir)}


def data_file(partition_dir: Path) -> Path:
    return partition_dir / load_manifest(partition_dir)["file_name"]


def rewrite_manifest(partition_dir: Path, **changes) -> None:
    m = load_manifest(partition_dir)
    m.update(changes)
    (partition_dir / "_manifest.json").write_text(json.dumps(m), encoding="utf-8")


def test_good_batch_passes(one_batch):
    assert rules(one_batch) == set()


def test_missing_manifest(one_batch):
    (one_batch / "_manifest.json").unlink()
    assert rules(one_batch) == {"MANIFEST_INVALID"}


def test_manifest_missing_field(one_batch):
    m = load_manifest(one_batch)
    del m["record_count"]
    (one_batch / "_manifest.json").write_text(json.dumps(m), encoding="utf-8")
    assert rules(one_batch) == {"MANIFEST_INVALID"}


def test_file_edited_after_delivery(one_batch):
    """Sửa tay 1 ô sau khi bàn giao -> checksum lệch, dù số dòng và cột vẫn đúng."""
    f = data_file(one_batch)
    text = f.read_text(encoding="utf-8-sig")
    f.write_text(text.replace(",5,", ",7,", 1), encoding="utf-8-sig")
    assert rules(one_batch) == {"CHECKSUM_MISMATCH"}


def test_record_count_mismatch(one_batch):
    rewrite_manifest(one_batch, record_count=load_manifest(one_batch)["record_count"] + 1)
    assert rules(one_batch) == {"COUNT_MISMATCH"}


def test_missing_column(one_batch):
    f = data_file(one_batch)
    text = f.read_text(encoding="utf-8-sig")
    f.write_text(text.replace("ma_dinh_gia", "valuation_id", 1), encoding="utf-8-sig")
    assert "SCHEMA_COLUMN_MISSING" in rules(one_batch)       # kèm CHECKSUM_MISMATCH vì file đã đổi


def test_partition_mismatch(one_batch):
    rewrite_manifest(one_batch, as_of_month="2025-02")
    assert rules(one_batch) == {"PARTITION_MISMATCH"}
