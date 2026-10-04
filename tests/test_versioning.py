"""Quản lý phiên bản: chọn đúng phiên bản ref, phát hiện ref bị sửa sau bàn giao, run_log chỉ ghi thêm,
bản chụp mart_ml_training không ghi đè nhau."""
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest

from src import bronze, gold, reference, silver
from src.config import REF_DIR, REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME, RUN_INFO_FILE
from src.run_pipeline import RUN_LOG_COLUMNS, append_run_log, build_run_log_row

NAME = "ref_model_map"
HEADER = "ma_dong_xe,ma_hieu,hang_xe,hieu_xe\n"


def make_version(ref_dir: Path, version: str, body: str, manifest: bool = True) -> Path:
    vdir = ref_dir / NAME / f"version={version}"
    vdir.mkdir(parents=True)
    path = vdir / f"{NAME}.csv"
    path.write_text(HEADER + body, encoding="utf-8")
    if manifest:
        (vdir / "_manifest.json").write_text(json.dumps({
            "ref_name": NAME, "version": version, "file_name": path.name,
            "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "row_count": body.count("\n")}), encoding="utf-8")
    return path


@pytest.fixture
def ref_dir(tmp_path: Path) -> Path:
    make_version(tmp_path, "2026-06", "TOY01001,TOY01,Toyota,Vios\n")
    make_version(tmp_path, "2026-07", "TOY01001,TOY01,Toyota,Vios\nMAZ24001,MAZ24,Mazda,CX5\n")
    make_version(tmp_path, "2026-08", "X,X,X,X\n", manifest=False)       # bàn giao dở dang
    return tmp_path


# ---- Chọn phiên bản ---------------------------------------------------------------------
def test_resolve_latest_and_as_of(ref_dir):
    v = lambda p: p.parent.name
    assert reference.list_versions(NAME, ref_dir) == ["2026-06", "2026-07"]   # 2026-08 thiếu manifest
    assert v(reference.resolve(NAME, ref_dir=ref_dir)) == "version=2026-07"
    assert v(reference.resolve(NAME, "2026-06", ref_dir)) == "version=2026-06"
    assert v(reference.resolve(NAME, "2026-12", ref_dir)) == "version=2026-07"
    with pytest.raises(FileNotFoundError):
        reference.resolve(NAME, "2026-05", ref_dir)                            # trước bản đầu tiên
    with pytest.raises(ValueError):
        reference.resolve(NAME, "2026-7", ref_dir)


def test_inspect_reads_manifest(ref_dir):
    r = reference.inspect(reference.resolve(NAME, ref_dir=ref_dir))
    assert (r.version, r.row_count) == ("2026-07", 2)


def test_inspect_detects_edit_after_delivery(ref_dir):
    """Sửa tay file ref đã bàn giao -> dừng. Muốn sửa thì bàn giao phiên bản mới."""
    path = reference.resolve(NAME, "2026-06", ref_dir)
    path.write_text(HEADER + "TOY01001,TOY01,Toyota,Corolla\n", encoding="utf-8")
    with pytest.raises(ValueError, match="sha256"):
        reference.inspect(path)


def test_inspect_unversioned_copy(tmp_path):
    path = tmp_path / "ref_model_map.csv"
    path.write_text(HEADER + "TOY01001,TOY01,Toyota,Vios\n", encoding="utf-8")
    assert reference.inspect(path).version == "unversioned"


@pytest.mark.parametrize("name", [REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME])
def test_real_landing_refs_match_manifest(name):
    """Mọi phiên bản ref thật trong landing vẫn khớp manifest (landing bất biến)."""
    versions = reference.list_versions(name)
    assert versions
    for v in versions:
        assert reference.inspect(REF_DIR / name / f"version={v}" / f"{name}.csv").version == v


# ---- run_log ----------------------------------------------------------------------------
SILVER_INFO = {
    "refs": {REF_MODEL_MAP_NAME: {"version": "2026-07", "sha256": "a" * 64, "row_count": 2167},
             REF_VEH_TYPE_NAME: {"version": "2026-06", "sha256": "b" * 64, "row_count": 40}},
    "active_batches": ["bvv_backfill_202501_01", "bvv_202608_01", "bvv_202607_01"],
    "counts": {"bronze": 100, "fact": 90, "quarantine": 10},
}
GOLD_COUNTS = {"ml_train": 60, "ml_test": 30,
               "ml_snapshot": "mart_ml_training/ref_version=2026-07/train_end_month=2026-03"}


def test_run_log_row():
    row = build_run_log_row(datetime(2026, 10, 4, 21, 0, 5), None, SILVER_INFO, "2026-03", GOLD_COUNTS)
    assert list(row) == RUN_LOG_COLUMNS
    assert row["run_id"] == "20261004T210005"
    assert (row["ref_as_of"], row["ref_model_map_version"], row["thang_cuoi"]) == ("latest", "2026-07", "2026-08")
    assert (row["so_lo_active"], row["ty_le_quarantine_pct"]) == (3, 10.0)


def test_run_log_is_append_only(tmp_path):
    path = tmp_path / "control" / "run_log.csv"
    row = build_run_log_row(datetime(2026, 10, 4), "2026-06", SILVER_INFO, "2026-03", GOLD_COUNTS)
    append_run_log(row, path)
    append_run_log({**row, "run_id": "second"}, path)
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["run_id"] for r in rows] == ["20261004T000000", "second"]

    path.write_text("cot_cu\n1\n", encoding="utf-8")                       # file của định dạng khác
    with pytest.raises(ValueError, match="header"):
        append_run_log(row, path)


# ---- Bản chụp mart_ml_training ------------------------------------------------------------
def test_ml_snapshots_are_kept_per_ref_and_split(spark, one_batch, tmp_path):
    """Hai phiên bản ref × hai mốc chia -> bốn bản chụp riêng; chạy lại cùng tham số -> cùng kết quả."""
    bronze_dir, silver_dir, gold_dir = tmp_path / "bronze", tmp_path / "silver", tmp_path / "gold"
    bronze.run(spark, one_batch.parent, bronze_dir, tmp_path / "loaded.csv")

    results = {}
    for ref_as_of in ("2026-06", None):
        s = silver.run(spark, bronze_dir, silver_dir, ref_as_of=ref_as_of)
        for split in ("2025-01", "2024-12"):
            results[(s["ref_versions"][REF_MODEL_MAP_NAME], split)] = gold.run(
                spark, silver_dir, gold_dir, csv_dir=None, train_end_month=split)

    assert set(results) == {("2026-06", "2025-01"), ("2026-06", "2024-12"),
                            ("2026-07", "2025-01"), ("2026-07", "2024-12")}
    for (ref, split), res in results.items():
        snap = gold.ml_snapshot_dir(gold_dir, ref, split)
        assert res["ml_snapshot"] == snap.relative_to(gold_dir).as_posix()
        info = json.loads((snap / RUN_INFO_FILE).read_text(encoding="utf-8"))
        assert info["refs"][REF_MODEL_MAP_NAME]["version"] == ref and info["train_end_month"] == split
        assert spark.read.parquet(snap.as_posix()).count() == res["mart_ml_training"]
    # Lô 2025-01: mốc 2025-01 -> toàn train, mốc 2024-12 -> toàn test
    assert results[("2026-07", "2025-01")]["ml_test"] == results[("2026-07", "2024-12")]["ml_train"] == 0
    # Ref mới giải mã thêm mã xe -> bảng ML (= fact) có nhiều dòng hơn
    assert results[("2026-07", "2025-01")]["mart_ml_training"] > results[("2026-06", "2025-01")]["mart_ml_training"]

    again = gold.run(spark, silver_dir, gold_dir, csv_dir=None, train_end_month="2024-12")
    assert again == results[("2026-07", "2024-12")]
