"""Test có Spark: rule Silver trên dữ liệu dựng thủ công + tính idempotent của Bronze trên batch thực tế."""
import json
import shutil

import pytest

from helpers import REF_MODEL_SCHEMA, REF_TYPE_SCHEMA, VALID, bronze_df, local_df
from src import bronze, silver

# ---- Record-level validation tại Silver ------------------------------------------------

# ma_dinh_gia -> (các ô thay đổi so với VALID, rule_id kỳ vọng; None = vào fact)
CASES = {
    "BV-1":  ({}, None),
    "BV-2":  ({"ma_dong_xe": "ZZZ99999"}, "MODEL_CODE_UNKNOWN"),
    "BV-3":  ({"gia_tri_dinh_gia": "0"}, "PRICE_INVALID"),
    "BV-4":  ({"gia_tri_dinh_gia": "1.669.000.000"}, "PRICE_INVALID"),       # dấu chấm kiểu VN
    "BV-5":  ({"so_cho_ngoi": "  "}, "MISSING_FIELD"),
    "BV-6":  ({"nam_san_xuat": "2030"}, "VALUE_INVALID"),                    # > năm duyệt + 1
    "BV-7":  ({"so_cho_ngoi": "0"}, "VALUE_INVALID"),
    "BV-8":  ({"nhom_xe": "TAI1"}, "OUT_OF_SCOPE"),                          # xe tải
    "BV-9":  ({"hieu_luc_den": "2026-01-01"}, "DATE_INVALID"),               # đến < từ
    "BV-10": ({"ngay_duyet": "10/06/2026"}, "DATE_INVALID"),                 # sai định dạng
    "BV-11": ({"hieu_luc_tu": "2008-06-11", "hieu_luc_den": "2009-06-11"}, "DATE_INVALID"),
    "BV-12": ({"ma_dong_xe": "ZZZ99999", "gia_tri_dinh_gia": "0"}, "MODEL_CODE_UNKNOWN"),  # thứ tự ưu tiên rule
    "":      ({}, "DUP_KEY"),                                                # khoá rỗng
}


@pytest.fixture(scope="module")
def dims(spark):
    ref_model = local_df(spark, [("TOY01001", "TOY01", "Toyota", "Vios")], REF_MODEL_SCHEMA)
    ref_type = local_df(spark, [("KKD1-1", "Non Commercial (0-5)", "1"), ("TAI1", "Truck", "0")],
                        REF_TYPE_SCHEMA)
    return silver.build_dim_model(ref_model), silver.build_dim_vehicle_type(ref_type)


@pytest.fixture(scope="module")
def result(spark, dims):
    rows = [{**VALID, **changes, "ma_dinh_gia": key, "_batch_id": "b1"}
            for key, (changes, _) in CASES.items()]
    # Trùng khoá BV-13: record có hiệu lực muộn hơn được giữ lại, record còn lại vào DUP_KEY
    rows += [{**VALID, "ma_dinh_gia": "BV-13", "hieu_luc_tu": "2026-06-20", "_batch_id": "b1"},
             {**VALID, "ma_dinh_gia": "BV-13", "hieu_luc_tu": "2026-06-11", "_batch_id": "b1"}]
    fact, quarantine = silver.transform(bronze_df(spark, rows), *dims)
    return fact.collect(), quarantine.collect(), len(rows)


def test_each_rule_catches_its_row(result):
    _, quarantine, _ = result
    got = {r.ma_dinh_gia: r.rule_id for r in quarantine if r.ma_dinh_gia != "BV-13"}
    expected = {k: rule for k, (_, rule) in CASES.items() if rule}
    assert got == expected


def test_duplicate_keeps_latest_effective_row(result):
    fact, quarantine, _ = result
    assert [r.hieu_luc_tu.isoformat() for r in fact if r.ma_dinh_gia == "BV-13"] == ["2026-06-20"]
    assert [r.rule_id for r in quarantine if r.ma_dinh_gia == "BV-13"] == ["DUP_KEY"]


def test_no_row_lost(result):
    fact, quarantine, n = result
    assert len(fact) + len(quarantine) == n


def test_fact_row_is_typed_and_keyed(result, dims):
    fact, _, _ = result
    row = next(r for r in fact if r.ma_dinh_gia == "BV-1")
    dim_model, _ = dims
    assert row.model_key == dim_model.first().model_key
    assert row.gia_tri_trieu_vnd == 500.0                    # VND -> triệu VND
    assert (row.ngay_duyet_key, row.hieu_luc_tu_key) == (20260610, 20260612)
    assert (row.nam_san_xuat, row.so_cho_ngoi, row.tuoi_xe) == (2020, 5, 6)


def test_surrogate_key_is_deterministic(spark, dims):
    """Re-run -> cùng khoá. Nếu khoá thay đổi giữa hai lần chạy, fact cũ sẽ tham chiếu sai dimension."""
    first_run, _ = dims
    second_run = silver.build_dim_model(local_df(
        spark, [("NEW01001", "NEW01", "Mới", "Xe mới"), ("TOY01001", "TOY01", "Toyota", "Vios")],
        REF_MODEL_SCHEMA))                                                      # thêm mã mới đứng TRƯỚC
    key = lambda df: df.filter("ma_dong_xe = 'TOY01001'").first().model_key
    assert key(first_run) == key(second_run)


# ---- Bronze: idempotent + batch được gửi lại ------------------------------------------
def test_bronze_is_idempotent(spark, one_batch, tmp_path):
    landing, bronze_dir, control = one_batch.parent, tmp_path / "bronze", tmp_path / "loaded.csv"

    first = bronze.run(spark, landing, bronze_dir, control)
    n1 = spark.read.parquet(bronze_dir.as_posix()).count()
    second = bronze.run(spark, landing, bronze_dir, control)
    n2 = spark.read.parquet(bronze_dir.as_posix()).count()

    assert len(first["loaded"]) == 1 and second["loaded"] == [] and len(second["skipped"]) == 1
    assert n1 == n2 == 3119


def test_redelivered_batch_silver_reads_latest_only(spark, one_batch, tmp_path):
    """Bên cung cấp gửi lại tháng 2025-01 (batch_id mới): Bronze giữ CẢ HAI bản, Silver chỉ đọc bản
    mới nhất (Bronze active)."""
    landing, bronze_dir, control = one_batch.parent, tmp_path / "bronze", tmp_path / "loaded.csv"
    bronze.run(spark, landing, bronze_dir, control)

    redelivered = tmp_path / "landing_r2" / one_batch.name
    shutil.copytree(one_batch, redelivered)
    m = json.loads((redelivered / "_manifest.json").read_text(encoding="utf-8"))
    m["batch_id"] = "bvv_backfill_202501_02"
    (redelivered / "_manifest.json").write_text(json.dumps(m), encoding="utf-8")
    bronze.run(spark, redelivered.parent, bronze_dir, control)

    all_rows = spark.read.parquet(bronze_dir.as_posix())
    active = silver.active_bronze(all_rows)
    assert all_rows.count() == 2 * 3119
    assert active.count() == 3119
    assert {r._batch_id for r in active.select("_batch_id").distinct().collect()} == {"bvv_backfill_202501_02"}
