"""mart_ml_training: grain, cách chia train/test và contract đầu ra (README §5.3).

Dữ liệu đi đúng đường thật: dòng Bronze dựng tay -> silver.transform -> view Silver -> SQL của Gold.
Nhờ vậy test cũng bắt được lệch kiểu cột giữa Silver và contract, không chỉ lỗi trong câu SQL.
"""
import pytest

from helpers import REF_MODEL_SCHEMA, REF_TYPE_SCHEMA, VALID, bronze_df, local_df
from src import gold, silver

# (ma_dinh_gia, ngày duyệt, ô đổi thêm). Mốc cắt dùng trong test: 2026-03.
ROWS = [
    ("BV-FEB", "2026-02-15", {}),
    ("BV-MAR", "2026-03-31", {"loai_dinh_gia": "new", "nam_san_xuat": "2026"}),   # sát mốc -> train
    ("BV-APR", "2026-04-01", {"ma_dong_xe": "HYU01001", "nhom_xe": "TAXI"}),      # ngay sau mốc -> test
    ("BV-BAD", "2026-04-02", {"gia_tri_dinh_gia": "0"}),                          # quarantine, không vào mart
]
TRAIN_END = "2026-03"


@pytest.fixture(scope="module")
def mart(spark):
    rows = [{**VALID, **extra, "ma_dinh_gia": key, "ngay_duyet": day, "hieu_luc_tu": day,
             "hieu_luc_den": "", "as_of_month": day[:7], "_batch_id": "b1"} for key, day, extra in ROWS]
    ref_model = local_df(spark, [("TOY01001", "TOY01", "Toyota", "Vios"),
                                 ("HYU01001", "HYU01", "Hyundai", "Accent")], REF_MODEL_SCHEMA)
    ref_type = local_df(spark, [("KKD1-1", "Non Commercial (0-5)", "1"), ("TAXI", "Taxi", "1")],
                        REF_TYPE_SCHEMA)
    dim_model = silver.build_dim_model(ref_model)
    dim_vehicle_type = silver.build_dim_vehicle_type(ref_type)
    fact, _ = silver.transform(bronze_df(spark, rows), dim_model, dim_vehicle_type)
    # Tính fact MỘT lần rồi cắt lineage: nếu không, mỗi action phía dưới Spark lại chạy lại
    # toàn bộ silver.transform (window, join).
    fact = fact.localCheckpoint()

    fact.createOrReplaceTempView("fact_valuation")
    dim_model.createOrReplaceTempView("dim_model")
    dim_vehicle_type.createOrReplaceTempView("dim_vehicle_type")
    silver.build_dim_date(spark, fact).createOrReplaceTempView("dim_date")
    df = spark.sql(gold.mart_ml_training_sql(TRAIN_END)).cache()
    return df, df.collect(), fact.count()


def test_one_row_per_contract(mart):
    """Grain = fact: không gộp, không nhân bản dòng khi join dimension, không mất dòng."""
    _, rows, n_fact = mart
    assert len(rows) == n_fact == 3
    assert len({r.ma_dinh_gia for r in rows}) == len(rows)
    assert "BV-BAD" not in {r.ma_dinh_gia for r in rows}


def test_split_by_approval_month(mart):
    _, rows, _ = mart
    assert {r.ma_dinh_gia: r.split for r in rows} == {"BV-FEB": "train", "BV-MAR": "train", "BV-APR": "test"}
    # Không tháng nào nằm ở cả hai tập
    months = {r.thang_duyet for r in rows}
    assert len(months) == len({(r.thang_duyet, r.split) for r in rows})


def test_features_come_from_dimensions(mart):
    _, rows, _ = mart
    apr = next(r for r in rows if r.ma_dinh_gia == "BV-APR")
    assert (apr.hang_xe, apr.hieu_xe, apr.muc_dich_su_dung) == ("Hyundai", "Accent", "Taxi")
    assert (apr.thang_duyet, apr.tuoi_xe, apr.gia_tri_trieu_vnd) == ("2026-04", 6, 500.0)


def test_output_matches_contract(mart):
    df, _, _ = mart
    gold.check_ml_contract(df)                    # ném lỗi nếu sai cột/kiểu/thứ tự hoặc có null


def test_contract_check_rejects_drift(mart):
    df, _, _ = mart
    with pytest.raises(ValueError, match="sai schema"):
        gold.check_ml_contract(df.drop("tuoi_xe"))
    with pytest.raises(ValueError, match="sai schema"):
        gold.check_ml_contract(df.withColumn("so_cho_ngoi", df.so_cho_ngoi.cast("string")))
