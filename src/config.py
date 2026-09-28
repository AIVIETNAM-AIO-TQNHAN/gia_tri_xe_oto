"""Đường dẫn và hằng số dùng chung. Mọi job lấy đường dẫn từ đây, không tự ghép chuỗi."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

# ---- Landing: file bàn giao, BẤT BIẾN --------------------------------------------
LANDING_DIR = DATA / "landing" / "business_vehicle_valuation"      # as_of_month=YYYY-MM/
REF_DIR = DATA / "landing" / "reference"
REF_MODEL_MAP = REF_DIR / "ref_model_map.csv"                      # ma_dong_xe -> hãng, dòng xe
REF_VEH_TYPE = REF_DIR / "ref_veh_type_scope.csv"                  # nhom_xe -> mô tả

# ---- Bronze: thô, append-only ----------------------------------------------------
BRONZE_DIR = DATA / "bronze" / "business_valuation"

# ---- Silver: star schema (fact_valuation, dim_model, dim_vehicle_type, dim_date) + quarantine
SILVER_DIR = DATA / "silver"

# ---- Gold: mart tổng hợp (mart_vehicle_price, mart_monthly_trend, mart_data_quality)
GOLD_DIR = DATA / "gold"

# ---- Control: sổ lô đã nạp (chống nạp trùng) ------------------------------------
LOADED_BATCHES = DATA / "control" / "loaded_batches.csv"

PARTITION_COL = "as_of_month"

# 16 cột theo đúng thứ tự contract (docs/DATA_CONTRACT.md §3)
EXPECTED_COLUMNS = [
    "ma_nghiep_vu", "nhom_xe", "hang_xe", "ma_dong_xe", "ma_kieu_dang", "so_cho_ngoi",
    "nam_san_xuat", "gia_tri_dinh_gia", "ngay_duyet", "hieu_luc_tu", "hieu_luc_den",
    "ky_bao_cao", "ma_dinh_gia", "don_vi_tinh", "loai_dinh_gia", "as_of_month",
]

# ---- Miền giá trị hợp lệ ở Silver (DATA_CONTRACT §5) -------------------------------
YEAR_MIN = 1980                   # cận trên = năm của ngay_duyet + 1, suy từ dữ liệu
SEATS_MIN, SEATS_MAX = 1, 64
PRICE_VND_MIN = 50_000_000        # 50 triệu
PRICE_VND_MAX = 50_000_000_000    # 50 tỷ
MAX_APPROVAL_GAP_DAYS = 366       # hieu_luc_tu phải nằm trong ±1 năm quanh ngay_duyet

# ---- Gold / mart_ml_training (ARCHITECTURE §4.1) -------------------------------------
ML_TRAIN_END_MONTH = "2026-03"    # tháng duyệt <= mốc -> train, sau mốc -> test
