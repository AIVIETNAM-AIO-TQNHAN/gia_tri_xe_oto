"""Đường dẫn và hằng số dùng chung. Mọi job lấy đường dẫn từ đây, không tự ghép chuỗi."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

# ---- Landing zone: tệp bàn giao, IMMUTABLE ---------------------------------------
LANDING_DIR = DATA / "landing" / "business_vehicle_valuation"      # as_of_month=YYYY-MM/
REF_DIR = DATA / "landing" / "reference"                           # <ref_name>/version=YYYY-MM/
REF_MODEL_MAP_NAME = "ref_model_map"                               # ma_dong_xe -> hãng, dòng xe
REF_VEH_TYPE_NAME = "ref_veh_type_scope"                           # nhom_xe -> mô tả

# ---- Bronze: dữ liệu thô, append-only --------------------------------------------
BRONZE_DIR = DATA / "bronze" / "business_valuation"

# ---- Silver: star schema (fact_valuation, dim_model, dim_vehicle_type, dim_date) + quarantine
SILVER_DIR = DATA / "silver"

# ---- Gold: data mart (mart_vehicle_price, mart_monthly_trend, mart_data_quality, mart_ml_training)
GOLD_DIR = DATA / "gold"

# ---- Control: sổ batch (batch log) đã ingest, ngăn ingest trùng lặp --------------
LOADED_BATCHES = DATA / "control" / "loaded_batches.csv"
# ---- Control: sổ các lần chạy — phiên bản ref + tham số + kết quả, append-only ------
RUN_LOG = DATA / "control" / "run_log.csv"
RUN_INFO_FILE = "_run_info.json"  # ghi cạnh output Silver và mỗi bản chụp mart_ml_training

PARTITION_COL = "as_of_month"

# Schema 16 cột theo đúng thứ tự của data contract (docs/DATA_CONTRACT.md §3)
EXPECTED_COLUMNS = [
    "ma_nghiep_vu", "nhom_xe", "hang_xe", "ma_dong_xe", "ma_kieu_dang", "so_cho_ngoi",
    "nam_san_xuat", "gia_tri_dinh_gia", "ngay_duyet", "hieu_luc_tu", "hieu_luc_den",
    "ky_bao_cao", "ma_dinh_gia", "don_vi_tinh", "loai_dinh_gia", "as_of_month",
]

# ---- Ngưỡng (threshold) của miền giá trị (domain) hợp lệ tại Silver (DATA_CONTRACT §7.2)
YEAR_MIN = 1980                   # cận trên = năm của ngay_duyet + 1, suy ra từ dữ liệu
SEATS_MIN, SEATS_MAX = 1, 64
PRICE_VND_MIN = 50_000_000        # 50 triệu
PRICE_VND_MAX = 50_000_000_000    # 50 tỷ
MAX_APPROVAL_GAP_DAYS = 366       # hieu_luc_tu phải nằm trong ±1 năm quanh ngay_duyet

# ---- Gold / mart_ml_training (README §5.3) -------------------------------------------
ML_TRAIN_END_MONTH = "2026-03"    # mốc chia: tháng duyệt <= mốc -> train, sau mốc -> test
