# SỐ LIỆU BÁO CÁO — sinh tự động

> Sinh bởi `python -m src.report` từ Bronze active + logic Silver/Gold của code hiện tại. **Không sửa tay**
> file này — sửa code/dữ liệu rồi chạy lại. Biểu đồ ở `report/figures/` (Hình 1–4 là sơ đồ, tự vẽ).

## Phạm vi số liệu

| Ký hiệu | Phạm vi | Lô | Ref `ref_model_map` | Dùng cho |
|---|---|---|---|---|
| **(A) Hệ thống** | Trạng thái pipeline hiện tại | 20 lô (2025-01 → 2026-08) | 2026-07 | Số kỹ thuật: Section 3–4, Slide 14–23 |
| **(B) Phát hiện** | Chỉ dữ liệu thật đã kiểm chứng | 18 lô (2025-01 → 2026-06) | 2026-06 | Kết luận về giá xe: Slide 4, 15, 24 |

Lô có thông tin xe **giả định** (dựng từ báo cáo BSS, loại khỏi (B)): 2026-07, 2026-08.
Ref 2026-07 là **đợt duyệt 2 giả định theo luật** (manifest `ref_model_map/version=2026-07`), chưa được
nghiệp vụ xác nhận. Đối chiếu: phạm vi (A) khớp Silver trên đĩa (`silver/_run_info.json`) — đã kiểm.

---

## Slide 4 — Bảng 1: Bốn trở ngại của dữ liệu nghiệp vụ thô · phạm vi (B)

| Trở ngại | Số đo |
|---|---|
| Không có tên xe — chỉ có mã dòng xe nội bộ | 1.426 mã trong ref 2026-06 → 61 dòng xe → 13 hãng; ref phủ 52,2% số mã xuất hiện; 9,76% dòng vào `MODEL_CODE_UNKNOWN` |
| Giá thiếu được ghi thành `0` | 3.012 dòng (5,7%) |
| Hãng xe, kiểu dáng gần như trống; không có hộp số, số km | hãng 0,33% · kiểu dáng 7,79% · hộp số 0% |
| Dữ liệu đến theo lô tháng | 18 lô, 1.982 – 4.401 dòng/lô, tổng 52.791 dòng |

## Slide 15 — Bảng 10: Những gì nguồn không có · phạm vi (B)

| Trường | Tình trạng |
|---|---|
| Hộp số, số km | Không có cột |
| Kiểu dáng (`ma_kieu_dang`) | 7,79% có giá trị |
| Hãng xe dạng chữ (`hang_xe`) | 0,33% có giá trị |
| Mã xe khác nhau trong dữ liệu | 1.975 mã |

## Slide 18 — Bảng 13: Grain và số dòng bảng Silver · phạm vi (A)

| Bảng | Dòng |
|---|---|
| `fact_valuation` | 53.064 |
| `dim_model` | 2.167 (106 dòng xe, 27 hãng) |
| `dim_vehicle_type` | 42 |
| `dim_date` | 1.461 |
| `quarantine` | 6.357 |

## Slide 20 — Bảng 15: Tám luật quarantine

| Luật | (A) dòng | (A) % quarantine | (B) dòng |
|---|---|---|---|
| `CORRUPT_RECORD` | 0 | 0,0 | 0 |
| `DUP_KEY` | 0 | 0,0 | 0 |
| `MISSING_FIELD` | 34 | 0,5 | 34 |
| `DATE_INVALID` | 231 | 3,6 | 225 |
| `OUT_OF_SCOPE` | 0 | 0,0 | 0 |
| `MODEL_CODE_UNKNOWN` | 3.291 | 51,8 | 5.154 |
| `VALUE_INVALID` | 288 | 4,5 | 280 |
| `PRICE_INVALID` | 2.513 | 39,5 | 2.350 |
| **Tổng** | **6.357** | 100 | **8.043** |

Đối soát (2): (A) 59.421 = 53.064 + 6.357 · (B) 52.791 = 44.748 + 8.043.

## Slide 21 — Bảng 16: Ba mart phân tích · phạm vi (A)

| Mart | Dòng |
|---|---|
| `mart_vehicle_price` | 1.417 |
| `mart_monthly_trend` | 1.243 |
| `mart_data_quality` | 20 |

## Slide 22 — Bảng ML · phạm vi (A), mốc chia m₀ = 2026-03

| | Tháng | Dòng | Tỷ lệ | Xe cũ | Dòng xe |
|---|---|---|---|---|---|
| train | 2025-01 → 2026-03 | 38.357 | 72,3% | 26.012 | 106 |
| test | 2026-04 → 2026-08 | 14.707 | 27,7% | 8.919 | 105 |

Dòng xe có trong test nhưng không có trong train: 0.
Hình 9: report/figures/hinh_09_train_test.png

## Slide 23 — Bảng 18: Số dòng qua từng tầng · phạm vi (A)

| Tầng | Bảng | Dòng |
|---|---|---|
| Landing → Bronze | 20/20 lô qua cổng kiểm tra | 59.421 |
| Silver | `fact_valuation` · `quarantine` | 53.064 · 6.357 |
| Gold | 3 mart BI · bảng ML | 1.417 / 1.243 / 20 · 53.064 |

Tỷ lệ quarantine (5): (A) toàn bộ **10,70%**, theo tháng 7,87% – 13,78% ·
(B) toàn bộ **15,24%**, theo tháng 11,28% – 18,28%.

Hình 5: report/figures/hinh_05_quarantine_theo_thang.png

Bảng số của Hình 5 (phạm vi A):

| Tháng | Bronze | Fact | Quarantine | % q | MODEL_CODE_UNKNOWN | PRICE_INVALID |
|---|---|---|---|---|---|---|
| 2025-01 | 3.119 | 2.819 | 300 | 9,62 | 174 | 124 |
| 2025-02 | 1.982 | 1.714 | 268 | 13,52 | 103 | 159 |
| 2025-03 | 2.650 | 2.318 | 332 | 12,53 | 141 | 175 |
| 2025-04 | 2.417 | 2.114 | 303 | 12,54 | 141 | 155 |
| 2025-05 | 2.422 | 2.117 | 305 | 12,59 | 117 | 181 |
| 2025-06 | 2.507 | 2.197 | 310 | 12,37 | 144 | 153 |
| 2025-07 | 2.775 | 2.415 | 360 | 12,97 | 194 | 142 |
| 2025-08 | 2.333 | 2.061 | 272 | 11,66 | 114 | 135 |
| 2025-09 | 2.279 | 2.020 | 259 | 11,36 | 138 | 100 |
| 2025-10 | 3.418 | 3.106 | 312 | 9,13 | 145 | 138 |
| 2025-11 | 3.113 | 2.868 | 245 | 7,87 | 132 | 89 |
| 2025-12 | 4.401 | 3.936 | 465 | 10,57 | 195 | 202 |
| 2026-01 | 3.924 | 3.546 | 378 | 9,63 | 197 | 146 |
| 2026-02 | 2.368 | 2.160 | 208 | 8,78 | 123 | 58 |
| 2026-03 | 3.440 | 2.966 | 474 | 13,78 | 156 | 183 |
| 2026-04 | 3.201 | 2.918 | 283 | 8,84 | 165 | 99 |
| 2026-05 | 3.048 | 2.753 | 295 | 9,68 | 153 | 117 |
| 2026-06 | 3.394 | 2.994 | 400 | 11,79 | 181 | 157 |
| 2026-07 | 3.695 | 3.388 | 307 | 8,31 | 301 | 0 |
| 2026-08 | 2.935 | 2.654 | 281 | 9,57 | 277 | 0 |

## Slide 24 — Phát hiện từ dữ liệu · phạm vi (B)

1. **Tỷ lệ còn lại ρ** (3): 115/951 nhóm có đủ giá mới lẫn giá cũ; trung vị ρ = **0,938**, khoảng 0,470 – 1,113.
2. **13 nhóm có ρ > 1** (bảng dưới).
3. **Chất lượng phụ thuộc bảng tham chiếu**: 64% số dòng quarantine là `MODEL_CODE_UNKNOWN`.

| Đời xe | Số nhóm đủ hai phía | Trung vị ρ |
|---|---|---|
| 2024 | 36 | 0,926 |
| 2025 | 67 | 0,938 |
| 2026 | 12 | 0,979 |

Hình 6: report/figures/hinh_06_rho_theo_doi_xe.png

Nhóm có ρ > 1:

| Hãng | Dòng xe | Đời | Chỗ | HĐ mới | HĐ cũ | ρ |
|---|---|---|---|---|---|---|
| VinFast | Limo Green | 2026 | 7 | 988 | 1 | 1,004 |
| Mitsubishi | Xforce | 2025 | 5 | 461 | 87 | 1,003 |
| Hyundai | Accent | 2025 | 5 | 379 | 49 | 1,019 |
| Mazda | 3 | 2025 | 5 | 242 | 57 | 1,010 |
| Toyota | Vios | 2024 | 5 | 3 | 210 | 1,027 |
| Ford | Everest | 2025 | 7 | 130 | 48 | 1,006 |
| Hyundai | Creta | 2026 | 5 | 126 | 14 | 1,082 |
| Hyundai | Tucson | 2025 | 5 | 99 | 23 | 1,008 |
| Honda | HRV | 2025 | 5 | 37 | 18 | 1,004 |
| Kia | Carnival | 2026 | 7 | 25 | 1 | 1,106 |
| Kia | Sportage | 2025 | 5 | 14 | 10 | 1,007 |
| BYD | Seal 5 | 2024 | 7 | 1 | 4 | 1,041 |
| VinFast | VF7 | 2025 | 7 | 3 | 1 | 1,113 |

Hình 8 (Q2 — số hợp đồng theo tháng, phạm vi B): report/figures/hinh_08_hop_dong_theo_thang.png

## Slide mới — Phiên bản reference data (ARCHITECTURE §4.2) · 20 lô

| Ref | Mã phiên bản | Dòng xe | Hãng | Fact | Quarantine | % q | MODEL_CODE_UNKNOWN |
|---|---|---|---|---|---|---|---|
| 2026-06 | 1.426 | 61 | 13 | 50.320 | 9.101 | 15,32 | 6.202 |
| 2026-07 | 2.167 | 106 | 27 | 53.064 | 6.357 | 10,70 | 3.291 |

Hình 7: report/figures/hinh_07_phien_ban_ref.png

Sổ lần chạy `data/control/run_log.csv`:

| run_id | ref_as_of | ref_model_map_version | train_end_month | fact | quarantine | ty_le_quarantine_pct | ml_train | ml_test |
|---|---|---|---|---|---|---|---|---|
| 20261004T204145 | 2026-06 | 2026-06 | 2026-03 | 50320 | 9101 | 15.32 | 36525 | 13795 |
| 20261004T204235 | latest | 2026-07 | 2026-03 | 53064 | 6357 | 10.7 | 38357 | 14707 |
| 20261004T204319 | latest | 2026-07 | 2026-05 | 53064 | 6357 | 10.7 | 44028 | 9036 |
| 20261004T204607 | latest | 2026-07 | 2026-03 | 53064 | 6357 | 10.7 | 38357 | 14707 |

---

## Danh mục hình sinh tự động

| Hình | File | Bảng số đi kèm |
|---|---|---|
| Hình 5 — Tỷ lệ quarantine theo tháng × luật (A) | `report/figures/hinh_05_quarantine_theo_thang.png` | Slide 23 |
| Hình 6 — ρ theo đời xe (B) | `report/figures/hinh_06_rho_theo_doi_xe.png` | Slide 24 |
| Hình 7 — Quarantine theo tháng, ref 2026-06 so với 2026-07 | `report/figures/hinh_07_phien_ban_ref.png` | Slide phiên bản ref |
| Hình 8 — Số hợp đồng theo tháng, mới/cũ (B) | `report/figures/hinh_08_hop_dong_theo_thang.png` | — (đếm từ fact) |
| Hình 9 — Train/test theo tháng (A) | `report/figures/hinh_09_train_test.png` | Slide 22 |
