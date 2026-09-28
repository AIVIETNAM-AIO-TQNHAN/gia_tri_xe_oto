# Data Contract — `business_vehicle_valuation` (rút gọn)

| | |
|---|---|
| **Phiên bản** | `1.0.0` — cùng schema với đề tài cũ, rút gọn phần luật |
| **Bên cung cấp** | Ban Nghiệp vụ Xe — bản trích từ CSDL nghiệp vụ xe. **Cách trích ngoài phạm vi** |
| **Bên tiêu thụ** | Pipeline Medallion của đề tài này |
| **Nhịp** | 1 lô / tháng, hạn trước ngày 05 tháng kế tiếp |
| **Hiện có** | 18 lô `2025-01` → `2026-06`, **52.791 dòng** (17 lô backfill + 1 lô thường kỳ) |

Contract nói **file bàn giao phải thế nào** và **luật nào dùng để nghiệm thu nó**. Luật ở §6 chạy
trước Bronze (từ chối cả lô); luật ở §7 chạy ở Silver (tách từng dòng).

---

## 1. Nội dung lô — bên cung cấp cam kết

1. Chỉ hợp đồng bảo hiểm **vật chất xe** (`ma_nghiep_vu = '070104'`): số tiền bảo hiểm của loại này
   **bằng giá trị chiếc xe**, nên dùng được làm giá xe.
2. Chỉ **ô tô chở người** (`nhom_xe` có `trong_pham_vi = 1` trong `ref_veh_type_scope.csv`).
3. **Một dòng / hợp đồng**; giá là giá trị hiện hành, không cộng dồn các lần sửa đổi.
4. **Không** có dữ liệu cá nhân hay định danh xe: tên, địa chỉ, SĐT, biển số, số khung, số máy, số
   hợp đồng, mã nhân viên/đại lý.

## 2. Định dạng file

| Thuộc tính | Giá trị |
|---|---|
| Định dạng | CSV có header |
| Encoding | UTF-8 có BOM (`utf-8-sig`) |
| Delimiter | `,` · bao chuỗi `"` · escape `""` |
| Số | Số trần, không phân cách nghìn: `872000000` |
| Ngày | `yyyy-mm-dd` |
| Rỗng | Ô trống — không dùng `NULL`, `N/A`, `-` |

Đường dẫn:

```
data/landing/business_vehicle_valuation/
└── as_of_month=YYYY-MM/
    ├── business_vehicle_valuation_YYYYMM.csv
    └── _manifest.json
```

File landing **bất biến**. Lô sửa lại = thư mục/lô mới với `batch_id` mới, không ghi đè.

## 3. Schema — 16 cột, đúng thứ tự

| # | Cột | Kiểu ở Silver | Ý nghĩa | Ghi chú |
|---|---|---|---|---|
| 1 | `ma_nghiep_vu` | — | Mã loại bảo hiểm | Hằng `070104` |
| 2 | `nhom_xe` | → `vehicle_type_key` | Mã loại xe | Tra `dim_vehicle_type` |
| 3 | `hang_xe` | — | Hãng xe (thô) | Chỉ phủ 0,33% → **không dùng**; hãng lấy từ `ma_dong_xe` |
| 4 | `ma_dong_xe` | → `model_key` | Mã dòng xe nội bộ | Tra `dim_model` ra hãng + dòng xe |
| 5 | `ma_kieu_dang` | — | Mã kiểu dáng | Phủ 7,79% → không dùng |
| 6 | `so_cho_ngoi` | int | Số chỗ | `[1, 64]` |
| 7 | `nam_san_xuat` | int | Đời xe | `[1980, năm duyệt + 1]` |
| 8 | `gia_tri_dinh_gia` | double → **triệu VND** | Số tiền bảo hiểm = giá trị xe (VND) | `[50 triệu, 50 tỷ]`; 5,7% dòng thô = `0` |
| 9 | `ngay_duyet` | date → `ngay_duyet_key` | Ngày duyệt đơn = lúc định giá | Nguồn của partition `as_of_month` |
| 10 | `hieu_luc_tu` | date → `hieu_luc_tu_key` | Bắt đầu hiệu lực | |
| 11 | `hieu_luc_den` | date | Hết hiệu lực | Rỗng = còn hiệu lực |
| 12 | `ky_bao_cao` | — | Kỳ snapshot nguồn | Chỉ để truy vết |
| 13 | `ma_dinh_gia` | string | Khoá hợp đồng (băm, không lộ số hợp đồng thật) | Duy nhất |
| 14 | `don_vi_tinh` | — | Hằng `VND` | |
| 15 | `loai_dinh_gia` | string | `new` / `used` | **Suy luận** (3.2) |
| 16 | `as_of_month` | string | `YYYY-MM` | Khớp tên thư mục |

### 3.1 Cột kỹ thuật do Bronze gắn thêm

`_batch_id` (từ manifest) · `_source_file` · `_ingest_ts` (lúc nạp; chỉ là metadata, không tham gia
tính toán nghiệp vụ) · `_corrupt_record` (dòng vỡ cột).

### 3.2 `loai_dinh_gia` là suy luận

CSDL nghiệp vụ không ghi xe mới hay cũ. Bên cung cấp suy ra: `new` nếu `nam_san_xuat ≥
năm(hieu_luc_tu)`, ngược lại `used`. Luật này **chưa được nghiệp vụ xác nhận** — mọi kết luận về
khấu hao ở Gold phụ thuộc vào nó (xem ARCHITECTURE §4).

## 4. Manifest

```json
{
  "batch_id": "bvv_202606_01",
  "schema_version": "1.0.0",
  "as_of_month": "2026-06",
  "record_count": 3394,
  "file_name": "business_vehicle_valuation_202606.csv",
  "file_sha256": "0021a365…",
  "encoding": "utf-8-sig",
  "delimiter": ",",
  "…": "các field truy vết khác của bên cung cấp — pipeline không dựa vào"
}
```

Bảy field `batch_id`, `as_of_month`, `record_count`, `file_name`, `file_sha256`, `encoding`,
`delimiter` là **bắt buộc**. `encoding`/`delimiter` là tham số đọc CSV — job **không hard-code**.

## 5. Khoá

| Khoá | Cột | Duy nhất? |
|---|---|---|
| Khoá lô | `batch_id` | ✅ — chống nạp trùng |
| Khoá dòng | `ma_dinh_gia` | ✅ — trùng thì giữ 1, còn lại `DUP_KEY` |

## 6. Cổng kiểm tra lô — REJECT cả lô ([src/validate.py](../src/validate.py))

| `rule_id` | Vi phạm |
|---|---|
| `MANIFEST_INVALID` | Không có/không đọc được `_manifest.json`, thiếu field bắt buộc, hoặc không thấy file dữ liệu |
| `CHECKSUM_MISMATCH` | sha256 của file ≠ `file_sha256` — file hỏng, truyền thiếu, hoặc bị sửa tay |
| `SCHEMA_COLUMN_MISSING` | Header thiếu cột ở §3 |
| `COUNT_MISMATCH` | Số dòng dữ liệu ≠ `record_count` |
| `PARTITION_MISMATCH` | Tên thư mục, `as_of_month` của manifest và cột `as_of_month` không trùng nhau |

Lô bị từ chối: **không nạp, không tự sửa**, file nằm nguyên ở landing, bên cung cấp gửi lại lô mới.
Các lô khác vẫn nạp bình thường.

## 7. Luật dòng — QUARANTINE ở Silver ([src/silver.py](../src/silver.py))

8 luật, thứ tự ưu tiên và số đo trên dữ liệu thật: [ARCHITECTURE.md §3.5](ARCHITECTURE.md#35-luật-quarantine).
Lô vẫn được nạp; dòng vi phạm vào `silver/quarantine` kèm `rule_id`, **không bị xoá**.

## 8. Hạn chế đã biết

| Hạn chế | Hệ quả |
|---|---|
| `ref_model_map` mới phủ ~52% số mã, ~90% số dòng | 9,76% dòng vào `MODEL_CODE_UNKNOWN`. Bổ sung bảng rồi chạy lại là cứu được |
| Không có hộp số, kiểu dáng (7,79%), số km | Giá nghiệp vụ là một con số cho cả dòng/đời xe, không phân biệt bản số sàn/tự động hay xe chạy nhiều/ít |
| Bên cung cấp đã bỏ 189 dòng có mã xe không có trong bảng giá tham khảo | Pipeline không thấy các dòng này |
| Bên cung cấp thực tế là chính nhóm | Quy trình "từ chối → gửi lại" là kỷ luật tự đặt |
