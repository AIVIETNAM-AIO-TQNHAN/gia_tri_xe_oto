# Data Contract — `business_vehicle_valuation`

| | |
|---|---|
| **Phiên bản** | `1.0.0` |
| **Bên cung cấp** | Ban Nghiệp vụ Xe — bản trích từ CSDL nghiệp vụ xe. **Phần Extract ngoài phạm vi** |
| **Bên tiêu thụ** | Pipeline Medallion của đề tài này |
| **Nhịp** | 1 lô / tháng, hạn trước ngày 05 tháng kế tiếp |
| **Hiện có** | 18 lô `2025-01` → `2026-06`, **52.791 dòng** (17 lô backfill + 1 lô thường kỳ) |

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
| 15 | `loai_dinh_gia` | string | `new` / `used` | **Suy luận**, luật đã xác nhận (3.2) |
| 16 | `as_of_month` | string | `YYYY-MM` | Khớp tên thư mục |

### 3.1 Cột kỹ thuật do Bronze gắn thêm

`_batch_id` (từ manifest) · `_source_file` · `_ingest_ts` (lúc nạp; chỉ là metadata, không tham gia
tính toán nghiệp vụ) · `_corrupt_record` (dòng vỡ cột).

### 3.2 `loai_dinh_gia` là suy luận

`new` nếu `nam_san_xuat ≥ năm(hieu_luc_tu)`, ngược lại `used`.

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

Lô đã qua cổng §6 thì được nạp vào Bronze nguyên vẹn. Sang Silver, **từng dòng** được kiểm theo 8
luật dưới đây: dòng sạch vào `fact_valuation`, dòng vi phạm vào `silver/quarantine` kèm `rule_id`.
Khác §6 ở chỗ: một dòng lỗi **không kéo cả lô** bị từ chối, và dòng lỗi **không bị xoá**.

| | §6 Cổng lô | §7 Luật dòng |
|---|---|---|
| Chạy ở | Trước Bronze | Silver |
| Đơn vị xử lý | Cả lô | Từng dòng |
| Khi vi phạm | REJECT — không nạp, bên cung cấp gửi lại | QUARANTINE — tách riêng, các dòng khác vẫn vào fact |
| Sửa bằng cách | Gửi lô mới (`batch_id` mới) | Sửa `ref_*` hoặc luật rồi chạy lại Silver |

### 7.1 Một dòng — một `rule_id`

Một dòng có thể vi phạm nhiều luật cùng lúc, nhưng chỉ được ghi **luật đầu tiên** theo thứ tự ở §7.2
(thứ tự này là `silver.RULES`). Nhờ vậy mỗi dòng Bronze ra đúng một dòng quarantine, và đếm theo
`rule_id` cộng lại **đúng bằng** tổng số dòng bị tách.

Thứ tự đi từ lỗi **cấu trúc** (đọc được không, khoá có duy nhất không, đủ trường chưa, ngày hợp lệ
không) → lỗi **tra cứu** (có thuộc phạm vi, có giải mã được mã xe không) → lỗi **giá trị** (số nằm
trong miền hợp lệ không).

Trước khi kiểm, mọi giá trị được `trim`. Giá trị có nhưng **không chuyển được** sang đúng kiểu (vd.
`nam_san_xuat = "abc"`) được coi là **vi phạm** của luật kiểm miền giá trị, không bị bỏ qua.

### 7.2 Tám luật

| # | `rule_id` | Vi phạm khi | Vì sao cần luật này | 18 lô thật |
|---|---|---|---|---|
| 1 | `CORRUPT_RECORD` | Dòng vỡ cột: Spark đọc ở chế độ PERMISSIVE và đẩy dòng vào cột `_corrupt_record` | Dòng không tách được thành 16 cột thì không cột nào đáng tin | 0 |
| 2 | `DUP_KEY` | `ma_dinh_gia` rỗng, **hoặc** trùng với dòng khác. Khi trùng, giữ **một** dòng theo thứ tự: `hieu_luc_tu` muộn nhất → giá lớn hơn → `_batch_id` lớn hơn; các dòng còn lại vào quarantine | Grain của fact là 1 hợp đồng (§1 mục 3, §5). Thứ tự chọn là tất định, không phụ thuộc thứ tự đọc file | 0 |
| 3 | `MISSING_FIELD` | Rỗng ở `ma_dong_xe`, `nam_san_xuat` hoặc `so_cho_ngoi` | Ba trường tối thiểu để biết đây là xe gì | 34 |
| 4 | `DATE_INVALID` | Một trong các trường hợp: `ngay_duyet` hoặc `hieu_luc_tu` rỗng/sai dạng `yyyy-mm-dd` · `hieu_luc_den` có giá trị nhưng sai dạng · `hieu_luc_den < hieu_luc_tu` · `hieu_luc_tu` lệch `ngay_duyet` quá **366 ngày** | `ngay_duyet` quyết định tháng và tuổi xe; ngày sai thì dòng rơi vào sai kỳ | 225 |
| 5 | `OUT_OF_SCOPE` | `nhom_xe` không có trong `dim_vehicle_type`, hoặc có nhưng `trong_pham_vi ≠ 1` | Bên cung cấp cam kết chỉ gửi ô tô chở người (§1 mục 2); luật này kiểm lại cam kết đó | 0 |
| 6 | `MODEL_CODE_UNKNOWN` | `ma_dong_xe` không có trong `dim_model` (tức `ref_model_map.csv`) | Hãng và dòng xe **chỉ** giải mã được từ mã này. Không giải mã được thì dòng không trả lời được câu hỏi "giá xe hãng X, dòng Y" | **5.154** |
| 7 | `VALUE_INVALID` | `nam_san_xuat ∉ [1980, năm(ngay_duyet) + 1]` · `so_cho_ngoi ∉ [1, 64]` · `loai_dinh_gia ∉ {new, used}` | Loại giá trị vô lý: xe sản xuất sau năm định giá hơn 1 năm, xe 0 chỗ… | 280 |
| 8 | `PRICE_INVALID` | `gia_tri_dinh_gia` sai dạng số hoặc ngoài `[50 triệu, 50 tỷ]` VND | Giá là measure duy nhất của fact; `0` hoặc số vô lý kéo lệch mọi thống kê | **2.350** |
| | | | **Tổng quarantine** | **8.043 / 52.791 (15,24%)** |

Còn lại **44.748 dòng** vào `fact_valuation`.

**Ghi chú khi đọc số đo**

- Dữ liệu thô có **3.012** dòng giá `0` (5,7%, §3), nhưng chỉ **2.350** dòng mang `PRICE_INVALID`.
  Số còn lại đã dính một luật đứng trước (thường là `MODEL_CODE_UNKNOWN`) — hệ quả của §7.1.
- Cận trên của `nam_san_xuat` lấy từ **năm của `ngay_duyet`**, không lấy từ ngày chạy job. Chạy
  lại lô cũ vào năm sau vẫn ra đúng kết quả cũ.
- Các ngưỡng (1980, 1–64 chỗ, 50 triệu–50 tỷ, 366 ngày) nằm ở [src/config.py](../src/config.py),
  không viết cứng trong luật.

### 7.3 Dòng quarantine chứa gì, cứu thế nào

- Giữ **nguyên mọi cột thô** như ở Bronze (16 cột §3 + cột kỹ thuật §3.1) và thêm cột `rule_id`.
  Không cột nào bị parse hay sửa, nên nhìn vào là thấy đúng giá trị bên cung cấp đã gửi.
- Lưu ở `silver/quarantine/`, chia thư mục theo `as_of_month` như fact.
- Silver ghi **full refresh**: mỗi lần chạy tính lại từ đầu. Vì vậy, sau khi bổ sung
  `ref_model_map.csv` hay chỉnh một luật, chỉ cần chạy lại Silver là các dòng đủ điều kiện **tự
  chuyển về fact** — không phải sửa tay dòng nào, không phải xin bên cung cấp gửi lại lô.
- Lỗi nằm ở chính dữ liệu gốc (giá `0`, ngày sai…) thì chỉ bên cung cấp sửa được, bằng một lô mới.

### 7.4 Đối soát — không dòng nào được biến mất

Silver chỉ kiểm **Bronze active**: mỗi tháng lấy bản nạp mới nhất (khi một tháng được gửi lại, bản cũ
vẫn nằm ở Bronze để truy vết nhưng không được dùng). Sau khi tách, job kiểm:

```
số dòng fact + số dòng quarantine = số dòng Bronze active
```

Lệch là **dừng job**, không ghi gì. Mọi dòng nhận vào phải nằm ở đúng một trong hai nơi.

## 8. Hạn chế đã biết

| Hạn chế | Hệ quả |
|---|---|
| `ref_model_map` mới phủ ~52% số mã, ~90% số dòng | 9,76% dòng vào `MODEL_CODE_UNKNOWN`. Bổ sung bảng rồi chạy lại là cứu được |
| Không có hộp số, kiểu dáng (7,79%), số km | Giá nghiệp vụ là một con số cho cả dòng/đời xe, không phân biệt bản số sàn/tự động hay xe chạy nhiều/ít |
| Bên cung cấp đã bỏ 189 dòng có mã xe không có trong bảng giá tham khảo | Pipeline không thấy các dòng này |
| Bên cung cấp thực tế là chính nhóm | Quy trình "từ chối → gửi lại" là kỷ luật tự đặt |
