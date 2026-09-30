# Data Contract — `business_vehicle_valuation`

| | |
|---|---|
| **Phiên bản** (version) | `1.0.0` |
| **Bên cung cấp** (data provider) | Ban Nghiệp vụ Xe — bản trích xuất từ cơ sở dữ liệu nghiệp vụ xe. **Bước Extract nằm ngoài phạm vi đề tài** |
| **Bên tiêu thụ** (data consumer) | Pipeline Medallion của đề tài này |
| **Tần suất** (frequency) | Batch ingestion, 1 batch / tháng, hạn chót trước ngày 05 của tháng kế tiếp |
| **Hiện trạng** | 18 batch từ `2025-01` đến `2026-06`, tổng cộng **52.791 record** (17 batch backfill + 1 batch định kỳ) |

---

## 1. Nội dung batch — cam kết của bên cung cấp

1. Chỉ bao gồm hợp đồng bảo hiểm **vật chất xe** (`ma_nghiep_vu = '070104'`). Với loại hình này, số
   tiền bảo hiểm **bằng giá trị của chiếc xe**, do đó có thể được sử dụng làm giá xe.
2. Chỉ bao gồm **ô tô chở người** (`nhom_xe` có `trong_pham_vi = 1` trong `ref_veh_type_scope.csv`).
3. **Một record tương ứng với một hợp đồng**; giá trị là giá trị hiện hành, không cộng dồn qua các
   lần sửa đổi hợp đồng.
4. **Không** chứa dữ liệu cá nhân (PII) hay dữ liệu định danh phương tiện: họ tên, địa chỉ, số điện
   thoại, biển số, số khung, số máy, số hợp đồng, mã nhân viên/đại lý.

## 2. Định dạng tệp (file format)

| Thuộc tính | Giá trị |
|---|---|
| Định dạng | Flat file CSV có header |
| Mã hoá ký tự (encoding) | UTF-8 có BOM (`utf-8-sig`) |
| Ký tự phân tách (delimiter) | `,` · ký tự bao chuỗi (quote) `"` · ký tự thoát (escape) `""` |
| Kiểu số | Số không có ký tự phân cách hàng nghìn: `872000000` |
| Kiểu ngày | `yyyy-mm-dd` |
| Giá trị rỗng (null) | Ô trống — không dùng `NULL`, `N/A`, `-` |

Cấu trúc thư mục tại vùng tiếp nhận (landing zone):

```
data/landing/business_vehicle_valuation/
└── as_of_month=YYYY-MM/
    ├── business_vehicle_valuation_YYYYMM.csv
    └── _manifest.json
```

Tệp tại landing zone là **bất biến** (immutable). Một batch được sửa lại phải được gửi dưới dạng thư
mục/batch mới mang `batch_id` mới, không ghi đè (overwrite) lên batch cũ.

## 3. Lược đồ (schema) — 16 cột, đúng thứ tự

| # | Cột | Kiểu dữ liệu tại Silver | Ý nghĩa | Ghi chú |
|---|---|---|---|---|
| 1 | `ma_nghiep_vu` | — | Mã loại hình bảo hiểm | Hằng số `070104` |
| 2 | `nhom_xe` | → `vehicle_type_key` | Mã loại xe | Lookup qua `dim_vehicle_type` |
| 3 | `hang_xe` | — | Hãng xe (dữ liệu thô) | Độ phủ (coverage) chỉ 0,33% → **không sử dụng**; hãng xe được suy ra từ `ma_dong_xe` |
| 4 | `ma_dong_xe` | → `model_key` | Mã dòng xe nội bộ | Lookup qua `dim_model` để suy ra hãng và dòng xe |
| 5 | `ma_kieu_dang` | — | Mã kiểu dáng | Coverage 7,79% → không sử dụng |
| 6 | `so_cho_ngoi` | int | Số chỗ ngồi | `[1, 64]` |
| 7 | `nam_san_xuat` | int | Năm sản xuất | `[1980, năm duyệt + 1]` |
| 8 | `gia_tri_dinh_gia` | double → **triệu VND** | Số tiền bảo hiểm = giá trị xe (VND) | `[50 triệu, 50 tỷ]`; 5,7% record thô có giá trị `0` |
| 9 | `ngay_duyet` | date → `ngay_duyet_key` | Ngày duyệt đơn = thời điểm định giá | Nguồn xác định partition `as_of_month` |
| 10 | `hieu_luc_tu` | date → `hieu_luc_tu_key` | Ngày bắt đầu hiệu lực | |
| 11 | `hieu_luc_den` | date | Ngày hết hiệu lực | Rỗng = còn hiệu lực |
| 12 | `ky_bao_cao` | — | Kỳ snapshot tại nguồn | Chỉ phục vụ truy vết (lineage) |
| 13 | `ma_dinh_gia` | string | Khoá hợp đồng (đã băm — hashed, không làm lộ số hợp đồng thật) | Duy nhất (unique) |
| 14 | `don_vi_tinh` | — | Hằng số `VND` | |
| 15 | `loai_dinh_gia` | string | `new` / `used` | **Thuộc tính suy diễn** (derived attribute), quy tắc đã được xác nhận (§3.2) |
| 16 | `as_of_month` | string | `YYYY-MM` | Phải khớp với tên thư mục |

### 3.1 Cột kỹ thuật (technical column) do tầng Bronze bổ sung

`_batch_id` (lấy từ manifest) · `_source_file` · `_ingest_ts` (thời điểm ingest; chỉ là metadata,
không tham gia tính toán nghiệp vụ) · `_corrupt_record` (record bị vỡ cột).

### 3.2 `loai_dinh_gia` là thuộc tính suy diễn

`new` nếu `nam_san_xuat ≥ năm(hieu_luc_tu)`, ngược lại là `used`.

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
  "…": "các field truy vết khác của bên cung cấp — pipeline không phụ thuộc vào"
}
```

Bảy field `batch_id`, `as_of_month`, `record_count`, `file_name`, `file_sha256`, `encoding`,
`delimiter` là **bắt buộc** (required). `encoding` và `delimiter` là tham số đọc CSV — job **không
hard-code** các giá trị này.

## 5. Khoá (key)

| Khoá | Cột | Tính duy nhất (uniqueness) |
|---|---|---|
| Batch key | `batch_id` | ✅ — ngăn ingest trùng lặp một batch |
| Record key | `ma_dinh_gia` | ✅ — khi trùng, giữ lại một record, các record còn lại được gán `DUP_KEY` |

## 6. Batch-level validation — REJECT toàn bộ batch ([src/validate.py](../src/validate.py))

| `rule_id` | Điều kiện vi phạm |
|---|---|
| `MANIFEST_INVALID` | Thiếu hoặc không đọc được `_manifest.json`, thiếu field bắt buộc, hoặc không tìm thấy tệp dữ liệu |
| `CHECKSUM_MISMATCH` | Giá trị SHA-256 của tệp ≠ `file_sha256` — tệp bị hỏng, truyền tải không đầy đủ, hoặc bị chỉnh sửa thủ công |
| `SCHEMA_COLUMN_MISSING` | Header thiếu cột so với schema tại §3 |
| `COUNT_MISMATCH` | Số record dữ liệu ≠ `record_count` |
| `PARTITION_MISMATCH` | Tên thư mục, `as_of_month` trong manifest và cột `as_of_month` không nhất quán |

Batch bị reject sẽ **không được ingest và không được tự động sửa**; tệp được giữ nguyên tại landing
zone và bên cung cấp có trách nhiệm gửi lại một batch mới. Các batch khác vẫn được ingest bình
thường.

## 7. Record-level validation — QUARANTINE tại Silver ([src/silver.py](../src/silver.py))

Batch đã vượt qua batch-level validation (§6) được ingest nguyên vẹn vào tầng Bronze. Tại tầng
Silver, **từng record** được kiểm tra theo 8 rule dưới đây: record hợp lệ được ghi vào
`fact_valuation`, record vi phạm được ghi vào `silver/quarantine` kèm `rule_id`. Điểm khác biệt so
với §6: một record lỗi **không khiến toàn bộ batch bị reject**, và record lỗi **không bị xoá bỏ**.

| | §6 Batch-level validation | §7 Record-level validation |
|---|---|---|
| Vị trí thực thi | Trước Bronze | Silver |
| Đơn vị xử lý (granularity) | Toàn bộ batch | Từng record |
| Hành động khi vi phạm | REJECT — không ingest, bên cung cấp gửi lại | QUARANTINE — tách riêng, các record khác vẫn được ghi vào fact |
| Phương thức khắc phục | Gửi batch mới (`batch_id` mới) | Cập nhật `ref_*` hoặc rule, sau đó re-run Silver |

### 7.1 Một record — một `rule_id`

Một record có thể đồng thời vi phạm nhiều rule, nhưng chỉ **rule đầu tiên** theo thứ tự tại §7.2
được ghi nhận (thứ tự này được định nghĩa trong `silver.RULES`). Nhờ đó, mỗi record tại Bronze sinh
ra đúng một record quarantine, và tổng số record đếm theo `rule_id` **bằng đúng** tổng số record bị
tách ra.

Thứ tự kiểm tra đi từ lỗi **cấu trúc** (structural: record có parse được không, khoá có duy nhất
không, có đủ trường bắt buộc không, ngày có hợp lệ không) → lỗi **tham chiếu** (referential: có thuộc
phạm vi không, mã xe có lookup được không) → lỗi **miền giá trị** (domain: giá trị số có nằm trong
miền hợp lệ không).

Trước khi kiểm tra, mọi giá trị đều được `trim`. Giá trị không rỗng nhưng **không thể chuyển đổi**
(cast) sang đúng kiểu dữ liệu (ví dụ `nam_san_xuat = "abc"`) được xem là **vi phạm** rule kiểm tra
miền giá trị tương ứng, chứ không bị bỏ qua.

### 7.2 Tám rule

| # | `rule_id` | Điều kiện vi phạm | Cơ sở của rule | 18 batch thực tế |
|---|---|---|---|---|
| 1 | `CORRUPT_RECORD` | Record bị vỡ cột: Spark đọc ở chế độ PERMISSIVE và đưa record vào cột `_corrupt_record` | Record không tách được thành 16 cột thì không cột nào đáng tin cậy | 0 |
| 2 | `DUP_KEY` | `ma_dinh_gia` rỗng, **hoặc** trùng với record khác. Khi trùng, giữ lại **một** record theo thứ tự ưu tiên: `hieu_luc_tu` muộn nhất → giá lớn hơn → `_batch_id` lớn hơn; các record còn lại được đưa vào quarantine | Grain của fact là một hợp đồng (§1 mục 3, §5). Thứ tự chọn là tất định (deterministic), không phụ thuộc thứ tự đọc tệp | 0 |
| 3 | `MISSING_FIELD` | Rỗng tại `ma_dong_xe`, `nam_san_xuat` hoặc `so_cho_ngoi` | Ba trường tối thiểu để xác định phương tiện | 34 |
| 4 | `DATE_INVALID` | Một trong các trường hợp: `ngay_duyet` hoặc `hieu_luc_tu` rỗng/sai định dạng `yyyy-mm-dd` · `hieu_luc_den` có giá trị nhưng sai định dạng · `hieu_luc_den < hieu_luc_tu` · `hieu_luc_tu` lệch `ngay_duyet` quá **366 ngày** | `ngay_duyet` quyết định kỳ báo cáo và tuổi xe; ngày sai khiến record bị xếp vào sai kỳ | 225 |
| 5 | `OUT_OF_SCOPE` | `nhom_xe` không tồn tại trong `dim_vehicle_type`, hoặc tồn tại nhưng `trong_pham_vi ≠ 1` | Bên cung cấp cam kết chỉ gửi ô tô chở người (§1 mục 2); rule này kiểm chứng lại cam kết đó | 0 |
| 6 | `MODEL_CODE_UNKNOWN` | `ma_dong_xe` không tồn tại trong `dim_model` (tức `ref_model_map.csv`) | Hãng và dòng xe **chỉ** có thể được giải mã từ mã này. Không giải mã được thì record không trả lời được câu hỏi "giá xe hãng X, dòng Y" | **5.154** |
| 7 | `VALUE_INVALID` | `nam_san_xuat ∉ [1980, năm(ngay_duyet) + 1]` · `so_cho_ngoi ∉ [1, 64]` · `loai_dinh_gia ∉ {new, used}` | Loại bỏ các giá trị phi lý: xe sản xuất sau năm định giá hơn một năm, xe 0 chỗ… | 280 |
| 8 | `PRICE_INVALID` | `gia_tri_dinh_gia` sai định dạng số hoặc nằm ngoài `[50 triệu, 50 tỷ]` VND | Giá là measure duy nhất của fact; giá trị `0` hoặc phi lý làm sai lệch mọi thống kê | **2.350** |
| | | | **Tổng quarantine** | **8.043 / 52.791 (15,24%)** |

**44.748 record** còn lại được ghi vào `fact_valuation`.

**Lưu ý khi diễn giải số liệu**

- Dữ liệu thô có **3.012** record với giá bằng `0` (5,7%, §3), nhưng chỉ **2.350** record mang
  `PRICE_INVALID`. Các record còn lại đã vi phạm một rule đứng trước (chủ yếu là
  `MODEL_CODE_UNKNOWN`) — hệ quả của nguyên tắc tại §7.1.
- Cận trên của `nam_san_xuat` được xác định theo **năm của `ngay_duyet`**, không theo ngày thực thi
  job. Nhờ đó, việc re-run một batch cũ vào năm sau vẫn cho kết quả như cũ (reproducibility).
- Các ngưỡng (threshold) — 1980, 1–64 chỗ, 50 triệu–50 tỷ, 366 ngày — được khai báo tập trung tại
  [src/config.py](../src/config.py), không hard-code trong rule.

### 7.3 Cấu trúc record quarantine và cơ chế khôi phục (recovery)

Mỗi record quarantine bảo toàn **toàn bộ thuộc tính thô** (raw attribute) như tại tầng Bronze (16 cột
định nghĩa ở §3 và các technical column ở §3.1), đồng thời được bổ sung thuộc tính `rule_id` xác định
rule bị vi phạm. Không thuộc tính nào trải qua bước chuyển đổi kiểu (parse/cast) hay hiệu chỉnh; do
đó record quarantine phản ánh chính xác giá trị do bên cung cấp gửi đến, phục vụ cho việc truy vết
(lineage) và phân tích nguyên nhân gốc (root-cause analysis).

Dữ liệu quarantine được lưu tại `silver/quarantine/` và được phân vùng (partition) theo
`as_of_month`, nhất quán với cách phân vùng của `fact_valuation`.

Tầng Silver được ghi theo cơ chế **làm mới toàn phần** (full refresh): ở mỗi lần thực thi, toàn bộ
kết quả được tính lại từ Bronze active. Tính chất này cho phép phân biệt hai nhóm nguyên nhân lỗi
với hai phương thức khôi phục tương ứng:

- **Lỗi thuộc dữ liệu tham chiếu (reference data) hoặc định nghĩa rule** (điển hình là
  `MODEL_CODE_UNKNOWN` do `ref_model_map.csv` chưa đầy đủ): sau khi bổ sung reference data hoặc điều
  chỉnh rule, việc re-run tầng Silver là đủ để các record thoả mãn điều kiện được **tự động chuyển về
  fact table**, mà không cần can thiệp thủ công trên từng record hay yêu cầu bên cung cấp gửi lại
  batch.
- **Lỗi thuộc bản thân dữ liệu nguồn** (source data — giá trị định giá bằng `0`, ngày không hợp
  lệ…): chỉ có thể được khắc phục từ phía bên cung cấp, thông qua một batch mới mang `batch_id` mới
  (§2).

### 7.4 Đối soát (reconciliation) và bất biến bảo toàn record

Phạm vi kiểm tra của tầng Silver là **Bronze active**, tức là với mỗi tháng, chỉ lần ingest gần nhất
được sử dụng. Khi một tháng được gửi lại, các lần ingest trước vẫn được lưu giữ tại Bronze nhằm phục
vụ truy vết (audit trail), song không tham gia vào quá trình xử lý.

Sau bước phân tách, job kiểm tra bất biến (invariant) sau:

```
|fact| + |quarantine| = |Bronze active|
```

Kết hợp với nguyên tắc một record — một `rule_id` (§7.1), bất biến này bảo đảm tập record đầu vào
được **phân hoạch** (partition theo nghĩa tập hợp) thành hai tập con rời nhau: mỗi record thuộc đúng
một trong hai tập, và không record nào bị mất trong quá trình xử lý. Trường hợp bất biến bị vi phạm,
job **dừng ngay (fail fast) và không ghi kết quả**, nhằm ngăn một trạng thái không nhất quán
(inconsistent state) bị ghi xuống tầng lưu trữ.

## 8. Hạn chế đã biết (known limitations)

| Hạn chế | Hệ quả |
|---|---|
| `ref_model_map` mới bao phủ ~52% số mã và ~90% số record | 9,76% record rơi vào `MODEL_CODE_UNKNOWN`. Có thể khôi phục bằng cách bổ sung reference data rồi re-run |
| Thiếu thông tin hộp số, kiểu dáng (coverage 7,79%) và số km đã đi | Giá nghiệp vụ là một giá trị duy nhất cho mỗi dòng xe/đời xe, không phân biệt phiên bản số sàn/tự động hay mức độ sử dụng của xe |
| Bên cung cấp đã loại bỏ 189 record có mã xe không tồn tại trong bảng giá tham khảo | Pipeline không quan sát được các record này |
