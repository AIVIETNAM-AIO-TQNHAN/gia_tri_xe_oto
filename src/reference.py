"""Reference data có PHIÊN BẢN — chọn phiên bản và kiểm tra trước khi Silver dùng.

Reference data (ref_*) là dữ liệu bàn giao giống batch tháng, nên theo cùng quy tắc của landing zone:
mỗi phiên bản là một thư mục BẤT BIẾN, có manifest; bản sửa = phiên bản mới, không ghi đè bản cũ.

    data/landing/reference/ref_model_map/version=2026-06/ref_model_map.csv + _manifest.json
                                        /version=2026-07/ref_model_map.csv + _manifest.json

Chọn phiên bản:
  - Mặc định: phiên bản MỚI NHẤT — mọi tháng được dựng lại theo cách giải mã mới nhất (restate).
  - ``as_of="2026-06"``: phiên bản mới nhất có version <= 2026-06 — để tái tạo đúng kết quả cũ.

Silver không lưu lại các phiên bản dim_model: dim_model dựng lại được từ ref bất kỳ lúc nào, chỉ cần
biết đã dùng phiên bản nào — thông tin đó nằm trong _run_info.json và data/control/run_log.csv.
"""
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

from src.config import REF_DIR

VERSION_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
MANIFEST = "_manifest.json"


@dataclass(frozen=True)
class RefVersion:
    name: str
    version: str          # "2026-07"; "unversioned" nếu file không nằm trong thư mục version=
    path: Path
    sha256: str
    row_count: int

    def as_dict(self) -> dict:
        d = asdict(self)
        d["path"] = self.path.as_posix()
        return d


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _count_rows(path: Path) -> int:
    """Số record dữ liệu (không tính header). Ref nhỏ, không có xuống dòng trong ô."""
    with open(path, encoding="utf-8-sig") as f:
        return sum(1 for line in f if line.strip()) - 1


def list_versions(name: str, ref_dir: Path = REF_DIR) -> list[str]:
    """Các phiên bản có manifest, tăng dần. Thư mục thiếu manifest bị bỏ qua (bàn giao dở dang)."""
    base = ref_dir / name
    out = [p.name.split("=", 1)[1] for p in base.glob("version=*") if (p / MANIFEST).exists()]
    return sorted(v for v in out if VERSION_RE.match(v))


def resolve(name: str, as_of: str | None = None, ref_dir: Path = REF_DIR) -> Path:
    """Đường dẫn CSV của phiên bản được chọn: mới nhất, hoặc mới nhất có version <= as_of."""
    if as_of is not None and not VERSION_RE.match(as_of):
        raise ValueError(f"ref version phải có dạng YYYY-MM, nhận được {as_of!r}")
    versions = list_versions(name, ref_dir)
    usable = [v for v in versions if as_of is None or v <= as_of]
    if not usable:
        raise FileNotFoundError(f"{name}: không có phiên bản nào"
                                f"{f' <= {as_of}' if as_of else ''} trong {ref_dir / name} "
                                f"(hiện có: {versions or 'không có'})")
    return ref_dir / name / f"version={usable[-1]}" / f"{name}.csv"


def inspect(path: Path) -> RefVersion:
    """Đọc thông tin phiên bản của một file ref, và kiểm tra nó với manifest (nếu có).

    File lệch manifest (sha256 hoặc số record) -> DỪNG: ref đã bị sửa tay sau khi bàn giao, mọi kết quả
    dựng từ nó sẽ không truy vết được. File không có manifest (ví dụ bản sao trong sandbox notebook) vẫn
    dùng được, version ghi là "unversioned".
    """
    sha, rows = _sha256(path), _count_rows(path)
    manifest_path = path.parent / MANIFEST
    if not manifest_path.exists():
        return RefVersion(path.stem, "unversioned", path, sha, rows)

    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    if m.get("file_name") != path.name:
        raise ValueError(f"{manifest_path}: file_name={m.get('file_name')!r} ≠ {path.name}")
    if m["file_sha256"] != sha:
        raise ValueError(f"{path}: sha256 {sha[:12]}… ≠ manifest {m['file_sha256'][:12]}… "
                         "— ref bị sửa sau khi bàn giao, hãy bàn giao thành phiên bản mới")
    if int(m["row_count"]) != rows:
        raise ValueError(f"{path}: {rows} record ≠ manifest {m['row_count']}")
    return RefVersion(m.get("ref_name", path.stem), m["version"], path, sha, rows)
