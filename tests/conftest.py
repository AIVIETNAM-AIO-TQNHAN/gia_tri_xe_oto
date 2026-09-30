import shutil
from pathlib import Path

import pytest

from src.config import LANDING_DIR


@pytest.fixture(scope="session")
def spark():
    from src.spark import get_spark
    s = get_spark("tests")
    yield s
    s.stop()


@pytest.fixture
def one_batch(tmp_path: Path) -> Path:
    """Bản sao batch 2025-01 trong thư mục tạm — test được phép làm hỏng nó, landing zone thật không
    bị tác động."""
    src = LANDING_DIR / "as_of_month=2025-01"
    dst = tmp_path / "landing" / src.name
    shutil.copytree(src, dst)
    return dst
