import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_FILES = [
    REPO_ROOT / "src" / "01_ingestion.py",
    REPO_ROOT / "src" / "02_cleaning.py",
    REPO_ROOT / "src" / "03_chunking.py",
]


@pytest.mark.parametrize("path", PIPELINE_FILES, ids=lambda p: p.name)
def test_pipeline_file_parses(path: Path):
    src = path.read_text(encoding="utf-8")
    ast.parse(src)
    compile(src, str(path), "exec", dont_inherit=True)


def test_pipeline_files_have_dlt_table_decorator():
    for path in PIPELINE_FILES:
        src = path.read_text(encoding="utf-8")
        assert "@dlt.table" in src
        assert "def " in src
