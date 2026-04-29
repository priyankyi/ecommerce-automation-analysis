from __future__ import annotations

import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BACKUP_DIR = PROJECT_ROOT / "data" / "output" / "code_backups"
BACKUP_NAME_PREFIX = "ecommerce_automation_code_backup_"

INCLUDED_PATHS = [
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "config",
    PROJECT_ROOT / "templates",
    PROJECT_ROOT / "apps_script",
    PROJECT_ROOT / "README.md",
    PROJECT_ROOT / "requirements.txt",
    PROJECT_ROOT / "run_flipkart_pipeline.ps1",
    PROJECT_ROOT / "notes" / "PROJECT_CONTEXT_GRAPH.md",
]

EXCLUDED_PARTS = {".venv", "credentials", "data", "__pycache__"}
EXCLUDED_FILENAMES = {"token.json", "credentials.json"}
EXCLUDED_SUFFIXES = {".pyc", ".xlsx", ".xls", ".xlsm", ".xlsb", ".csv"}


def _should_exclude(path: Path) -> bool:
    if path.name in EXCLUDED_FILENAMES:
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES and "data" in path.parts:
        return True
    if any(part in EXCLUDED_PARTS for part in path.parts):
        return True
    return False


def _iter_files_to_backup() -> List[Path]:
    files: List[Path] = []
    for root in INCLUDED_PATHS:
        if not root.exists():
            continue
        if root.is_file():
            if not _should_exclude(root):
                files.append(root)
            continue
        for path in root.rglob("*"):
            if path.is_file() and not _should_exclude(path):
                files.append(path)
    unique_files = {path.resolve(): path for path in files}
    return [unique_files[key] for key in sorted(unique_files)]


def _zip_arcname(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")


def backup_project_code() -> Dict[str, object]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_zip_path = BACKUP_DIR / f"{BACKUP_NAME_PREFIX}{timestamp}.zip"
    files = _iter_files_to_backup()

    with zipfile.ZipFile(backup_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in files:
            archive.write(file_path, arcname=_zip_arcname(file_path))

    result = {
        "status": "SUCCESS",
        "backup_zip_path": str(backup_zip_path),
        "files_included": len(files),
        "excluded_rules": [
            ".venv/",
            "credentials/",
            "data/input/",
            "data/output/",
            "__pycache__/",
            "*.pyc",
            "token.json",
            "credentials.json",
            "raw marketplace reports",
            "large Excel/CSV files",
        ],
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    backup_project_code()


if __name__ == "__main__":
    main()
