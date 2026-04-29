"""Repo safety check for private GitHub backups.

The checker only inspects Git commit candidates:
- tracked files, plus
- untracked files that are not ignored by .gitignore

If Git is not initialized, it falls back to a local .gitignore-aware scan.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
SENSITIVE_FILENAMES = {"credentials.json", "token.json", ".env"}
SENSITIVE_FOLDERS = {"credentials", "input", "output", "logs"}
SKIP_DIRECTORIES = {
    ".git",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "env",
    "node_modules",
    "venv",
}
SECRET_VALUE_LINE = re.compile(
    r'(?i)^\s*["\']?(client_secret|refresh_token|developer_token|api_secret|password|private_key)["\']?'
    r"\s*[:=]\s*['\"]?([^'\",\s#]+)"
)


def run_git(args: list[str]) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout


def git_available() -> bool:
    return shutil.which("git") is not None


def is_git_repo() -> bool:
    if not git_available():
        return False
    return run_git(["rev-parse", "--is-inside-work-tree"]).strip().lower() == "true"


def iter_local_files(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRECTORIES]
        current_dir = Path(dirpath)
        for filename in filenames:
            yield current_dir / filename


def parse_gitignore() -> list[tuple[bool, str, bool]]:
    gitignore_path = REPO_ROOT / ".gitignore"
    patterns: list[tuple[bool, str, bool]] = []
    if not gitignore_path.exists():
        return patterns

    for raw_line in gitignore_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        negate = line.startswith("!")
        if negate:
            line = line[1:]
        directory_only = line.endswith("/")
        if directory_only:
            line = line[:-1]
        patterns.append((negate, line, directory_only))
    return patterns


def candidate_targets(rel_path: Path) -> list[str]:
    parts = rel_path.as_posix().split("/")
    targets = ["/".join(parts[:index]) for index in range(1, len(parts) + 1)]
    return targets


def pattern_matches(pattern: str, target: str, directory_only: bool) -> bool:
    if directory_only:
        return target == pattern or target.startswith(f"{pattern}/")
    if "/" not in pattern:
        return fnmatch.fnmatchcase(Path(target).name, pattern)
    return fnmatch.fnmatchcase(target, pattern)


def is_ignored_by_gitignore(rel_path: Path, patterns: list[tuple[bool, str, bool]]) -> bool:
    ignored = False
    targets = candidate_targets(rel_path)
    for negate, pattern, directory_only in patterns:
        if any(pattern_matches(pattern, target, directory_only) for target in targets):
            ignored = not negate
    return ignored


def load_candidate_paths() -> tuple[list[Path], bool]:
    if is_git_repo():
        tracked = run_git(["ls-files", "--cached", "-z"])
        untracked = run_git(["ls-files", "--others", "--exclude-standard", "-z"])
        raw = [part for part in (tracked + untracked).split("\0") if part]
        paths = [REPO_ROOT / item for item in raw]
        return paths, True

    patterns = parse_gitignore()
    paths: list[Path] = []
    for path in iter_local_files(REPO_ROOT):
        rel_path = path.relative_to(REPO_ROOT)
        if not is_ignored_by_gitignore(rel_path, patterns):
            paths.append(path)
    return paths, False


def is_source_or_config_file(path: Path) -> bool:
    if path.name == ".env":
        return True
    if path.name.startswith("."):
        return path.suffix in {".json", ".yaml", ".yml"}
    if path.suffix.lower() in {
        ".env",
        ".ini",
        ".json",
        ".js",
        ".md",
        ".ps1",
        ".py",
        ".toml",
        ".txt",
        ".yaml",
        ".yml",
    }:
        return True
    return path.name in {"README.md", ".gitignore", "requirements.txt"}


def is_placeholder_template(path: Path) -> bool:
    return "_template" in path.name


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def add_issue(issues: list[dict], issue_type: str, path: Path, details: str) -> None:
    issues.append(
        {
            "type": issue_type,
            "path": str(path.relative_to(REPO_ROOT)),
            "details": details,
        }
    )


def is_sensitive_local_artifact(path: Path) -> bool:
    rel_path = path.relative_to(REPO_ROOT)
    parts = rel_path.parts

    if path.name in SENSITIVE_FILENAMES:
        return True

    if len(parts) >= 2 and parts[0] == "credentials" and path.name != ".gitkeep":
        return True

    if len(parts) >= 2 and parts[0] == "data" and parts[1] in SENSITIVE_FOLDERS and path.name != ".gitkeep":
        return True

    return False


def scan_candidate_file(path: Path) -> list[dict]:
    issues: list[dict] = []
    rel_path = path.relative_to(REPO_ROOT)
    rel_str = rel_path.as_posix()

    if path.name in SENSITIVE_FILENAMES:
        add_issue(issues, "sensitive_file", path, f"Sensitive file present at {rel_str}")

    if len(rel_path.parts) >= 2 and rel_path.parts[0] == "credentials" and path.name != ".gitkeep":
        add_issue(
            issues,
            "credentials_folder_file",
            path,
            f"File under credentials/ should not be committed: {rel_str}",
        )

    if len(rel_path.parts) >= 3 and rel_path.parts[0] == "data" and rel_path.parts[1] in {"input", "output", "logs"} and path.name != ".gitkeep":
        add_issue(
            issues,
            "data_artifact",
            path,
            f"File under data/input, data/output, or data/logs should stay out of Git: {rel_str}",
        )

    try:
        if path.stat().st_size > MAX_FILE_SIZE_BYTES:
            add_issue(issues, "large_file", path, f"File is larger than 10 MB: {rel_str}")
    except OSError:
        add_issue(issues, "unreadable_file", path, f"Could not inspect file size: {rel_str}")

    if is_source_or_config_file(path) and not is_placeholder_template(path):
        content = read_text(path)
        matches: set[str] = set()
        for line in content.splitlines():
            match = SECRET_VALUE_LINE.search(line)
            if match:
                value = match.group(2).strip().strip("'\"")
                if not value.upper().startswith(("YOUR_", "OPTIONAL_", "PLACEHOLDER")):
                    matches.add(match.group(1).lower())
        if matches:
            add_issue(
                issues,
                "possible_secret",
                path,
                f"Possible secret values found for: {', '.join(sorted(matches))}",
            )

    return issues


def main() -> int:
    candidate_paths, git_repo = load_candidate_paths()
    candidate_files = [path for path in candidate_paths if path.is_file()]
    issues: list[dict] = []
    ignored_sensitive_count = 0

    patterns = parse_gitignore() if not git_repo else []
    for path in iter_local_files(REPO_ROOT):
        if not path.is_file():
            continue
        rel_path = path.relative_to(REPO_ROOT)
        if git_repo:
            ignored = False
        else:
            ignored = is_ignored_by_gitignore(rel_path, patterns)
        if ignored and is_sensitive_local_artifact(path):
            ignored_sensitive_count += 1

    for path in candidate_files:
        issues.extend(scan_candidate_file(path))

    recommendations: list[str] = []
    if issues:
        recommendations.extend(
            [
                "Remove or relocate commit-candidate secrets and data artifacts before committing.",
                "Keep reports, exports, logs, and credential files out of Git.",
                "Store real secrets outside the repo and use the template files as placeholders.",
            ]
        )
    else:
        recommendations.append("Repo is clean for backup.")
        if ignored_sensitive_count:
            recommendations.append("Sensitive local files exist but are ignored by Git.")
        if not git_repo:
            recommendations.append("Run `git init` before the first commit so tracked-file checks can be exact.")
        else:
            recommendations.append("Proceed with the commit workflow for the private GitHub repo.")

    report = {
        "status": "FAIL" if issues else "PASS",
        "safe_to_commit": not issues,
        "commit_candidate_count": len(candidate_files),
        "issues_found": issues,
        "issue_count": len(issues),
        "ignored_sensitive_files_detected_count": ignored_sensitive_count,
        "recommendations": recommendations,
    }

    print(json.dumps(report, indent=2))
    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
