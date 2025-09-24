# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
# ==========================================
# ========== FILE: scripts/generate_filelist.py
# ==========================================
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

ROOTS = [Path(".")]  # puedes restringir a ["src", "data", "reports"] si prefieres
OUT = Path("reports/filelist.csv")


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for root in ROOTS:
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            # ignora virtualenvs y binarios grandes
            if any(
                seg in {".venv", ".git", ".mypy_cache", ".pytest_cache", "__pycache__"}
                for seg in p.parts
            ):
                continue
            try:
                size = p.stat().st_size
                digest = (
                    sha256(p) if size < 50_000_000 else ""
                )  # evita hashear archivos muy grandes
                rows.append({"path": str(p), "size_bytes": size, "sha256": digest})
            except Exception:
                # tolerante a race conditions / permisos
                continue

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["path", "size_bytes", "sha256"])
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: r["path"]))

    print(f"[OK] Inventario escrito en {OUT} ({len(rows)} archivos)")


if __name__ == "__main__":
    main()
