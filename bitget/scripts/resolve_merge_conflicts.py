"""One-shot: keep HEAD side of git conflict markers in bitget/."""
from __future__ import annotations

from pathlib import Path


def resolve_head(text: str) -> str:
    out: list[str] = []
    lines = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("<<<<<<< HEAD"):
            i += 1
            while i < len(lines) and not lines[i].startswith("======="):
                out.append(lines[i])
                i += 1
            while i < len(lines) and not lines[i].startswith(">>>>>>>"):
                i += 1
            if i < len(lines):
                i += 1
            continue
        out.append(line)
        i += 1
    return "".join(out)


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    patterns = [
        "bitget/governance/*.py",
        "bitget/validation/*.py",
        "bitget/tests/test_phase*.py",
        "bitget/docs/*.md",
        "bitget/*.md",
    ]
    for pat in patterns:
        for path in sorted(root.glob(pat)):
            if path.is_dir():
                continue
            text = path.read_text(encoding="utf-8")
            if "<<<<<<< HEAD" not in text:
                continue
            path.write_text(resolve_head(text), encoding="utf-8")
            print(f"resolved {path.relative_to(root)}")


if __name__ == "__main__":
    main()
