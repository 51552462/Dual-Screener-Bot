"""One-shot: move bitget_*.py into bitget/ package + root shims."""
from __future__ import annotations

import os
import re
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BITGET_DIR = os.path.join(ROOT, "bitget")


def main() -> None:
    os.makedirs(BITGET_DIR, exist_ok=True)
    files = [f for f in os.listdir(ROOT) if f.startswith("bitget_") and f.endswith(".py")]
    renames = {f: f[len("bitget_") :] for f in files}

    init_path = os.path.join(BITGET_DIR, "__init__.py")
    if not os.path.isfile(init_path):
        with open(init_path, "w", encoding="utf-8") as fh:
            fh.write('"""Bitget (crypto) pipeline — isolated from equity KR/US factory."""\n')

    for old, new in renames.items():
        src = os.path.join(ROOT, old)
        dst = os.path.join(BITGET_DIR, new)
        if os.path.isfile(src):
            shutil.move(src, dst)
            print(f"moved {old} -> bitget/{new}")

    for fn in os.listdir(BITGET_DIR):
        if not fn.endswith(".py") or fn == "__init__.py":
            continue
        path = os.path.join(BITGET_DIR, fn)
        text = open(path, encoding="utf-8", errors="ignore").read()
        orig = text
        for old in sorted(renames.keys(), key=len, reverse=True):
            old_mod = old[:-3]
            new_mod = renames[old][:-3]
            text = re.sub(
                rf"\bimport {re.escape(old_mod)}\b",
                f"import bitget.{new_mod} as {old_mod}",
                text,
            )
            text = re.sub(
                rf"\bfrom {re.escape(old_mod)} import",
                f"from bitget.{new_mod} import",
                text,
            )
            text = text.replace(
                f'importlib.import_module("{old_mod}")',
                f'importlib.import_module("bitget.{new_mod}")',
            )
            text = text.replace(
                f"importlib.import_module('{old_mod}')",
                f"importlib.import_module('bitget.{new_mod}')",
            )
        if text != orig:
            open(path, "w", encoding="utf-8").write(text)
            print(f"patched {fn}")

    for old, new in renames.items():
        mod = new[:-3]
        shim = (
            f'"""Compatibility shim — implementation in bitget.{mod}."""\n'
            f"from bitget.{mod} import *  # noqa: F401,F403\n"
        )
        with open(os.path.join(ROOT, old), "w", encoding="utf-8") as fh:
            fh.write(shim)
        print(f"shim {old}")

    print(f"done {len(renames)} modules")


if __name__ == "__main__":
    main()
