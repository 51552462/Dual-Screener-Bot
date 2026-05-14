#!/usr/bin/env python3
"""
systemd `dante-factory` 엔트리: 작업 디렉터리 정리 후 `main.py`를 __main__ 으로 기동한다.
`main.py` 매매·스캐너 로직은 수정하지 않는다.
"""
from __future__ import annotations

import os
import runpy
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    os.chdir(_ROOT)
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    runpy.run_path(os.path.join(_ROOT, "main.py"), run_name="__main__")


if __name__ == "__main__":
    main()
