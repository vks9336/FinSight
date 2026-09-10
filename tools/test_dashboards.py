#!/usr/bin/env python3
"""Renders every dashboard page headlessly and fails on any exception.

Streamlit serving HTTP 200 only proves the shell loaded; a page can still raise
during its script run. AppTest executes the page for real, so this catches the
errors a curl check would miss.

    python tools/test_dashboards.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
PAGES = [
    ROOT / "dashboards" / "app.py",
    ROOT / "dashboards" / "pages" / "1_Fraud_Alert_Board.py",
    ROOT / "dashboards" / "pages" / "2_Customer_360.py",
    ROOT / "dashboards" / "pages" / "3_Risk_and_Compliance.py",
]


def main() -> int:
    failures = 0
    for page in PAGES:
        name = str(page.relative_to(ROOT))
        try:
            at = AppTest.from_file(str(page), default_timeout=120).run()
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL] {name}: {type(exc).__name__}: {exc}")
            failures += 1
            continue

        if at.exception:
            print(f"[FAIL] {name}")
            for e in at.exception:
                print(f"       {e.value}")
            failures += 1
            continue

        print(f"[ OK ] {name:<48} "
              f"metrics={len(at.metric):<3} charts={len(at.get('plotly_chart')):<3} "
              f"tables={len(at.dataframe)}")

        for m in at.metric:
            print(f"         {m.label:<22} {m.value:<12} {m.delta or ''}")

    print()
    if failures:
        print(f"{failures} page(s) failed")
        return 1
    print(f"All {len(PAGES)} pages rendered cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
