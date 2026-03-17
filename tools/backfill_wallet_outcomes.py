from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from paper.paper_trader import _record_closed_trade_outcome


def main() -> None:
    closed_path = Path("data/paper/closed_trades.json")
    if not closed_path.exists():
        print("no closed_trades.json found")
        return

    try:
        rows = json.loads(closed_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"failed_to_read_closed_trades: {exc}")
        return

    if not isinstance(rows, list):
        print("closed_trades.json is not a list")
        return

    total = 0
    recorded = 0
    skipped = 0

    for trade in rows:
        if not isinstance(trade, dict):
            continue
        total += 1
        try:
            changed = _record_closed_trade_outcome(trade)
            if changed:
                recorded += 1
            else:
                skipped += 1
        except Exception as exc:
            skipped += 1
            mint = str(trade.get("mint") or "")
            print(f"error mint={mint}: {exc}")

    print(f"closed_trades_total={total}")
    print(f"wallet_outcomes_recorded={recorded}")
    print(f"wallet_outcomes_skipped={skipped}")


if __name__ == "__main__":
    main()
