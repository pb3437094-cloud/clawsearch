import json
from pathlib import Path
from datetime import datetime


RAW_DIR = Path("data/raw_launch_snapshots")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def save_snapshot(source: str, items: list[dict]) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = RAW_DIR / f"{source}_{timestamp}.json"

    payload = {
        "source": source,
        "captured_at_utc": datetime.utcnow().isoformat(),
        "count": len(items),
        "items": items,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return str(filename)
