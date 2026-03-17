#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from enrichment.helius_client import HeliusClient  # noqa: E402


def main() -> None:
    client = HeliusClient()
    print("[helius-smoke] client_initialized=true")
    print(f"[helius-smoke] enabled={getattr(client, 'enabled', False)}")

    if not getattr(client, "enabled", False):
        print("[helius-smoke] status=disabled_or_missing_key")
        return

    try:
        if hasattr(client, "smoke_test"):
            result = client.smoke_test()
        else:
            result = {
                "status": "no_builtin_smoke_test",
                "enabled": True,
            }
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as exc:
        print(f"[helius-smoke] status=failed")
        print(f"[helius-smoke] error={type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
