from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.wallet_intelligence import WalletIntelligenceEngine
import scoring.feature_engine as feature_engine


def main() -> None:
    engine = WalletIntelligenceEngine()

    print(f"wallet_engine_ok={engine is not None}")
    print(
        "feature_engine_wallet_memory_ok="
        f"{feature_engine._get_wallet_intelligence_engine() is not None}"
    )

    top = engine.top_wallets(10)
    print(f"top_wallet_count={len(top)}")

    for row in top[:5]:
        print(
            {
                "wallet": row.wallet,
                "quality_score": row.quality_score,
                "confidence_score": row.confidence_score,
                "paper_trade_count": row.paper_trade_count,
                "paper_trade_wins": row.paper_trade_wins,
                "paper_trade_losses": row.paper_trade_losses,
                "tokens_seen": row.tokens_seen,
            }
        )


if __name__ == "__main__":
    main()
