from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_ago(seconds: float) -> str:
    return (_utcnow() - timedelta(seconds=float(seconds))).isoformat()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    details: dict[str, Any]
    error: str | None = None


class PaperTraderHarness:
    def __init__(self, module_path: Path):
        self.module_path = Path(module_path).resolve()
        self.repo_root = self.module_path.parent.parent if self.module_path.parent.name == "paper" else self.module_path.parent
        if str(self.repo_root) not in sys.path:
            sys.path.insert(0, str(self.repo_root))
        self._tmp = tempfile.TemporaryDirectory(prefix="paper_trader_regression_")
        self.temp_root = Path(self._tmp.name)
        self.paper_dir = self.temp_root / "data" / "paper"
        self.paper_dir.mkdir(parents=True, exist_ok=True)
        self.open_path = self.paper_dir / "open_trades.json"
        self.closed_path = self.paper_dir / "closed_trades.json"
        _save_json(self.open_path, {})
        _save_json(self.closed_path, [])
        token_cohort_dir = self.temp_root / "data" / "enrichment" / "token_cohorts"
        token_cohort_dir.mkdir(parents=True, exist_ok=True)
        self.module = self._load_module()
        self._configure_module(token_cohort_dir)

    def close(self) -> None:
        self._tmp.cleanup()

    def _load_module(self):
        name = f"paper_trader_under_test_{abs(hash(str(self.module_path)))}"
        spec = importlib.util.spec_from_file_location(name, self.module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load module from {self.module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _configure_module(self, token_cohort_dir: Path) -> None:
        self.module.PAPER_DIR = self.paper_dir
        self.module.OPEN_TRADES_PATH = self.open_path
        self.module.CLOSED_TRADES_PATH = self.closed_path
        self.module.TOKEN_COHORT_CACHE_DIR = token_cohort_dir
        self.module._record_closed_trade_outcome = lambda trade: False
        self.module._record_closed_trade_creator_entity_outcome = lambda trade: False
        self.module._WALLET_INTELLIGENCE_ENGINE = False
        self.module._CREATOR_ENTITY_INTELLIGENCE_ENGINE = False

    def reset_storage(self) -> None:
        _save_json(self.open_path, {})
        _save_json(self.closed_path, [])

    def read_open(self) -> dict[str, Any]:
        return _load_json(self.open_path, {})

    def read_closed(self) -> list[dict[str, Any]]:
        return _load_json(self.closed_path, [])

    def write_open(self, payload: dict[str, Any]) -> None:
        _save_json(self.open_path, payload)

    def write_closed(self, payload: list[dict[str, Any]]) -> None:
        _save_json(self.closed_path, payload)

    def set_trade_times(self, mint: str, *, opened_ago: float | None = None, updated_ago: float | None = None) -> dict[str, Any]:
        open_trades = self.read_open()
        trade = deepcopy(open_trades[mint])
        if opened_ago is not None:
            trade["opened_at_utc"] = _iso_ago(opened_ago)
        if updated_ago is not None:
            trade["updated_at_utc"] = _iso_ago(updated_ago)
        open_trades[mint] = trade
        self.write_open(open_trades)
        return trade

    def patch_open_trade(self, mint: str, patch: dict[str, Any]) -> dict[str, Any]:
        open_trades = self.read_open()
        trade = deepcopy(open_trades[mint])
        trade.update(patch)
        open_trades[mint] = trade
        self.write_open(open_trades)
        return trade

    def sync(self, snapshot: dict[str, Any], alert: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        return self.module.sync_trade(snapshot, alert)

    def sweep(self, preserve_mint: str | None = None) -> dict[str, Any]:
        return self.module.sweep_open_trades(preserve_mint=preserve_mint)


BASE_ALERT = {
    "action": "transition",
    "band": "paper_hold",
    "score": 0.0,
}


def make_alert(score: float) -> dict[str, Any]:
    alert = dict(BASE_ALERT)
    alert["score"] = float(score)
    return alert


BASE_SNAPSHOT = {
    "symbol": "TST",
    "name": "Test Token",
    "priority_tier": "alpha",
    "confidence": "medium",
    "strategy_name": "momentum_research",
    "primary_archetype": "MOMENTUM",
    "secondary_archetype": "EARLY",
    "setup_state": "paper_entry",
    "entry_bias": "long",
    "analysis_mode": "full",
    "composite_score": 95.0,
    "local_composite_score": 95.0,
    "entry_confirmation_score": 0.72,
    "current_market_cap_sol": 100.0,
    "peak_market_cap_sol": 100.0,
    "helius_enrichment_tier": "deep",
    "helius_enrichment_status": "deep_enriched",
    "helius_requested_wallet_count": 6,
    "helius_completed_wallet_count": 6,
    "helius_profile_depth_bucket": "deep_high_quality",
    "helius_profile_completion_ratio": 1.0,
    "helius_selected_wallet_count": 6,
    "helius_profiled_wallet_count": 6,
    "helius_selected_wallets": ["wallet_a", "wallet_b"],
    "creator_wallet": "creator_wallet_1",
    "creator_entity_key": "funder::example",
    "risk_flags": [],
    "invalidation_reasons": [],
    "trigger_reasons": ["paper_entry_ready"],
    "why_now": ["test_fixture"],
    "regime_tags": [],
    "quant_features": {
        "participant_quality_score_v2": 0.72,
        "dust_trade_share_1m": 0.02,
        "dust_trade_share_5m": 0.01,
        "mcap_to_peak_ratio": 0.50,
        "creator_entity_quality_score": 0.65,
        "creator_entity_confidence_score": 0.25,
        "creator_entity_launch_count": 3.0,
        "creator_entity_paper_trade_count": 4.0,
        "creator_entity_creator_wallet_count": 1.0,
        "creator_entity_funder_wallet_count": 1.0,
    },
    "helius_wallet_cohort_summary": {
        "cohort_quality_score": 0.7,
        "fresh_wallet_share": 0.4,
        "sniper_wallet_share": 0.08,
        "recycled_wallet_share": 0.02,
        "funding_diversity_score": 0.55,
        "creator_shared_funder_score": 0.03,
        "profile_completion_confidence": 0.9,
    },
}


def make_snapshot(mint: str, **overrides: Any) -> dict[str, Any]:
    snapshot = deepcopy(BASE_SNAPSHOT)
    snapshot["mint"] = mint
    if "symbol" not in overrides:
        snapshot["symbol"] = mint[:6].upper()
    for key, value in overrides.items():
        if key == "quant_features":
            merged = deepcopy(snapshot["quant_features"])
            merged.update(value)
            snapshot["quant_features"] = merged
        elif key == "helius_wallet_cohort_summary":
            merged = deepcopy(snapshot["helius_wallet_cohort_summary"])
            merged.update(value)
            snapshot["helius_wallet_cohort_summary"] = merged
        else:
            snapshot[key] = value
    return snapshot


def scenario_invalidated_loser_closes(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_invalidated_loser"
    h.reset_storage()
    status, opened = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.set_trade_times(mint, opened_ago=20.0, updated_ago=20.0)
    status, trade = h.sync(
        make_snapshot(
            mint,
            current_market_cap_sol=90.0,
            peak_market_cap_sol=100.0,
            composite_score=58.0,
            local_composite_score=58.0,
            quant_features={"mcap_to_peak_ratio": 0.50},
            invalidation_reasons=["invalidated_fixture"],
        ),
        make_alert(58.0),
    )
    closed = h.read_closed()
    last = closed[-1]
    passed = status == "closed" and last["exit_reason"] == "invalidated" and last["pnl_pct_proxy"] <= -9.9
    return ScenarioResult(
        name="invalidated_loser_closes",
        passed=passed,
        details={"sync_status": status, "exit_reason": last.get("exit_reason"), "pnl_pct_proxy": last.get("pnl_pct_proxy")},
    )


def scenario_invalidated_green_hold(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_invalidated_green_hold"
    h.reset_storage()
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.set_trade_times(mint, opened_ago=20.0, updated_ago=20.0)
    status, trade = h.sync(
        make_snapshot(
            mint,
            current_market_cap_sol=106.0,
            peak_market_cap_sol=106.0,
            composite_score=62.0,
            local_composite_score=62.0,
            quant_features={"mcap_to_peak_ratio": 0.50},
            invalidation_reasons=["invalidated_fixture"],
        ),
        make_alert(62.0),
    )
    open_trades = h.read_open()
    active = open_trades.get(mint, {})
    passed = status == "updated" and mint in open_trades and active.get("reason") == "hold_below_entry_threshold"
    return ScenarioResult(
        name="invalidated_green_hold",
        passed=passed,
        details={"sync_status": status, "reason": active.get("reason"), "qualify_reason": active.get("qualify_reason"), "pnl_pct_proxy": active.get("pnl_pct_proxy")},
    )


def scenario_winner_floor_stop_protected(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_winner_floor_stop"
    h.reset_storage()
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.set_trade_times(mint, opened_ago=20.0, updated_ago=20.0)
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=160.0, peak_market_cap_sol=160.0, composite_score=110.0, local_composite_score=110.0, quant_features={"mcap_to_peak_ratio": 0.50}), make_alert(110.0))
    assert status == "updated", status
    h.set_trade_times(mint, opened_ago=40.0, updated_ago=5.0)
    status, trade = h.sync(
        make_snapshot(
            mint,
            current_market_cap_sol=103.0,
            peak_market_cap_sol=160.0,
            composite_score=70.0,
            local_composite_score=70.0,
            quant_features={"mcap_to_peak_ratio": 0.50},
            invalidation_reasons=["invalidated_fixture"],
        ),
        make_alert(70.0),
    )
    closed = h.read_closed()
    last = closed[-1]
    passed = (
        status == "closed"
        and last.get("exit_reason") == "winner_floor_stop"
        and bool(last.get("winner_lock_floor_applied"))
        and float(last.get("pnl_pct_proxy", 0.0)) >= 8.0
        and float(last.get("winner_lock_raw_pnl_pct_proxy", 0.0)) <= 3.1
    )
    return ScenarioResult(
        name="winner_floor_stop_protected",
        passed=passed,
        details={
            "sync_status": status,
            "exit_reason": last.get("exit_reason"),
            "raw_pnl_pct_proxy": last.get("winner_lock_raw_pnl_pct_proxy"),
            "protected_pnl_pct_proxy": last.get("pnl_pct_proxy"),
            "floor": last.get("winner_lock_floor_pnl_pct"),
        },
    )


def scenario_trail_stop_winner(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_trail_stop"
    h.reset_storage()
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.set_trade_times(mint, opened_ago=20.0, updated_ago=20.0)
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=170.0, peak_market_cap_sol=170.0, composite_score=115.0, local_composite_score=115.0, quant_features={"mcap_to_peak_ratio": 0.50}), make_alert(115.0))
    assert status == "updated", status
    h.set_trade_times(mint, opened_ago=40.0, updated_ago=5.0)
    status, trade = h.sync(
        make_snapshot(
            mint,
            current_market_cap_sol=135.0,
            peak_market_cap_sol=170.0,
            composite_score=96.0,
            local_composite_score=96.0,
            quant_features={"mcap_to_peak_ratio": 0.50},
            invalidation_reasons=[],
        ),
        make_alert(96.0),
    )
    last = h.read_closed()[-1]
    passed = status == "closed" and last.get("exit_reason") == "trail_stop" and float(last.get("pnl_pct_proxy", 0.0)) >= 35.0
    return ScenarioResult(
        name="trail_stop_winner",
        passed=passed,
        details={"sync_status": status, "exit_reason": last.get("exit_reason"), "pnl_pct_proxy": last.get("pnl_pct_proxy")},
    )


def scenario_stale_invalidated_timeout(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_stale_invalidated"
    h.reset_storage()
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.patch_open_trade(
        mint,
        {
            "opened_at_utc": _iso_ago(240.0),
            "updated_at_utc": _iso_ago(240.0),
            "reason": "hold_below_entry_threshold",
            "qualify_reason": "invalidated",
            "current_market_cap_sol": 99.0,
            "current_score": 41.0,
            "pnl_pct_proxy": -1.0,
            "max_pnl_pct_proxy": 4.0,
            "min_pnl_pct_proxy": -1.0,
            "peak_market_cap_sol": 104.0,
        },
    )
    result = h.sweep()
    last = h.read_closed()[-1]
    passed = result.get("closed_count") == 1 and last.get("exit_reason") == "stale_invalidated_timeout"
    return ScenarioResult(
        name="stale_invalidated_timeout",
        passed=passed,
        details={"sweep_result": result, "exit_reason": last.get("exit_reason"), "pnl_pct_proxy": last.get("pnl_pct_proxy")},
    )


def scenario_winner_floor_timeout_protected(h: PaperTraderHarness) -> ScenarioResult:
    mint = "mint_winner_floor_timeout"
    h.reset_storage()
    status, _ = h.sync(make_snapshot(mint, current_market_cap_sol=100.0), make_alert(95.0))
    assert status == "opened", status
    h.patch_open_trade(
        mint,
        {
            "opened_at_utc": _iso_ago(300.0),
            "updated_at_utc": _iso_ago(90.0),
            "reason": "hold_below_entry_threshold",
            "qualify_reason": "invalidated",
            "current_market_cap_sol": 89.0,
            "peak_market_cap_sol": 160.0,
            "current_score": 40.0,
            "pnl_pct_proxy": -11.0,
            "max_pnl_pct_proxy": 60.0,
            "min_pnl_pct_proxy": -11.0,
            "invalidation_reasons": ["invalidated_fixture"],
        },
    )
    result = h.sweep()
    last = h.read_closed()[-1]
    passed = (
        result.get("closed_count") == 1
        and last.get("exit_reason") == "winner_floor_timeout"
        and bool(last.get("winner_lock_floor_applied"))
        and float(last.get("pnl_pct_proxy", 0.0)) >= 8.0
    )
    return ScenarioResult(
        name="winner_floor_timeout_protected",
        passed=passed,
        details={
            "sweep_result": result,
            "exit_reason": last.get("exit_reason"),
            "raw_pnl_pct_proxy": last.get("winner_lock_raw_pnl_pct_proxy"),
            "protected_pnl_pct_proxy": last.get("pnl_pct_proxy"),
            "floor": last.get("winner_lock_floor_pnl_pct"),
        },
    )


SCENARIOS: list[Callable[[PaperTraderHarness], ScenarioResult]] = [
    scenario_invalidated_loser_closes,
    scenario_invalidated_green_hold,
    scenario_winner_floor_stop_protected,
    scenario_trail_stop_winner,
    scenario_stale_invalidated_timeout,
    scenario_winner_floor_timeout_protected,
]


def run_suite(module_path: Path) -> dict[str, Any]:
    harness = PaperTraderHarness(module_path)
    try:
        results: list[ScenarioResult] = []
        for scenario in SCENARIOS:
            try:
                results.append(scenario(harness))
            except Exception as exc:
                results.append(
                    ScenarioResult(
                        name=scenario.__name__,
                        passed=False,
                        details={},
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        return {
            "module_path": str(module_path),
            "passed": passed,
            "failed": failed,
            "success": failed == 0,
            "scenarios": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "details": r.details,
                    **({"error": r.error} if r.error else {}),
                }
                for r in results
            ],
        }
    finally:
        harness.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Regression harness for paper/paper_trader.py close logic.")
    parser.add_argument(
        "--module-path",
        default="paper/paper_trader.py",
        help="Path to the paper_trader module to test. Defaults to paper/paper_trader.py",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to save the JSON report.",
    )
    args = parser.parse_args()

    module_path = Path(args.module_path)
    report = run_suite(module_path)

    print("=== paper_trader_regression ===")
    print(json.dumps(report, indent=2, ensure_ascii=False))

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"json_report_saved={out_path}")

    return 0 if report.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
