from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from time import monotonic

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from alerts.alert_engine import write_alert
from collector.storage import save_snapshot
from enrichment.enrichment_policy import build_enrichment_hint
from enrichment.wallet_enrichment import WalletEnrichmentManager
from filters.escalation_persistence import write_escalation_regime_if_changed
from filters.pump_filters import should_escalate
from paper.paper_trader import initialize_paper_book, sweep_open_trades, sync_trade
from scoring.score_engine import StrategyScoreEngine
from signals.event_manager import SignalEventManager
from signals.leaderboard import StrategyLeaderboard
from state.token_state_manager import TokenStateManager

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
WS_PING_INTERVAL_SECONDS = 20
WS_PING_TIMEOUT_SECONDS = 20
WS_OPEN_TIMEOUT_SECONDS = 20
WS_CLOSE_TIMEOUT_SECONDS = 10
WS_MAX_QUEUE = 2048
RECONNECT_BASE_DELAY_SECONDS = 2
RECONNECT_MAX_DELAY_SECONDS = 30

# Round 2.1 collector tuning:
# Give staged enrichment a slightly more generous outer timeout so deep enrichment
# has a realistic chance to profile 2-4 wallets without blocking discovery.
ENRICHMENT_TIMEOUT_SECONDS = 4.8
ENRICHMENT_TIME_BUDGET_SECONDS = 4.0

# Revisit-cooldown tuning:
# When a mint is repeatedly invalidated / backgrounded without materially improving,
# suppress full reevaluation for a short period so the collector preserves recall
# without spending cycles reprocessing the same "no" candidate every trade tick.
REVISIT_BACKOFF_SECONDS = 45.0
REVISIT_SCORE_IMPROVEMENT_THRESHOLD = 8.0
REVISIT_NET_FLOW_IMPROVEMENT_THRESHOLD = 4.0
REVISIT_BUY_COUNT_IMPROVEMENT_THRESHOLD = 2
REVISIT_MCAP_IMPROVEMENT_RATIO = 1.25
REVISIT_MCAP_IMPROVEMENT_ABS_SOL = 10.0
REVISIT_PARTICIPANT_QUALITY_IMPROVEMENT = 0.18
PAPER_SWEEP_INTERVAL_SECONDS = 30.0
OPEN_TRADES_PATH = Path("data/paper/open_trades.json")


def normalize_token(raw: dict) -> dict:
    return {
        "source": "pumpportal_ws",
        "captured_at_utc": datetime.utcnow().isoformat(),
        "event_type": "new_token",
        "mint": raw.get("mint"),
        "name": raw.get("name"),
        "symbol": raw.get("symbol"),
        "creator_wallet": raw.get("traderPublicKey"),
        "market_cap_sol": raw.get("marketCapSol") or raw.get("marketCap"),
        "initial_buy": raw.get("initialBuy"),
        "sol_amount": raw.get("solAmount"),
        "bonding_curve_key": raw.get("bondingCurveKey"),
        "tx_type": raw.get("txType"),
        "uri": raw.get("uri"),
        "pool": raw.get("pool"),
        "raw": raw,
    }


def normalize_trade(raw: dict) -> dict:
    return {
        "source": "pumpportal_ws",
        "captured_at_utc": datetime.utcnow().isoformat(),
        "event_type": "token_trade",
        "mint": raw.get("mint"),
        "signature": raw.get("signature"),
        "trader_wallet": raw.get("traderPublicKey"),
        "tx_type": raw.get("txType"),
        "token_amount": raw.get("tokenAmount"),
        "sol_amount": raw.get("solAmount"),
        "market_cap_sol": raw.get("marketCapSol") or raw.get("marketCap"),
        "pool": raw.get("pool"),
        "raw": raw,
    }


def should_watch_token(token: dict) -> bool:
    initial_buy = token.get("initial_buy") or 0
    market_cap_sol = token.get("market_cap_sol") or 0
    return initial_buy > 0 or market_cap_sol >= 30


async def maybe_run_paper_sweep(reason: str) -> None:
    try:
        sweep_result = await asyncio.to_thread(sweep_open_trades)
    except Exception as exc:
        print(f"[paper] sweep-error | reason={reason} | {exc}")
        return

    closed_count = int(sweep_result.get("closed_count", 0) or 0)
    if not closed_count:
        return

    reasons = sweep_result.get("closed_by_reason", {}) or {}
    reasons_text = ",".join(
        f"{key}:{value}" for key, value in sorted(reasons.items())
    ) or "none"
    print(
        f"[paper] sweep | reason={reason} | closed={closed_count} | "
        f"details={reasons_text} | open={sweep_result.get('open_trade_count')} | "
        f"closed_total={sweep_result.get('closed_trade_count')}"
    )


async def subscribe_token_trades(websocket, mint: str):
    payload = {"method": "subscribeTokenTrade", "keys": [mint]}
    await websocket.send(json.dumps(payload))
    print(f"[pumpportal] subscribed to trades for {mint}")


async def enrich_state_with_timeout(
    enrichment_manager: WalletEnrichmentManager,
    state_dict: dict,
    hint: dict | None = None,
) -> dict:
    if not enrichment_manager.enabled:
        return state_dict

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                enrichment_manager.maybe_attach_enrichment,
                state_dict,
                hint,
                time_budget_seconds=ENRICHMENT_TIME_BUDGET_SECONDS,
            ),
            timeout=ENRICHMENT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        mint = state_dict.get("mint") or "unknown"
        print(f"[helius] enrichment-timeout | {mint} | skipped")
        merged = dict(state_dict)
        merged["helius_enrichment_status"] = "collector_timeout"
        merged["helius_last_error"] = "collector_timeout"
        merged["helius_cached_error"] = None
        merged["helius_requested_wallet_count"] = 0
        merged["helius_completed_wallet_count"] = 0
        merged["helius_selected_wallet_count"] = 0
        merged["helius_profiled_wallet_count"] = 0
        merged["helius_profile_depth_bucket"] = "none"
        merged["helius_profile_completion_ratio"] = 0.0
        merged["helius_partial_enrichment"] = False
        merged["helius_wallet_attempt_details"] = []
        merged["helius_time_budget_seconds"] = ENRICHMENT_TIME_BUDGET_SECONDS
        merged["helius_time_budget_exhausted"] = False
        if hint:
            merged["helius_enrichment_tier"] = hint.get("tier", "none")
            merged["helius_trigger_reason"] = hint.get("reason")
            merged["helius_wallet_target"] = int(hint.get("max_wallets", 0) or 0)
            merged["helius_local_score_hint"] = float(hint.get("local_score", 0.0) or 0.0)
        return merged
    except Exception as exc:
        mint = state_dict.get("mint") or "unknown"
        print(f"[helius] enrichment-error | {mint} | {exc}")
        merged = dict(state_dict)
        merged["helius_enrichment_status"] = "collector_error"
        merged["helius_last_error"] = str(exc)
        merged["helius_cached_error"] = None
        merged["helius_requested_wallet_count"] = 0
        merged["helius_completed_wallet_count"] = 0
        merged["helius_selected_wallet_count"] = 0
        merged["helius_profiled_wallet_count"] = 0
        merged["helius_profile_depth_bucket"] = "none"
        merged["helius_profile_completion_ratio"] = 0.0
        merged["helius_partial_enrichment"] = False
        merged["helius_wallet_attempt_details"] = []
        merged["helius_time_budget_seconds"] = ENRICHMENT_TIME_BUDGET_SECONDS
        merged["helius_time_budget_exhausted"] = False
        if hint:
            merged["helius_enrichment_tier"] = hint.get("tier", "none")
            merged["helius_trigger_reason"] = hint.get("reason")
            merged["helius_wallet_target"] = int(hint.get("max_wallets", 0) or 0)
            merged["helius_local_score_hint"] = float(hint.get("local_score", 0.0) or 0.0)
        return merged


def _coerce_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _load_open_trade_mints() -> set[str]:
    if not OPEN_TRADES_PATH.exists():
        return set()
    try:
        payload = json.loads(OPEN_TRADES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(payload, dict):
        return set()
    return {str(mint).strip() for mint in payload.keys() if str(mint).strip()}


def _quant_features(snapshot: dict | None) -> dict:
    if not isinstance(snapshot, dict):
        return {}
    quant = snapshot.get("quant_features", {}) or {}
    return quant if isinstance(quant, dict) else {}


def _revisit_metrics(state_dict: dict, local_snapshot: dict) -> dict:
    quant = _quant_features(local_snapshot)
    return {
        "status": str(state_dict.get("status") or "").strip(),
        "status_reason": str(state_dict.get("status_reason") or "").strip(),
        "local_score": _coerce_float(local_snapshot.get("composite_score"), 0.0),
        "net5m": _coerce_float(state_dict.get("net_sol_flow_last_5m"), 0.0),
        "buys5m": int(state_dict.get("buys_last_5m") or 0),
        "sells5m": int(state_dict.get("sells_last_5m") or 0),
        "market_cap_sol": _coerce_float(state_dict.get("market_cap_sol"), 0.0),
        "participant_quality": _coerce_float(
            quant.get("participant_quality_score_v2"),
            _coerce_float(state_dict.get("participant_quality_score_v2"), 0.0),
        ),
    }


class MintRevisitBackoffManager:
    def __init__(self) -> None:
        self._records: dict[str, dict] = {}

    def clear(self, mint: str) -> None:
        if mint:
            self._records.pop(mint, None)

    def should_skip(
        self,
        mint: str,
        metrics: dict,
        *,
        has_open_trade: bool,
    ) -> tuple[bool, dict | None]:
        if not mint:
            return False, None
        if has_open_trade:
            self.clear(mint)
            return False, None

        record = self._records.get(mint)
        if not record:
            return False, None

        now = monotonic()
        if now >= float(record.get("cooldown_until", 0.0) or 0.0):
            self.clear(mint)
            return False, None

        if self._has_material_improvement(record.get("baseline", {}), metrics):
            self.clear(mint)
            return False, {
                "reason": "material_improvement",
                "remaining_seconds": 0.0,
            }

        record["suppressed_count"] = int(record.get("suppressed_count", 0) or 0) + 1
        remaining = max(0.0, float(record.get("cooldown_until", 0.0) or 0.0) - now)
        return True, {
            "reason": str(record.get("reason") or "revisit_backoff"),
            "remaining_seconds": round(remaining, 1),
            "suppressed_count": int(record.get("suppressed_count", 0) or 0),
        }

    def arm(self, mint: str, reason: str, metrics: dict) -> None:
        if not mint:
            return
        self._records[mint] = {
            "reason": reason,
            "baseline": dict(metrics),
            "cooldown_until": monotonic() + REVISIT_BACKOFF_SECONDS,
            "suppressed_count": 0,
        }

    def _has_material_improvement(self, baseline: dict, current: dict) -> bool:
        base_status = str(baseline.get("status") or "")
        current_status = str(current.get("status") or "")
        if base_status in {"invalidated", "background"} and current_status not in {
            "invalidated",
            "background",
            "",
        }:
            return True

        base_score = _coerce_float(baseline.get("local_score"), 0.0)
        current_score = _coerce_float(current.get("local_score"), 0.0)
        if current_score >= base_score + REVISIT_SCORE_IMPROVEMENT_THRESHOLD:
            return True

        base_net = _coerce_float(baseline.get("net5m"), 0.0)
        current_net = _coerce_float(current.get("net5m"), 0.0)
        if current_net >= base_net + REVISIT_NET_FLOW_IMPROVEMENT_THRESHOLD:
            return True

        base_buys = int(baseline.get("buys5m") or 0)
        current_buys = int(current.get("buys5m") or 0)
        if current_buys >= base_buys + REVISIT_BUY_COUNT_IMPROVEMENT_THRESHOLD:
            return True

        base_mcap = _coerce_float(baseline.get("market_cap_sol"), 0.0)
        current_mcap = _coerce_float(current.get("market_cap_sol"), 0.0)
        if base_mcap > 0:
            if (
                current_mcap >= base_mcap * REVISIT_MCAP_IMPROVEMENT_RATIO
                and current_mcap >= base_mcap + REVISIT_MCAP_IMPROVEMENT_ABS_SOL
            ):
                return True

        base_quality = _coerce_float(baseline.get("participant_quality"), 0.0)
        current_quality = _coerce_float(current.get("participant_quality"), 0.0)
        if current_quality >= max(
            0.45,
            base_quality + REVISIT_PARTICIPANT_QUALITY_IMPROVEMENT,
        ):
            return True

        return False


def _should_arm_revisit_backoff(
    state_dict: dict,
    local_snapshot: dict,
    strategy_snapshot: dict,
    trade_action: str,
    paper_trade: dict | None,
    *,
    has_open_trade: bool,
) -> tuple[bool, str | None]:
    if has_open_trade:
        return False, None

    status = str(state_dict.get("status") or "").strip()
    status_reason = str(state_dict.get("status_reason") or "").strip()
    local_score = _coerce_float(local_snapshot.get("composite_score"), 0.0)
    final_score = _coerce_float(strategy_snapshot.get("composite_score"), 0.0)
    priority_tier = str(strategy_snapshot.get("priority_tier") or "").strip()
    net5m = _coerce_float(state_dict.get("net_sol_flow_last_5m"), 0.0)
    buys5m = int(state_dict.get("buys_last_5m") or 0)
    qualify_reason = str((paper_trade or {}).get("qualify_reason") or "").strip()
    wait_reason = str((paper_trade or {}).get("reason") or "").strip()

    if trade_action == "entry_wait" and qualify_reason == "invalidated":
        return True, "entry_wait:invalidated"
    if trade_action == "entry_wait" and wait_reason == "await_entry_confirmation" and local_score < 56:
        return True, "entry_wait:low_score_confirmation"
    if status == "invalidated":
        return True, f"state:{status_reason or 'invalidated'}"
    if priority_tier == "background" and final_score < 60 and (net5m <= 0 or buys5m <= 1):
        return True, "background_low_priority"
    return False, None


async def process_stream_session(
    state_manager: TokenStateManager,
    event_manager: SignalEventManager,
    strategy_engine: StrategyScoreEngine,
    leaderboard: StrategyLeaderboard,
    enrichment_manager: WalletEnrichmentManager,
    revisit_backoff: MintRevisitBackoffManager,
    active_paper_trade_mints: set[str],
    seen_mints: set[str],
    watched_mints: set[str],
):
    token_batch: list[dict] = []
    trade_batch: list[dict] = []
    trades_since_leaderboard = 0

    sweep_stop_event = asyncio.Event()

    async def periodic_paper_sweep() -> None:
        await maybe_run_paper_sweep("startup")
        while not sweep_stop_event.is_set():
            try:
                await asyncio.wait_for(
                    sweep_stop_event.wait(),
                    timeout=PAPER_SWEEP_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                await maybe_run_paper_sweep("interval")

    sweep_task = asyncio.create_task(periodic_paper_sweep())

    try:
        async with websockets.connect(
            PUMPPORTAL_WS_URL,
            ping_interval=WS_PING_INTERVAL_SECONDS,
            ping_timeout=WS_PING_TIMEOUT_SECONDS,
            open_timeout=WS_OPEN_TIMEOUT_SECONDS,
            close_timeout=WS_CLOSE_TIMEOUT_SECONDS,
            max_queue=WS_MAX_QUEUE,
        ) as websocket:
            await websocket.send(json.dumps({"method": "subscribeNewToken"}))
            print("[pumpportal] subscribed to new token events")
    
            for mint in watched_mints:
                await subscribe_token_trades(websocket, mint)
    
            async for message in websocket:
                try:
                    raw = json.loads(message)
                except json.JSONDecodeError:
                    print("[pumpportal] received non-json message")
                    continue
    
                tx_type = raw.get("txType")
    
                if tx_type == "create":
                    token = normalize_token(raw)
                    mint = token.get("mint")
                    if not mint or mint in seen_mints:
                        continue
    
                    seen_mints.add(mint)
                    token_batch.append(token)
                    state = state_manager.process_new_token(token)
                    print(f"[state] {mint} status={state.status} reason={state.status_reason}")
                    print(
                        f"[pumpportal] new token | {token.get('symbol') or '?'} | "
                        f"{token.get('name') or '?'} | {mint} | "
                        f"initial_buy={token.get('initial_buy')} | "
                        f"mcap={token.get('market_cap_sol')}"
                    )
    
                    if should_watch_token(token) and mint not in watched_mints:
                        watched_mints.add(mint)
                        await subscribe_token_trades(websocket, mint)
    
                    if len(token_batch) >= 10:
                        saved_path = save_snapshot("pumpportal_ws_tokens", token_batch)
                        print(f"[pumpportal] token snapshot saved: {saved_path}")
                        token_batch = []
                    continue
    
                mint = raw.get("mint")
                if mint not in watched_mints:
                    continue
    
                previous_state = state_manager.get_or_create_state(mint).to_dict()
                trade = normalize_trade(raw)
                trade_batch.append(trade)
    
                state = state_manager.process_trade(trade)
                state_dict = state.to_dict()
                print(
                    f"[state] {mint} status={state.status} "
                    f"net5m={round(state.net_sol_flow_last_5m, 4)} "
                    f"buys5m={state.buys_last_5m} sells5m={state.sells_last_5m}"
                )
    
                events = event_manager.process_state_transition(previous_state, state_dict)
                for event in events:
                    print(
                        f"[signal] {mint} | {event['event_type']} | "
                        f"severity={event['severity']}"
                    )
    
                previous_snapshot = strategy_engine.latest_snapshots.get(mint)
                local_snapshot = strategy_engine.evaluate(
                    state_dict,
                    events,
                    include_helius=False,
                    persist=False,
                )
                revisit_metrics = _revisit_metrics(state_dict, local_snapshot)
                skip_revisit, skip_info = revisit_backoff.should_skip(
                    mint,
                    revisit_metrics,
                    has_open_trade=(mint in active_paper_trade_mints),
                )
                if skip_revisit:
                    print(
                        f"[revisit] suppressed | {mint} | "
                        f"reason={skip_info.get('reason')} | "
                        f"remaining={skip_info.get('remaining_seconds')}s | "
                        f"suppressed={skip_info.get('suppressed_count', 0)}"
                    )
                    print(
                        f"[pumpportal] trade | {mint} | {trade.get('tx_type')} | "
                        f"sol={trade.get('sol_amount')}"
                    )
                    if len(trade_batch) >= 20:
                        saved_path = save_snapshot("pumpportal_ws_trades", trade_batch)
                        print(f"[pumpportal] trade snapshot saved: {saved_path}")
                        trade_batch = []
                    continue
    
                enrichment_hint = build_enrichment_hint(
                    state_dict,
                    events,
                    local_snapshot,
                    previous_snapshot=previous_snapshot,
                )
    
                enriched_state_dict = await enrich_state_with_timeout(
                    enrichment_manager,
                    state_dict,
                    enrichment_hint,
                )
    
                cohort_summary = (
                    enriched_state_dict.get("helius_wallet_cohort_summary", {}) or {}
                )
                helius_status = str(
                    enriched_state_dict.get("helius_enrichment_status", "unknown")
                )
                helius_error = enriched_state_dict.get("helius_last_error")
                profile_count = int(
                    enriched_state_dict.get(
                        "helius_completed_wallet_count",
                        cohort_summary.get("profile_count", 0),
                    )
                    or 0
                )
                requested_count = int(
                    enriched_state_dict.get("helius_requested_wallet_count", 0) or 0
                )
                depth_bucket = str(
                    enriched_state_dict.get("helius_profile_depth_bucket", "none")
                )
                completion_ratio = float(
                    enriched_state_dict.get("helius_profile_completion_ratio", 0.0) or 0.0
                )
    
                if helius_status not in {"disabled", "unqualified", "unknown"}:
                    message = (
                        f"[helius] {mint} | "
                        f"tier={enriched_state_dict.get('helius_enrichment_tier', 'none')} "
                        f"| status={helius_status} "
                        f"| profiles={profile_count}/{requested_count} "
                        f"| bucket={depth_bucket} "
                        f"| completion={round(completion_ratio, 4)}"
                    )
    
                    if cohort_summary.get("profile_count", 0) > 0:
                        message += (
                            f" | cohort_quality={cohort_summary.get('cohort_quality_score')} "
                            f"| fresh_share={cohort_summary.get('fresh_wallet_share')} "
                            f"| recycled_share={cohort_summary.get('recycled_wallet_share')}"
                        )
    
                    if helius_error:
                        message += f" | error={helius_error}"
    
                    print(message)
    
                strategy_snapshot = strategy_engine.evaluate(
                    enriched_state_dict,
                    events,
                    local_snapshot=local_snapshot,
                )
                print(
                    f"[strategy] {mint} | "
                    f"local={local_snapshot['composite_score']} -> "
                    f"final={strategy_snapshot['composite_score']} "
                    f"| delta={strategy_snapshot.get('enriched_score_delta')} | "
                    f"tier={strategy_snapshot['priority_tier']} | "
                    f"confidence={strategy_snapshot['confidence']}"
                )
    
                alert = write_alert(strategy_snapshot)
                print(
                    f"[alert] {mint} | action={alert['action']} | band={alert['band']} | "
                    f"score={alert['score']} | tier={alert['priority_tier']}"
                )
    
                trade_action, paper_trade = sync_trade(strategy_snapshot, alert)
                if trade_action in {"opened", "reopened"} and paper_trade:
                    active_paper_trade_mints.add(mint)
                    revisit_backoff.clear(mint)
                    print(
                        f"[paper] {trade_action} | {mint} | "
                        f"entry_mcap={paper_trade['entry_market_cap_sol']} | "
                        f"entry_score={paper_trade['entry_score']}"
                    )
                elif trade_action == "updated" and paper_trade:
                    active_paper_trade_mints.add(mint)
                    revisit_backoff.clear(mint)
                    print(
                        f"[paper] updated | {mint} | "
                        f"current_mcap={paper_trade.get('current_market_cap_sol')} | "
                        f"pnl={paper_trade.get('pnl_pct_proxy')}"
                    )
                elif trade_action == "closed" and paper_trade:
                    active_paper_trade_mints.discard(mint)
                    print(
                        f"[paper] closed | {mint} | "
                        f"exit_mcap={paper_trade.get('exit_market_cap_sol')} | "
                        f"pnl={paper_trade.get('pnl_pct_proxy')} | "
                        f"reason={paper_trade.get('exit_reason')}"
                    )
                elif trade_action == "reclaim_wait" and paper_trade:
                    progress = paper_trade.get("confirmation_progress")
                    required = paper_trade.get("confirmation_required")
                    qualify_reason = paper_trade.get("qualify_reason")
                    confirmation_mode = paper_trade.get("confirmation_mode")
                    extra = ""
                    if progress is not None and required is not None:
                        extra = f" | progress={progress}/{required}"
                    if confirmation_mode:
                        extra += f" | mode={confirmation_mode}"
                    if qualify_reason:
                        extra += f" | qualify_reason={qualify_reason}"
                    print(
                        f"[paper] reclaim-wait | {mint} | "
                        f"reason={paper_trade.get('reason')}{extra}"
                    )
                elif trade_action == "entry_wait" and paper_trade:
                    progress = paper_trade.get("confirmation_progress")
                    required = paper_trade.get("confirmation_required")
                    qualify_reason = paper_trade.get("qualify_reason")
                    extra = ""
                    if progress is not None and required is not None:
                        extra = f" | progress={progress}/{required}"
                    if qualify_reason:
                        extra += f" | qualify_reason={qualify_reason}"
                    print(
                        f"[paper] entry-wait | {mint} | "
                        f"reason={paper_trade.get('reason')}{extra}"
                    )
    
                arm_backoff, arm_reason = _should_arm_revisit_backoff(
                    state_dict,
                    local_snapshot,
                    strategy_snapshot,
                    trade_action,
                    paper_trade,
                    has_open_trade=(mint in active_paper_trade_mints),
                )
                if arm_backoff and arm_reason:
                    revisit_backoff.arm(mint, arm_reason, revisit_metrics)
                    print(
                        f"[revisit] armed | {mint} | cooldown={REVISIT_BACKOFF_SECONDS}s | "
                        f"reason={arm_reason} | local={round(revisit_metrics.get('local_score', 0.0), 2)} | "
                        f"net5m={round(revisit_metrics.get('net5m', 0.0), 4)} | "
                        f"buys5m={revisit_metrics.get('buys5m', 0)}"
                    )
                else:
                    revisit_backoff.clear(mint)
    
                escalate, score, reasons = should_escalate(enriched_state_dict)
                if escalate:
                    path, change_kind, regime = write_escalation_regime_if_changed(
                        state_dict,
                        score,
                        reasons,
                    )
                    if path:
                        print(
                            f"[filter] escalated-{change_kind} | {mint} | score={score} | "
                            f"mcap_band={regime['mcap_band']} | "
                            f"net_band={regime['net_flow_band']} | "
                            f"reasons={','.join(reasons[:4])} | saved={path}"
                        )
                    else:
                        print(
                            f"[filter] escalated-nochange | {mint} | score={score} | "
                            f"mcap_band={regime['mcap_band']} | "
                            f"net_band={regime['net_flow_band']} | "
                            f"reasons={','.join(reasons[:4])}"
                        )
    
                print(
                    f"[pumpportal] trade | {mint} | {trade.get('tx_type')} | "
                    f"sol={trade.get('sol_amount')}"
                )
    
                trades_since_leaderboard += 1
                if (
                    events
                    or strategy_snapshot["composite_score"] >= 40
                    or trades_since_leaderboard >= 10
                ):
                    payload = leaderboard.build_and_persist(
                        strategy_engine.latest_snapshots,
                        state_manager.states,
                    )
                    trades_since_leaderboard = 0
                    print(
                        f"[leaderboard] updated | entries={payload['entry_count']} | "
                        f"top={payload['rows'][0]['rank_mint'] if payload['rows'] else 'none'}"
                    )
    
                if len(trade_batch) >= 20:
                    saved_path = save_snapshot("pumpportal_ws_trades", trade_batch)
                    print(f"[pumpportal] trade snapshot saved: {saved_path}")
                    trade_batch = []
    finally:
        sweep_stop_event.set()
        sweep_task.cancel()
        await asyncio.gather(sweep_task, return_exceptions=True)


async def stream_new_tokens():
    print("PumpPortal websocket collector started.")

    state_manager = TokenStateManager()
    event_manager = SignalEventManager()
    strategy_engine = StrategyScoreEngine()
    leaderboard = StrategyLeaderboard(max_entries=25)
    enrichment_manager = WalletEnrichmentManager()
    revisit_backoff = MintRevisitBackoffManager()
    active_paper_trade_mints = _load_open_trade_mints()
    if active_paper_trade_mints:
        print(
            f"[paper] seeded-open-trades | count={len(active_paper_trade_mints)}"
        )
    seen_mints: set[str] = set()
    watched_mints: set[str] = set()

    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
    while True:
        try:
            await process_stream_session(
                state_manager,
                event_manager,
                strategy_engine,
                leaderboard,
                enrichment_manager,
                revisit_backoff,
                active_paper_trade_mints,
                seen_mints,
                watched_mints,
            )
            reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        except (
            ConnectionClosedError,
            ConnectionClosed,
            OSError,
            asyncio.TimeoutError,
        ) as exc:
            print(
                f"[pumpportal] websocket reconnect | "
                f"reason={exc} | retry_in={reconnect_delay}s"
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * 2,
                RECONNECT_MAX_DELAY_SECONDS,
            )
        except asyncio.CancelledError:
            print("[pumpportal] shutdown requested | closing collector")
            raise
        except KeyboardInterrupt:
            print("[pumpportal] shutdown requested | closing collector")
            break
        except Exception as exc:
            print(
                f"[pumpportal] collector error | {exc} | "
                f"retry_in={reconnect_delay}s"
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * 2,
                RECONNECT_MAX_DELAY_SECONDS,
            )


def run_pumpfun_polling():
    try:
        asyncio.run(stream_new_tokens())
    except KeyboardInterrupt:
        print("[pumpportal] stopped cleanly")
