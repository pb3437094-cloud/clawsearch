import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from state.participant_quality import build_participant_metrics


STATE_DIR = Path("data/token_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)

STATE_INDEX_FILE = STATE_DIR / "current_state.json"

DUST_TRADE_SOL_THRESHOLD = 0.001


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class TokenStatus(str, Enum):
    NEW = "new"
    WATCHING = "watching"
    ESCALATED = "escalated"
    COOLING_OFF = "cooling_off"
    INACTIVE = "inactive"
    REAWAKENED = "reawakened"


@dataclass
class TokenState:
    mint: str

    # Identity / launch metadata
    symbol: str | None = None
    name: str | None = None
    creator_wallet: str | None = None
    uri: str | None = None
    pool: str | None = None
    bonding_curve_key: str | None = None
    launch_signature: str | None = None
    created_timestamp: str | None = None

    # Timestamps
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    first_trade_at: str | None = None
    last_trade_at: str | None = None
    last_buy_at: str | None = None
    last_sell_at: str | None = None
    watchlisted_at: str | None = None
    escalated_at: str | None = None
    cooled_off_at: str | None = None
    inactive_at: str | None = None
    reawakened_at: str | None = None

    # Launch metrics
    initial_buy: float = 0.0
    launch_sol_amount: float = 0.0
    launch_market_cap_sol: float = 0.0

    # Live market metrics
    current_market_cap_sol: float = 0.0
    peak_market_cap_sol: float = 0.0
    trough_market_cap_sol: float = 0.0
    peak_market_cap_at: str | None = None
    trough_market_cap_at: str | None = None

    # Aggregate trade metrics
    total_trade_count: int = 0
    total_buy_count: int = 0
    total_sell_count: int = 0

    total_buy_sol: float = 0.0
    total_sell_sol: float = 0.0
    net_sol_flow: float = 0.0

    largest_buy_sol: float = 0.0
    largest_sell_sol: float = 0.0

    # Rolling windows
    trades_last_1m: int = 0
    trades_last_5m: int = 0
    buys_last_1m: int = 0
    buys_last_5m: int = 0
    sells_last_1m: int = 0
    sells_last_5m: int = 0
    buy_sol_last_1m: float = 0.0
    buy_sol_last_5m: float = 0.0
    sell_sol_last_1m: float = 0.0
    sell_sol_last_5m: float = 0.0
    net_sol_flow_last_1m: float = 0.0
    net_sol_flow_last_5m: float = 0.0

    # Participants
    unique_traders: int = 0
    unique_buyers: int = 0
    unique_sellers: int = 0
    unique_traders_last_1m: int = 0
    unique_traders_last_5m: int = 0
    unique_buyers_last_1m: int = 0
    unique_buyers_last_5m: int = 0
    unique_sellers_last_1m: int = 0
    unique_sellers_last_5m: int = 0
    new_traders_last_1m: int = 0
    new_traders_last_5m: int = 0
    new_buyers_last_1m: int = 0
    new_buyers_last_5m: int = 0
    repeat_wallet_ratio: float = 0.0
    repeat_buyer_ratio: float = 0.0
    buyer_overlap_ratio: float = 0.0
    participant_churn_ratio: float = 0.0
    cluster_entropy: float = 0.0
    participant_concentration_score: float = 0.0
    wallet_novelty_score: float = 0.0
    new_buyer_velocity: float = 0.0
    participant_quality_score_v2: float = 0.0

    # State and lifecycle
    status: str = TokenStatus.NEW.value
    status_reason: str = "first_seen"
    was_ever_watchlisted: bool = False
    was_ever_escalated: bool = False
    revival_count: int = 0

    # Internal working data
    recent_trades: list[dict[str, Any]] = field(default_factory=list)
    trader_wallets: set[str] = field(default_factory=set)
    buyer_wallets: set[str] = field(default_factory=set)
    seller_wallets: set[str] = field(default_factory=set)
    trader_trade_counts: dict[str, int] = field(default_factory=dict)
    buyer_trade_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["trader_wallets"] = sorted(self.trader_wallets)
        data["buyer_wallets"] = sorted(self.buyer_wallets)
        data["seller_wallets"] = sorted(self.seller_wallets)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TokenState":
        trader_wallets = list(data.get("trader_wallets", []))
        buyer_wallets = list(data.get("buyer_wallets", []))
        seller_wallets = list(data.get("seller_wallets", []))

        trader_trade_counts = {
            wallet: int(count or 0)
            for wallet, count in (data.get("trader_trade_counts") or {}).items()
        }
        buyer_trade_counts = {
            wallet: int(count or 0)
            for wallet, count in (data.get("buyer_trade_counts") or {}).items()
        }

        if not trader_trade_counts and trader_wallets:
            trader_trade_counts = {wallet: 1 for wallet in trader_wallets}
        if not buyer_trade_counts and buyer_wallets:
            buyer_trade_counts = {wallet: 1 for wallet in buyer_wallets}

        state = cls(
            mint=data["mint"],
            symbol=data.get("symbol"),
            name=data.get("name"),
            creator_wallet=data.get("creator_wallet"),
            uri=data.get("uri"),
            pool=data.get("pool"),
            bonding_curve_key=data.get("bonding_curve_key"),
            launch_signature=data.get("launch_signature"),
            created_timestamp=data.get("created_timestamp"),
            first_seen_at=data.get("first_seen_at"),
            last_seen_at=data.get("last_seen_at"),
            first_trade_at=data.get("first_trade_at"),
            last_trade_at=data.get("last_trade_at"),
            last_buy_at=data.get("last_buy_at"),
            last_sell_at=data.get("last_sell_at"),
            watchlisted_at=data.get("watchlisted_at"),
            escalated_at=data.get("escalated_at"),
            cooled_off_at=data.get("cooled_off_at"),
            inactive_at=data.get("inactive_at"),
            reawakened_at=data.get("reawakened_at"),
            initial_buy=float(data.get("initial_buy", 0.0) or 0.0),
            launch_sol_amount=float(data.get("launch_sol_amount", 0.0) or 0.0),
            launch_market_cap_sol=float(data.get("launch_market_cap_sol", 0.0) or 0.0),
            current_market_cap_sol=float(data.get("current_market_cap_sol", 0.0) or 0.0),
            peak_market_cap_sol=float(data.get("peak_market_cap_sol", 0.0) or 0.0),
            trough_market_cap_sol=float(data.get("trough_market_cap_sol", 0.0) or 0.0),
            peak_market_cap_at=data.get("peak_market_cap_at"),
            trough_market_cap_at=data.get("trough_market_cap_at"),
            total_trade_count=int(data.get("total_trade_count", 0) or 0),
            total_buy_count=int(data.get("total_buy_count", 0) or 0),
            total_sell_count=int(data.get("total_sell_count", 0) or 0),
            total_buy_sol=float(data.get("total_buy_sol", 0.0) or 0.0),
            total_sell_sol=float(data.get("total_sell_sol", 0.0) or 0.0),
            net_sol_flow=float(data.get("net_sol_flow", 0.0) or 0.0),
            largest_buy_sol=float(data.get("largest_buy_sol", 0.0) or 0.0),
            largest_sell_sol=float(data.get("largest_sell_sol", 0.0) or 0.0),
            trades_last_1m=int(data.get("trades_last_1m", 0) or 0),
            trades_last_5m=int(data.get("trades_last_5m", 0) or 0),
            buys_last_1m=int(data.get("buys_last_1m", 0) or 0),
            buys_last_5m=int(data.get("buys_last_5m", 0) or 0),
            sells_last_1m=int(data.get("sells_last_1m", 0) or 0),
            sells_last_5m=int(data.get("sells_last_5m", 0) or 0),
            buy_sol_last_1m=float(data.get("buy_sol_last_1m", 0.0) or 0.0),
            buy_sol_last_5m=float(data.get("buy_sol_last_5m", 0.0) or 0.0),
            sell_sol_last_1m=float(data.get("sell_sol_last_1m", 0.0) or 0.0),
            sell_sol_last_5m=float(data.get("sell_sol_last_5m", 0.0) or 0.0),
            net_sol_flow_last_1m=float(data.get("net_sol_flow_last_1m", 0.0) or 0.0),
            net_sol_flow_last_5m=float(data.get("net_sol_flow_last_5m", 0.0) or 0.0),
            unique_traders=int(data.get("unique_traders", 0) or 0),
            unique_buyers=int(data.get("unique_buyers", 0) or 0),
            unique_sellers=int(data.get("unique_sellers", 0) or 0),
            unique_traders_last_1m=int(data.get("unique_traders_last_1m", 0) or 0),
            unique_traders_last_5m=int(data.get("unique_traders_last_5m", 0) or 0),
            unique_buyers_last_1m=int(data.get("unique_buyers_last_1m", 0) or 0),
            unique_buyers_last_5m=int(data.get("unique_buyers_last_5m", 0) or 0),
            unique_sellers_last_1m=int(data.get("unique_sellers_last_1m", 0) or 0),
            unique_sellers_last_5m=int(data.get("unique_sellers_last_5m", 0) or 0),
            new_traders_last_1m=int(data.get("new_traders_last_1m", 0) or 0),
            new_traders_last_5m=int(data.get("new_traders_last_5m", 0) or 0),
            new_buyers_last_1m=int(data.get("new_buyers_last_1m", 0) or 0),
            new_buyers_last_5m=int(data.get("new_buyers_last_5m", 0) or 0),
            repeat_wallet_ratio=float(data.get("repeat_wallet_ratio", 0.0) or 0.0),
            repeat_buyer_ratio=float(data.get("repeat_buyer_ratio", 0.0) or 0.0),
            buyer_overlap_ratio=float(data.get("buyer_overlap_ratio", 0.0) or 0.0),
            participant_churn_ratio=float(data.get("participant_churn_ratio", 0.0) or 0.0),
            cluster_entropy=float(data.get("cluster_entropy", 0.0) or 0.0),
            participant_concentration_score=float(data.get("participant_concentration_score", 0.0) or 0.0),
            wallet_novelty_score=float(data.get("wallet_novelty_score", 0.0) or 0.0),
            new_buyer_velocity=float(data.get("new_buyer_velocity", 0.0) or 0.0),
            participant_quality_score_v2=float(data.get("participant_quality_score_v2", 0.0) or 0.0),
            status=data.get("status", TokenStatus.NEW.value),
            status_reason=data.get("status_reason", "loaded_from_disk"),
            was_ever_watchlisted=bool(data.get("was_ever_watchlisted", False)),
            was_ever_escalated=bool(data.get("was_ever_escalated", False)),
            revival_count=int(data.get("revival_count", 0) or 0),
            recent_trades=data.get("recent_trades", []),
            trader_trade_counts=trader_trade_counts,
            buyer_trade_counts=buyer_trade_counts,
        )
        state.trader_wallets = set(trader_wallets)
        state.buyer_wallets = set(buyer_wallets)
        state.seller_wallets = set(seller_wallets)
        return state


class TokenStateManager:
    def __init__(self) -> None:
        self.states: dict[str, TokenState] = {}
        self.load_state_index()

    def load_state_index(self) -> None:
        if not STATE_INDEX_FILE.exists():
            return

        try:
            with open(STATE_INDEX_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)

            for mint, state_data in payload.items():
                state = TokenState.from_dict(state_data)
                self._trim_recent_trades(state)
                self._recompute_rolling_metrics(state)
                self.states[mint] = state

        except Exception as e:
            print(f"[state] failed to load state index: {e}")

    def save_state_index(self) -> None:
        payload = {mint: state.to_dict() for mint, state in self.states.items()}

        with open(STATE_INDEX_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    def save_token_state(self, mint: str) -> None:
        state = self.states[mint]
        path = STATE_DIR / f"{mint}.json"

        with open(path, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f, indent=2, ensure_ascii=False)

    def persist(self, mint: str) -> None:
        self.save_token_state(mint)
        self.save_state_index()

    def get_or_create_state(self, mint: str) -> TokenState:
        if mint not in self.states:
            self.states[mint] = TokenState(mint=mint)
        return self.states[mint]

    def process_new_token(self, token: dict[str, Any]) -> TokenState:
        mint = token["mint"]
        state = self.get_or_create_state(mint)
        now = token.get("captured_at_utc") or utc_now_iso()

        if not state.first_seen_at:
            state.first_seen_at = now
        state.last_seen_at = now

        state.symbol = token.get("symbol") or state.symbol
        state.name = token.get("name") or state.name
        state.creator_wallet = token.get("creator_wallet") or state.creator_wallet
        state.uri = token.get("uri") or state.uri
        state.pool = token.get("pool") or state.pool
        state.bonding_curve_key = token.get("bonding_curve_key") or state.bonding_curve_key
        state.launch_signature = token.get("raw", {}).get("signature") or state.launch_signature
        state.created_timestamp = token.get("created_timestamp") or state.created_timestamp

        state.initial_buy = float(token.get("initial_buy", 0.0) or 0.0)
        state.launch_sol_amount = float(token.get("sol_amount", 0.0) or 0.0)
        state.launch_market_cap_sol = float(token.get("market_cap_sol", 0.0) or 0.0)
        state.current_market_cap_sol = state.launch_market_cap_sol or state.current_market_cap_sol

        if state.peak_market_cap_sol == 0.0:
            state.peak_market_cap_sol = state.current_market_cap_sol
            state.peak_market_cap_at = now
        if state.trough_market_cap_sol == 0.0:
            state.trough_market_cap_sol = state.current_market_cap_sol
            state.trough_market_cap_at = now

        self.recompute_status(state)
        self.persist(mint)
        return state

    def process_trade(self, trade: dict[str, Any]) -> TokenState:
        mint = trade["mint"]
        state = self.get_or_create_state(mint)
        now = trade.get("captured_at_utc") or utc_now_iso()

        state.last_seen_at = now
        if not state.first_trade_at:
            state.first_trade_at = now
        state.last_trade_at = now

        sol_amount = float(trade.get("sol_amount", 0.0) or 0.0)
        tx_type = (trade.get("tx_type") or "").lower()
        trader_wallet = trade.get("trader_wallet")

        market_cap_sol = float(trade.get("market_cap_sol", 0.0) or 0.0)
        if market_cap_sol > 0:
            state.current_market_cap_sol = market_cap_sol
            if market_cap_sol > state.peak_market_cap_sol:
                state.peak_market_cap_sol = market_cap_sol
                state.peak_market_cap_at = now
            if state.trough_market_cap_sol == 0.0:
                state.trough_market_cap_sol = market_cap_sol
                state.trough_market_cap_at = now
            elif market_cap_sol < state.trough_market_cap_sol:
                state.trough_market_cap_sol = market_cap_sol
                state.trough_market_cap_at = now

        state.total_trade_count += 1

        if trader_wallet:
            state.trader_wallets.add(trader_wallet)
            state.trader_trade_counts[trader_wallet] = state.trader_trade_counts.get(trader_wallet, 0) + 1

        if tx_type == "buy":
            state.total_buy_count += 1
            state.total_buy_sol += sol_amount
            state.last_buy_at = now
            state.largest_buy_sol = max(state.largest_buy_sol, sol_amount)
            if trader_wallet:
                state.buyer_wallets.add(trader_wallet)
                state.buyer_trade_counts[trader_wallet] = state.buyer_trade_counts.get(trader_wallet, 0) + 1

        elif tx_type == "sell":
            state.total_sell_count += 1
            state.total_sell_sol += sol_amount
            state.last_sell_at = now
            state.largest_sell_sol = max(state.largest_sell_sol, sol_amount)
            if trader_wallet:
                state.seller_wallets.add(trader_wallet)

        state.net_sol_flow = state.total_buy_sol - state.total_sell_sol
        state.unique_traders = len(state.trader_wallets)
        state.unique_buyers = len(state.buyer_wallets)
        state.unique_sellers = len(state.seller_wallets)

        is_dust_trade = sol_amount > 0 and sol_amount < DUST_TRADE_SOL_THRESHOLD
        state.recent_trades.append(
            {
                "captured_at_utc": now,
                "tx_type": tx_type,
                "sol_amount": sol_amount,
                "effective_sol_amount": 0.0 if is_dust_trade else sol_amount,
                "is_dust_trade": is_dust_trade,
                "trader_wallet": trader_wallet,
                "market_cap_sol": market_cap_sol,
            }
        )

        self._trim_recent_trades(state)
        self._recompute_rolling_metrics(state)
        self.recompute_status(state)
        self.persist(mint)
        return state

    def _trim_recent_trades(self, state: TokenState) -> None:
        now = iso_to_dt(utc_now_iso())
        if not now:
            return

        trimmed = []
        for trade in state.recent_trades:
            trade_dt = iso_to_dt(trade.get("captured_at_utc"))
            if not trade_dt:
                continue
            age_seconds = (now - trade_dt).total_seconds()
            if age_seconds <= 300:
                trimmed.append(trade)

        state.recent_trades = trimmed

    def _recompute_rolling_metrics(self, state: TokenState) -> None:
        now = iso_to_dt(utc_now_iso())
        if not now:
            return

        raw_trades_1m = []
        raw_trades_5m = []
        trades_1m = []
        trades_5m = []
        prior_trades_5m = []

        for trade in state.recent_trades:
            trade_dt = iso_to_dt(trade.get("captured_at_utc"))
            if not trade_dt:
                continue

            age_seconds = (now - trade_dt).total_seconds()
            is_dust_trade = bool(trade.get("is_dust_trade")) or float(trade.get("sol_amount", 0.0) or 0.0) < DUST_TRADE_SOL_THRESHOLD

            if age_seconds <= 300:
                raw_trades_5m.append(trade)
                if not is_dust_trade:
                    trades_5m.append(trade)
                    if age_seconds > 60:
                        prior_trades_5m.append(trade)
            if age_seconds <= 60:
                raw_trades_1m.append(trade)
                if not is_dust_trade:
                    trades_1m.append(trade)

        def count_type(trades: list[dict[str, Any]], tx_type: str) -> int:
            return sum(1 for t in trades if t.get("tx_type") == tx_type)

        def sum_type(trades: list[dict[str, Any]], tx_type: str) -> float:
            return sum(float(t.get("effective_sol_amount", t.get("sol_amount", 0.0)) or 0.0) for t in trades if t.get("tx_type") == tx_type)

        state.trades_last_1m = len(trades_1m)
        state.trades_last_5m = len(trades_5m)

        state.buys_last_1m = count_type(trades_1m, "buy")
        state.buys_last_5m = count_type(trades_5m, "buy")

        state.sells_last_1m = count_type(trades_1m, "sell")
        state.sells_last_5m = count_type(trades_5m, "sell")

        state.buy_sol_last_1m = sum_type(trades_1m, "buy")
        state.buy_sol_last_5m = sum_type(trades_5m, "buy")

        state.sell_sol_last_1m = sum_type(trades_1m, "sell")
        state.sell_sol_last_5m = sum_type(trades_5m, "sell")

        state.net_sol_flow_last_1m = state.buy_sol_last_1m - state.sell_sol_last_1m
        state.net_sol_flow_last_5m = state.buy_sol_last_5m - state.sell_sol_last_5m

        participant_metrics = build_participant_metrics(
            trades_1m,
            trades_5m,
            prior_trades_5m,
            buyer_trade_counts=state.buyer_trade_counts,
            trader_trade_counts=state.trader_trade_counts,
        )
        for field_name, value in participant_metrics.items():
            setattr(state, field_name, value)

    def recompute_status(self, state: TokenState) -> None:
        old_status = state.status

        buys = state.total_buy_count
        net_flow_5m = state.net_sol_flow_last_5m
        trades_5m = state.trades_last_5m
        current_mcap = state.current_market_cap_sol
        initial_buy = state.initial_buy
        time_since_last_trade = self._seconds_since(state.last_trade_at)

        if (
            current_mcap >= 35
            and trades_5m >= 8
            and net_flow_5m > 0.5
            and state.unique_buyers_last_5m >= 3
        ):
            state.status = TokenStatus.ESCALATED.value
            state.status_reason = "strong_5m_flow_and_activity"

            if not state.escalated_at:
                state.escalated_at = utc_now_iso()
            state.was_ever_escalated = True

        elif (
            initial_buy > 0
            or current_mcap >= 28
            or buys >= 3
            or state.buy_sol_last_5m >= 0.5
            or state.unique_buyers_last_5m >= 2
        ):
            if old_status in {TokenStatus.INACTIVE.value, TokenStatus.COOLING_OFF.value}:
                state.status = TokenStatus.REAWAKENED.value
                state.status_reason = "renewed_activity_after_inactive_period"
                state.reawakened_at = utc_now_iso()
                state.revival_count += 1
            else:
                state.status = TokenStatus.WATCHING.value
                state.status_reason = "meets_watch_thresholds"

            if not state.watchlisted_at:
                state.watchlisted_at = utc_now_iso()
            state.was_ever_watchlisted = True

        elif time_since_last_trade is not None and time_since_last_trade > 3600:
            state.status = TokenStatus.INACTIVE.value
            state.status_reason = "inactive_over_1h"
            state.inactive_at = utc_now_iso()

        elif time_since_last_trade is not None and time_since_last_trade > 900:
            state.status = TokenStatus.COOLING_OFF.value
            state.status_reason = "no_recent_trade_activity"
            state.cooled_off_at = utc_now_iso()

        else:
            state.status = TokenStatus.NEW.value
            state.status_reason = "insufficient_activity"

        if old_status == TokenStatus.ESCALATED.value and state.status == TokenStatus.WATCHING.value:
            state.status_reason = "post_escalation_monitoring"

    def _seconds_since(self, iso_value: str | None) -> float | None:
        dt = iso_to_dt(iso_value)
        now = iso_to_dt(utc_now_iso())
        if not dt or not now:
            return None
        return (now - dt).total_seconds()

    def get_ranked_tokens(self) -> list[dict[str, Any]]:
        ranked = sorted(
            (state.to_dict() for state in self.states.values()),
            key=lambda s: (
                1 if s["status"] == TokenStatus.ESCALATED.value else 0,
                s.get("participant_quality_score_v2", 0.0),
                s.get("net_sol_flow_last_5m", 0.0),
                s.get("buy_sol_last_5m", 0.0),
                s.get("current_market_cap_sol", 0.0),
            ),
            reverse=True,
        )
        return ranked
