import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EVENTS_DIR = Path("data/signals/events")
LATEST_DIR = Path("data/signals/latest")
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
LATEST_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SignalEvent:
    event_type: str
    mint: str
    created_at_utc: str
    severity: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "mint": self.mint,
            "created_at_utc": self.created_at_utc,
            "severity": self.severity,
            "details": self.details,
        }


class SignalEventManager:
    def __init__(self) -> None:
        self._last_fingerprints: dict[tuple[str, str], str] = {}

    def process_state_transition(
        self,
        previous_state: dict[str, Any] | None,
        current_state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        previous_state = previous_state or {}
        events: list[SignalEvent] = []
        mint = current_state["mint"]

        self._maybe_add_status_change_event(events, previous_state, current_state)
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="current_market_cap_sol",
            threshold=30.0,
            event_type="MCAP_CROSS_30",
            severity="medium",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="current_market_cap_sol",
            threshold=35.0,
            event_type="MCAP_CROSS_35",
            severity="high",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="buy_sol_last_5m",
            threshold=2.0,
            event_type="BUY_FLOW_5M_CROSS_2",
            severity="high",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="net_sol_flow_last_5m",
            threshold=1.0,
            event_type="NET_FLOW_5M_CROSS_1",
            severity="high",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="trades_last_1m",
            threshold=5.0,
            event_type="TX_ACCELERATION_1M",
            severity="medium",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="unique_buyers_last_5m",
            threshold=3.0,
            event_type="UNIQUE_BUYERS_CROSS_3",
            severity="medium",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="participant_quality_score_v2",
            threshold=0.65,
            event_type="PARTICIPANT_QUALITY_HIGH",
            severity="high",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="wallet_novelty_score",
            threshold=0.50,
            event_type="WALLET_NOVELTY_SURGE",
            severity="medium",
        )
        self._maybe_add_threshold_cross_event(
            events,
            previous_state,
            current_state,
            metric="repeat_wallet_ratio",
            threshold=0.55,
            event_type="REPEAT_WALLET_CHURN",
            severity="high",
        )
        self._maybe_add_threshold_drop_event(
            events,
            previous_state,
            current_state,
            metric="participant_quality_score_v2",
            threshold=0.35,
            event_type="PARTICIPANT_QUALITY_BREAKDOWN",
            severity="high",
        )
        self._maybe_add_peak_breakout_event(events, previous_state, current_state)

        event_dicts = [event.to_dict() for event in events]
        if event_dicts:
            self._persist_events(mint, current_state, event_dicts)
        return event_dicts

    def _maybe_add_status_change_event(
        self,
        events: list[SignalEvent],
        previous_state: dict[str, Any],
        current_state: dict[str, Any],
    ) -> None:
        prev_status = previous_state.get("status")
        curr_status = current_state.get("status")
        if prev_status and curr_status and prev_status != curr_status:
            events.append(
                SignalEvent(
                    event_type=f"STATUS_{prev_status.upper()}_TO_{curr_status.upper()}",
                    mint=current_state["mint"],
                    created_at_utc=utc_now_iso(),
                    severity="high" if curr_status == "escalated" else "medium",
                    details={
                        "from_status": prev_status,
                        "to_status": curr_status,
                        "status_reason": current_state.get("status_reason"),
                    },
                )
            )

    def _maybe_add_threshold_cross_event(
        self,
        events: list[SignalEvent],
        previous_state: dict[str, Any],
        current_state: dict[str, Any],
        *,
        metric: str,
        threshold: float,
        event_type: str,
        severity: str,
    ) -> None:
        previous_value = float(previous_state.get(metric, 0.0) or 0.0)
        current_value = float(current_state.get(metric, 0.0) or 0.0)
        if previous_value < threshold <= current_value:
            events.append(
                SignalEvent(
                    event_type=event_type,
                    mint=current_state["mint"],
                    created_at_utc=utc_now_iso(),
                    severity=severity,
                    details={
                        "metric": metric,
                        "threshold": threshold,
                        "previous_value": round(previous_value, 4),
                        "current_value": round(current_value, 4),
                    },
                )
            )

    def _maybe_add_threshold_drop_event(
        self,
        events: list[SignalEvent],
        previous_state: dict[str, Any],
        current_state: dict[str, Any],
        *,
        metric: str,
        threshold: float,
        event_type: str,
        severity: str,
    ) -> None:
        previous_value = float(previous_state.get(metric, 0.0) or 0.0)
        current_value = float(current_state.get(metric, 0.0) or 0.0)
        if previous_value > threshold >= current_value:
            events.append(
                SignalEvent(
                    event_type=event_type,
                    mint=current_state["mint"],
                    created_at_utc=utc_now_iso(),
                    severity=severity,
                    details={
                        "metric": metric,
                        "threshold": threshold,
                        "previous_value": round(previous_value, 4),
                        "current_value": round(current_value, 4),
                    },
                )
            )

    def _maybe_add_peak_breakout_event(
        self,
        events: list[SignalEvent],
        previous_state: dict[str, Any],
        current_state: dict[str, Any],
    ) -> None:
        prev_peak = float(previous_state.get("peak_market_cap_sol", 0.0) or 0.0)
        current_peak = float(current_state.get("peak_market_cap_sol", 0.0) or 0.0)
        current_mcap = float(current_state.get("current_market_cap_sol", 0.0) or 0.0)

        if prev_peak > 0 and current_peak > prev_peak and current_mcap >= prev_peak * 1.05:
            events.append(
                SignalEvent(
                    event_type="PEAK_BREAKOUT",
                    mint=current_state["mint"],
                    created_at_utc=utc_now_iso(),
                    severity="high",
                    details={
                        "previous_peak_market_cap_sol": round(prev_peak, 4),
                        "new_peak_market_cap_sol": round(current_peak, 4),
                        "current_market_cap_sol": round(current_mcap, 4),
                    },
                )
            )

    def _persist_events(
        self,
        mint: str,
        current_state: dict[str, Any],
        events: list[dict[str, Any]],
    ) -> None:
        latest_path = LATEST_DIR / f"{mint}.json"
        latest_payload = {
            "mint": mint,
            "captured_at_utc": utc_now_iso(),
            "status": current_state.get("status"),
            "score_inputs": {
                "current_market_cap_sol": current_state.get("current_market_cap_sol"),
                "peak_market_cap_sol": current_state.get("peak_market_cap_sol"),
                "trades_last_1m": current_state.get("trades_last_1m"),
                "trades_last_5m": current_state.get("trades_last_5m"),
                "buy_sol_last_5m": current_state.get("buy_sol_last_5m"),
                "net_sol_flow_last_5m": current_state.get("net_sol_flow_last_5m"),
                "unique_buyers_last_5m": current_state.get("unique_buyers_last_5m"),
                "unique_traders_last_5m": current_state.get("unique_traders_last_5m"),
                "wallet_novelty_score": current_state.get("wallet_novelty_score"),
                "repeat_wallet_ratio": current_state.get("repeat_wallet_ratio"),
                "buyer_overlap_ratio": current_state.get("buyer_overlap_ratio"),
                "participant_quality_score_v2": current_state.get("participant_quality_score_v2"),
            },
            "events": events,
        }
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_payload, f, indent=2, ensure_ascii=False)

        for event in events:
            fingerprint = json.dumps(event, sort_keys=True)
            key = (mint, event["event_type"])
            if self._last_fingerprints.get(key) == fingerprint:
                continue
            self._last_fingerprints[key] = fingerprint

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            event_path = EVENTS_DIR / f"{mint}_{event['event_type']}_{timestamp}.json"
            with open(event_path, "w", encoding="utf-8") as f:
                json.dump(event, f, indent=2, ensure_ascii=False)
