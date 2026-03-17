
from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
for candidate in (PROJECT_ROOT, PROJECT_ROOT.parent):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


DEFAULT_COHORT_DIR = Path("data/enrichment/token_cohorts")
DEFAULT_CONTROL_DIR = Path("data/enrichment/token_control")
DEFAULT_OPEN_TRADES = Path("data/paper/open_trades.json")
DEFAULT_CLOSED_TRADES = Path("data/paper/closed_trades.json")
DEFAULT_ARCHIVE_DIR = Path("data/archive/dead_tokens")
DEFAULT_REPORT_PATH = Path("data/analysis/dead_token_retention_report.json")


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


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            value = float(value)
        except Exception:
            return None
        if value <= 0:
            return None
        return value

    text = str(value).strip()
    if not text:
        return None

    # Numeric strings
    try:
        num = float(text)
        if num > 0:
            return num
    except Exception:
        pass

    # ISO strings
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _max_ts(*values: Any) -> float | None:
    parsed = [_parse_ts(v) for v in values]
    parsed = [v for v in parsed if v is not None]
    return max(parsed) if parsed else None


def _mint_from_path(path: Path) -> str:
    return path.stem


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def _payload_last_activity_ts(payload: dict[str, Any], file_path: Path) -> float:
    creator_profile = payload.get("helius_creator_profile", {}) or {}
    timestamps = [
        payload.get("last_activity_at"),
        payload.get("last_seen_at"),
        payload.get("updated_at"),
        payload.get("updated_at_utc"),
        payload.get("last_updated_at"),
        payload.get("last_trade_at"),
        payload.get("latest_trade_at"),
        payload.get("latest_event_at"),
        payload.get("snapshot_at"),
        payload.get("snapshot_time"),
        creator_profile.get("updated_at"),
    ]
    candidate = _max_ts(*timestamps)
    if candidate is not None:
        return candidate
    return file_path.stat().st_mtime


def _payload_trade_activity(payload: dict[str, Any]) -> dict[str, float]:
    return {
        "trades_1m": _safe_int(payload.get("trades_last_1m")),
        "trades_5m": _safe_int(payload.get("trades_last_5m")),
        "buys_5m": _safe_int(payload.get("buys_last_5m")),
        "unique_buyers_5m": _safe_int(
            payload.get("unique_buyers_last_5m", payload.get("unique_buyers"))
        ),
        "current_mcap": _safe_float(
            payload.get("current_market_cap_sol", payload.get("market_cap_sol"))
        ),
    }


def _recent_trade_mints(
    open_trades: dict[str, Any] | list[Any],
    closed_trades: list[dict[str, Any]] | dict[str, Any],
    *,
    closed_trade_keep_hours: float,
) -> tuple[set[str], dict[str, float]]:
    active_mints: set[str] = set()
    recent_closed_ts: dict[str, float] = {}
    now_ts = _now_ts()

    if isinstance(open_trades, dict):
        for mint in open_trades.keys():
            mint = str(mint or "").strip()
            if mint:
                active_mints.add(mint)
    elif isinstance(open_trades, list):
        for row in open_trades:
            if isinstance(row, dict):
                mint = str(row.get("mint") or "").strip()
                if mint:
                    active_mints.add(mint)

    closed_iterable: list[dict[str, Any]] = []
    if isinstance(closed_trades, list):
        closed_iterable = [row for row in closed_trades if isinstance(row, dict)]
    elif isinstance(closed_trades, dict):
        closed_iterable = [row for row in closed_trades.values() if isinstance(row, dict)]

    cutoff = now_ts - closed_trade_keep_hours * 3600.0
    for row in closed_iterable:
        mint = str(row.get("mint") or "").strip()
        if not mint:
            continue
        ts = _max_ts(
            row.get("closed_at_utc"),
            row.get("updated_at_utc"),
            row.get("opened_at_utc"),
        )
        if ts is None:
            continue
        if ts >= cutoff:
            active_mints.add(mint)
            previous = recent_closed_ts.get(mint, 0.0)
            if ts > previous:
                recent_closed_ts[mint] = ts

    return active_mints, recent_closed_ts


@dataclass
class CandidateDecision:
    mint: str
    cohort_path: str
    control_path: str | None
    action: str
    reason: str
    age_hours: float
    current_mcap: float
    trades_1m: int
    trades_5m: int
    buys_5m: int
    unique_buyers_5m: int
    last_activity_at: float
    has_open_trade: bool
    recently_closed: bool
    file_size_bytes: int


def evaluate_candidate(
    cohort_path: Path,
    *,
    control_path: Path | None,
    active_mints: set[str],
    recent_closed_ts: dict[str, float],
    inactive_hours: float,
    recent_trade_grace_hours: float,
    min_dead_mcap_sol: float,
    max_recent_trades_5m: int,
    max_recent_buys_5m: int,
    min_file_age_hours: float,
) -> CandidateDecision:
    payload = _load_json(cohort_path, {})
    mint = str(payload.get("mint") or cohort_path.stem)
    last_activity_at = _payload_last_activity_ts(payload, cohort_path)
    age_hours = max(0.0, (_now_ts() - last_activity_at) / 3600.0)
    activity = _payload_trade_activity(payload)

    has_open_trade = mint in active_mints and mint not in recent_closed_ts
    recently_closed = mint in recent_closed_ts and (
        (_now_ts() - recent_closed_ts[mint]) / 3600.0 <= recent_trade_grace_hours
    )

    if mint in active_mints:
        return CandidateDecision(
            mint=mint,
            cohort_path=str(cohort_path),
            control_path=str(control_path) if control_path else None,
            action="keep",
            reason="active_or_recent_trade",
            age_hours=round(age_hours, 4),
            current_mcap=activity["current_mcap"],
            trades_1m=activity["trades_1m"],
            trades_5m=activity["trades_5m"],
            buys_5m=activity["buys_5m"],
            unique_buyers_5m=activity["unique_buyers_5m"],
            last_activity_at=last_activity_at,
            has_open_trade=has_open_trade,
            recently_closed=recently_closed,
            file_size_bytes=cohort_path.stat().st_size,
        )

    if age_hours < min_file_age_hours:
        reason = "too_fresh"
        action = "keep"
    elif age_hours < inactive_hours:
        reason = "inside_inactive_ttl"
        action = "keep"
    elif activity["current_mcap"] > min_dead_mcap_sol:
        reason = "mcap_above_dead_threshold"
        action = "keep"
    elif activity["trades_5m"] > max_recent_trades_5m:
        reason = "recent_trade_velocity"
        action = "keep"
    elif activity["buys_5m"] > max_recent_buys_5m:
        reason = "recent_buy_pressure"
        action = "keep"
    else:
        reason = "inactive_and_dead"
        action = "archive"

    return CandidateDecision(
        mint=mint,
        cohort_path=str(cohort_path),
        control_path=str(control_path) if control_path else None,
        action=action,
        reason=reason,
        age_hours=round(age_hours, 4),
        current_mcap=activity["current_mcap"],
        trades_1m=activity["trades_1m"],
        trades_5m=activity["trades_5m"],
        buys_5m=activity["buys_5m"],
        unique_buyers_5m=activity["unique_buyers_5m"],
        last_activity_at=last_activity_at,
        has_open_trade=has_open_trade,
        recently_closed=recently_closed,
        file_size_bytes=cohort_path.stat().st_size,
    )


def _archive_file(src: Path, dst_root: Path) -> str:
    dst_root.mkdir(parents=True, exist_ok=True)
    dst = dst_root / src.name
    if dst.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dst = dst_root / f"{src.stem}_{stamp}{src.suffix}"
    shutil.move(str(src), str(dst))
    return str(dst)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Archive or delete dead token cohort/control payloads."
    )
    parser.add_argument("--cohort-dir", type=Path, default=DEFAULT_COHORT_DIR)
    parser.add_argument("--control-dir", type=Path, default=DEFAULT_CONTROL_DIR)
    parser.add_argument("--open-trades", type=Path, default=DEFAULT_OPEN_TRADES)
    parser.add_argument("--closed-trades", type=Path, default=DEFAULT_CLOSED_TRADES)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--inactive-hours", type=float, default=6.0)
    parser.add_argument("--min-file-age-hours", type=float, default=0.75)
    parser.add_argument("--closed-trade-keep-hours", type=float, default=24.0)
    parser.add_argument("--recent-trade-grace-hours", type=float, default=24.0)
    parser.add_argument("--min-dead-mcap-sol", type=float, default=18.0)
    parser.add_argument("--max-recent-trades-5m", type=int, default=1)
    parser.add_argument("--max-recent-buys-5m", type=int, default=1)
    parser.add_argument(
        "--mode",
        choices=("dry-run", "archive", "delete"),
        default="dry-run",
        help="dry-run is default and safest.",
    )
    args = parser.parse_args()

    open_trades = _load_json(args.open_trades, {})
    closed_trades = _load_json(args.closed_trades, [])
    active_mints, recent_closed_ts = _recent_trade_mints(
        open_trades,
        closed_trades,
        closed_trade_keep_hours=args.closed_trade_keep_hours,
    )

    cohort_dir = args.cohort_dir
    control_dir = args.control_dir
    cohort_files = sorted(cohort_dir.glob("*.json")) if cohort_dir.exists() else []

    decisions: list[CandidateDecision] = []
    archived: list[dict[str, Any]] = []
    deleted: list[str] = []

    for cohort_path in cohort_files:
        mint = _mint_from_path(cohort_path)
        control_path = (control_dir / cohort_path.name) if control_dir.exists() else None
        if control_path is not None and not control_path.exists():
            control_path = None

        decision = evaluate_candidate(
            cohort_path,
            control_path=control_path,
            active_mints=active_mints,
            recent_closed_ts=recent_closed_ts,
            inactive_hours=args.inactive_hours,
            recent_trade_grace_hours=args.recent_trade_grace_hours,
            min_dead_mcap_sol=args.min_dead_mcap_sol,
            max_recent_trades_5m=args.max_recent_trades_5m,
            max_recent_buys_5m=args.max_recent_buys_5m,
            min_file_age_hours=args.min_file_age_hours,
        )
        decisions.append(decision)

        if decision.action != "archive":
            continue

        if args.mode == "archive":
            archived_paths = {
                "mint": mint,
                "cohort_archived_to": _archive_file(cohort_path, args.archive_dir / "token_cohorts"),
                "control_archived_to": None,
            }
            if control_path is not None and control_path.exists():
                archived_paths["control_archived_to"] = _archive_file(
                    control_path,
                    args.archive_dir / "token_control",
                )
            archived.append(archived_paths)
        elif args.mode == "delete":
            cohort_path.unlink(missing_ok=True)
            if control_path is not None and control_path.exists():
                control_path.unlink(missing_ok=True)
            deleted.append(mint)

    summary = {
        "mode": args.mode,
        "inputs": {
            "cohort_dir": str(args.cohort_dir),
            "control_dir": str(args.control_dir),
            "open_trades": str(args.open_trades),
            "closed_trades": str(args.closed_trades),
            "inactive_hours": args.inactive_hours,
            "min_file_age_hours": args.min_file_age_hours,
            "closed_trade_keep_hours": args.closed_trade_keep_hours,
            "recent_trade_grace_hours": args.recent_trade_grace_hours,
            "min_dead_mcap_sol": args.min_dead_mcap_sol,
            "max_recent_trades_5m": args.max_recent_trades_5m,
            "max_recent_buys_5m": args.max_recent_buys_5m,
            "cohort_file_count": len(cohort_files),
            "active_or_recent_trade_mint_count": len(active_mints),
        },
        "summary": {
            "keep_count": sum(1 for d in decisions if d.action == "keep"),
            "archive_candidate_count": sum(1 for d in decisions if d.action == "archive"),
            "archived_count": len(archived),
            "deleted_count": len(deleted),
            "bytes_in_archive_candidates": sum(
                d.file_size_bytes for d in decisions if d.action == "archive"
            ),
        },
        "by_reason": {},
        "top_archive_candidates": [
            asdict(d)
            for d in sorted(
                [d for d in decisions if d.action == "archive"],
                key=lambda row: (row.age_hours, -row.file_size_bytes),
                reverse=True,
            )[:25]
        ],
        "archived": archived,
        "deleted": deleted,
    }

    reason_counts: dict[str, int] = {}
    for d in decisions:
        reason_counts[d.reason] = reason_counts.get(d.reason, 0) + 1
    summary["by_reason"] = reason_counts

    _save_json(args.report_out, summary)

    print("\n=== dead_token_retention ===")
    print(json.dumps(summary, indent=2))
    print(f"\nreport_saved={args.report_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
