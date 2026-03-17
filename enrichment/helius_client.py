from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from typing import Any


class HeliusClient:
    """Minimal Helius client for Phase 2 wallet enrichment.

    Works in a fail-soft way:
    - if no API key is present, `enabled` is False and methods return empty payloads
    - request errors return defaults instead of raising
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        rpc_base_url: str | None = None,
        rpc_fallback_url: str | None = None,
        api_base_url: str | None = None,
        timeout_seconds: float = 3.5,
    ) -> None:
        self.api_key = api_key or os.getenv("HELIUS_API_KEY")
        self.rpc_base_url = (
            rpc_base_url
            or os.getenv("HELIUS_RPC_BASE_URL")
            or "https://mainnet.helius-rpc.com"
        ).rstrip("/")
        self.rpc_fallback_url = (
            rpc_fallback_url
            or os.getenv("HELIUS_RPC_FALLBACK_URL")
            or "https://mainnet.helius-rpc.com"
        ).rstrip("/")
        self.api_base_url = (
            api_base_url
            or os.getenv("HELIUS_API_BASE_URL")
            or "https://api-mainnet.helius-rpc.com"
        ).rstrip("/")
        self.timeout_seconds = float(
            os.getenv("HELIUS_TIMEOUT_SECONDS", str(timeout_seconds))
            or timeout_seconds
        )
        self.last_error: str | None = None
        self.last_rpc_url_used: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _rpc_url(self, base_url: str | None = None) -> str:
        chosen = (base_url or self.rpc_base_url).rstrip("/")
        return f"{chosen}/?api-key={urllib.parse.quote(self.api_key or '')}"

    def _api_url(self, path: str, query: dict[str, Any] | None = None) -> str:
        query = dict(query or {})
        query["api-key"] = self.api_key or ""
        return f"{self.api_base_url}{path}?{urllib.parse.urlencode(query, doseq=True)}"

    def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        if not self.enabled:
            return None
        self.last_error = None
        try:
            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "clawsearch/1.0",
                },
                method="POST",
            )
            effective_timeout = float(timeout_seconds or self.timeout_seconds)
            with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            return None

    def _get_json(
        self,
        url: str,
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        if not self.enabled:
            return None
        self.last_error = None
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "clawsearch/1.0",
                },
                method="GET",
            )
            effective_timeout = float(timeout_seconds or self.timeout_seconds)
            with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            return None

    def rpc_call(
        self,
        method: str,
        params: list[Any] | dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        if not self.enabled:
            return None
        payload = {
            "jsonrpc": "2.0",
            "id": "clawsearch",
            "method": method,
            "params": params or [],
        }
        urls = [self.rpc_base_url]
        if self.rpc_fallback_url and self.rpc_fallback_url not in urls:
            urls.append(self.rpc_fallback_url)

        for base in urls:
            response = self._post_json(
                self._rpc_url(base),
                payload,
                timeout_seconds=timeout_seconds,
            )
            self.last_rpc_url_used = base
            if not isinstance(response, dict):
                continue
            if response.get("error"):
                self.last_error = json.dumps(
                    response.get("error"),
                    ensure_ascii=False,
                )
                continue
            return response.get("result")
        return None

    def get_signatures_for_address(
        self,
        address: str,
        *,
        limit: int = 25,
        before: str | None = None,
        until: str | None = None,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": max(1, min(limit, 1000))}
        if before:
            params["before"] = before
        if until:
            params["until"] = until
        result = self.rpc_call(
            "getSignaturesForAddress",
            [address, params],
            timeout_seconds=timeout_seconds,
        )
        return result if isinstance(result, list) else []

    def get_transactions_for_address(
        self,
        address: str,
        *,
        limit: int = 25,
        sort_order: str = "desc",
        transaction_details: str = "signatures",
        pagination_token: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "transactionDetails": transaction_details,
            "limit": max(
                1,
                min(limit, 100 if transaction_details == "full" else 1000),
            ),
            "sortOrder": "asc" if str(sort_order).lower() == "asc" else "desc",
            "commitment": "finalized",
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        result = self.rpc_call(
            "getTransactionsForAddress",
            [address, params],
            timeout_seconds=timeout_seconds,
        )
        return result if isinstance(result, dict) else {
            "data": [],
            "paginationToken": None,
        }

    def get_enhanced_transactions_by_address(
        self,
        address: str,
        *,
        limit: int = 25,
        before_signature: str | None = None,
        after_signature: str | None = None,
        tx_type: str | None = None,
        source: str | None = None,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "limit": max(1, min(limit, 100)),
            "commitment": "finalized",
        }
        if before_signature:
            query["before-signature"] = before_signature
        if after_signature:
            query["after-signature"] = after_signature
        if tx_type:
            query["type"] = tx_type
        if source:
            query["source"] = source
        data = self._get_json(
            self._api_url(f"/v0/addresses/{address}/transactions", query),
            timeout_seconds=timeout_seconds,
        )
        return data if isinstance(data, list) else []

    def parse_transactions(
        self,
        signatures: list[str],
        *,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        signatures = [sig for sig in signatures if sig][:100]
        if not signatures:
            return []
        data = self._post_json(
            self._api_url("/v0/transactions"),
            {"transactions": signatures, "commitment": "finalized"},
            timeout_seconds=timeout_seconds,
        )
        return data if isinstance(data, list) else []

    def get_asset(
        self,
        asset_id: str,
        *,
        show_fungible: bool = True,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"id": asset_id}
        if show_fungible:
            params["displayOptions"] = {"showFungible": True}
        result = self.rpc_call("getAsset", params, timeout_seconds=timeout_seconds)
        return result if isinstance(result, dict) else {}

    def smoke_test(self, address: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "enabled": self.enabled,
            "rpc_base_url": self.rpc_base_url,
            "rpc_fallback_url": self.rpc_fallback_url,
            "api_base_url": self.api_base_url,
        }
        if not self.enabled:
            payload["status"] = "disabled"
            return payload

        slot = self.rpc_call("getSlot", [{"commitment": "finalized"}])
        payload["slot_ok"] = isinstance(slot, int)
        payload["slot"] = slot if isinstance(slot, int) else None
        payload["last_error"] = self.last_error

        if address:
            signatures = self.get_signatures_for_address(address, limit=1)
            payload["address"] = address
            payload["signatures_ok"] = isinstance(signatures, list)
            payload["signature_count"] = len(signatures)
            payload["enhanced_txs_count"] = len(
                self.get_enhanced_transactions_by_address(address, limit=3)
            )
            payload["last_error"] = self.last_error

        payload["status"] = "ok" if payload.get("slot_ok") else "failed"
        payload["last_rpc_url_used"] = self.last_rpc_url_used
        return payload