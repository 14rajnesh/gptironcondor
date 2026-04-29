from __future__ import annotations

import json
import requests
from datetime import datetime
from typing import Optional

from fyers_apiv3 import fyersModel

from config import CFG
from models import OptionRow, MarketSnapshot
from utils import IST, now_ist, safe_float, safe_int


class MarketDataError(RuntimeError):
    pass


class AngelGreeksProvider:
    """Optional Angel One Greeks provider.

    Requires ANGEL_API_KEY and ANGEL_JWT_TOKEN. If unavailable or the API fails,
    the bot falls back to Black-Scholes Greeks.
    """

    ENDPOINT = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/marketData/v1/optionGreek"

    def __init__(self):
        self.enabled = bool(CFG.ANGEL_API_KEY and CFG.ANGEL_JWT_TOKEN)

    def fetch_greeks(self, expiry_date: str) -> dict[tuple[float, str], dict]:
        if not self.enabled:
            return {}

        headers = {
            "Authorization": f"Bearer {CFG.ANGEL_JWT_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": CFG.ANGEL_CLIENT_LOCAL_IP,
            "X-ClientPublicIP": CFG.ANGEL_CLIENT_PUBLIC_IP,
            "X-MACAddress": CFG.ANGEL_MAC_ADDRESS,
            "X-PrivateKey": CFG.ANGEL_API_KEY,
        }
        body = {"name": "NIFTY", "expirydate": expiry_date.upper()}

        try:
            r = requests.post(self.ENDPOINT, headers=headers, json=body, timeout=10)
            payload = r.json()
        except Exception:
            return {}

        if not payload.get("status") or not payload.get("data"):
            return {}

        result: dict[tuple[float, str], dict] = {}
        for item in payload.get("data", []):
            strike = safe_float(item.get("strikePrice"), 0)
            opt_type = str(item.get("optionType", "")).upper()
            if strike <= 0 or opt_type not in {"CE", "PE"}:
                continue
            result[(strike, opt_type)] = {
                "delta": safe_float(item.get("delta"), 0),
                "gamma": safe_float(item.get("gamma"), 0),
                "theta": safe_float(item.get("theta"), 0),
                "vega": safe_float(item.get("vega"), 0),
                "iv": safe_float(item.get("impliedVolatility"), 0),
                "volume": safe_int(item.get("tradeVolume"), 0),
            }
        return result


class FyersProvider:
    def __init__(self):
        if not CFG.FYERS_CLIENT_ID or not CFG.FYERS_ACCESS_TOKEN:
            raise MarketDataError("FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN missing")
        self.fyers = fyersModel.FyersModel(
            client_id=CFG.FYERS_CLIENT_ID,
            token=CFG.FYERS_ACCESS_TOKEN,
            is_async=False,
            log_path="",
        )

    def get_spot_vix(self) -> tuple[float, float]:
        symbols = "NSE:NIFTY50-INDEX,NSE:INDIAVIX-INDEX"
        resp = self.fyers.quotes({"symbols": symbols})
        if resp.get("s") != "ok":
            raise MarketDataError(f"Fyers quote error: {resp}")

        d = resp.get("d", [])
        if len(d) < 1:
            raise MarketDataError(f"Empty Fyers quote response: {resp}")

        def read_price(i: int) -> float:
            v = d[i].get("v", {}) if len(d) > i else {}
            price = safe_float(v.get("lp"), 0)
            if price <= 0:
                price = safe_float(v.get("ltp"), 0)
            if price <= 0:
                price = safe_float(v.get("prev_close_price"), 0)
            return price

        spot = read_price(0)
        vix = read_price(1) if len(d) > 1 else 0.0
        if spot <= 0:
            raise MarketDataError("NIFTY spot is zero or missing")
        return spot, vix

    def fetch_option_chain(self) -> tuple[list[dict], list[dict]]:
        data = {"symbol": "NSE:NIFTY50-INDEX", "strikecount": CFG.STRIKECOUNT, "timestamp": ""}
        resp = self.fyers.optionchain(data=data)
        if resp.get("s") != "ok":
            raise MarketDataError(f"Fyers optionchain error: {json.dumps(resp, indent=2)[:1500]}")

        payload = resp.get("data", {}) or {}
        chain = payload.get("optionsChain", []) or []
        expiry_data = payload.get("expiryData", []) or []
        if not chain:
            raise MarketDataError("Fyers optionchain returned empty optionsChain")
        return chain, expiry_data

    def batch_quotes(self, symbols: list[str], batch_size: int = 50) -> dict[str, dict]:
        out = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            resp = self.fyers.quotes({"symbols": ",".join(batch)})
            if resp.get("s") != "ok":
                continue
            for item in resp.get("d", []):
                symbol = item.get("n") or item.get("symbol")
                v = item.get("v", {}) or {}
                if symbol:
                    out[symbol] = v
        return out

    @staticmethod
    def _expiry_from_expiry_data(expiry_data: list[dict]) -> tuple[str, str]:
        if not expiry_data:
            return "", ""
        first = expiry_data[0] or {}
        expiry_timestamp = str(first.get("expiry") or first.get("timestamp") or first.get("expiryTimestamp") or "")
        expiry_text = str(first.get("date") or first.get("expiryDate") or first.get("expiry_date") or "")
        if not expiry_text:
            expiry_text = str(first)
        return expiry_text, expiry_timestamp

    @staticmethod
    def _parse_expiry_date(expiry_text: str) -> Optional[datetime]:
        for fmt in ["%d-%m-%Y", "%d %b %Y", "%d-%b-%Y", "%Y-%m-%d", "%d %B %Y"]:
            try:
                return datetime.strptime(expiry_text, fmt).replace(tzinfo=IST)
            except Exception:
                continue
        return None

    def normalize_chain(self, chain: list[dict], expiry_text: str, expiry_timestamp: str) -> list[OptionRow]:
        rows: list[OptionRow] = []
        for item in chain:
            symbol = str(item.get("symbol") or "")
            opt_type = str(item.get("option_type") or item.get("optionType") or item.get("type") or "").upper()
            strike = safe_float(item.get("strike_price") or item.get("strikePrice") or item.get("strike"), 0)
            if not symbol.startswith("NSE:") or opt_type not in {"CE", "PE"} or strike <= 0:
                continue
            rows.append(OptionRow(
                symbol=symbol,
                strike=strike,
                option_type=opt_type,
                expiry=expiry_text,
                expiry_timestamp=expiry_timestamp,
                bid=safe_float(item.get("bid") or item.get("bid_price") or item.get("best_bid_price"), 0),
                ask=safe_float(item.get("ask") or item.get("ask_price") or item.get("best_ask_price"), 0),
                ltp=safe_float(item.get("ltp") or item.get("lp") or item.get("last_price"), 0),
                volume=safe_int(item.get("volume") or item.get("vol_traded_today") or item.get("total_traded_volume"), 0),
                oi=safe_int(item.get("oi") or item.get("open_interest"), 0),
                iv=safe_float(item.get("iv") or item.get("impliedVolatility"), 0),
                delta=safe_float(item.get("delta"), None) if item.get("delta") is not None else None,
                gamma=safe_float(item.get("gamma"), None) if item.get("gamma") is not None else None,
                theta=safe_float(item.get("theta"), None) if item.get("theta") is not None else None,
                vega=safe_float(item.get("vega"), None) if item.get("vega") is not None else None,
            ))
        return rows

    def enrich_quotes(self, rows: list[OptionRow]) -> list[OptionRow]:
        symbols = [r.symbol for r in rows if r.symbol]
        quotes = self.batch_quotes(symbols)
        for row in rows:
            q = quotes.get(row.symbol, {}) or {}
            if not q:
                continue
            row.bid = safe_float(q.get("bid") or q.get("bid_price") or q.get("best_bid_price"), row.bid)
            row.ask = safe_float(q.get("ask") or q.get("ask_price") or q.get("best_ask_price"), row.ask)
            row.ltp = safe_float(q.get("lp") or q.get("ltp") or q.get("last_price"), row.ltp)
            row.volume = safe_int(q.get("volume") or q.get("vol_traded_today") or q.get("total_traded_volume"), row.volume)
            row.oi = safe_int(q.get("oi") or q.get("open_interest"), row.oi)
        return rows

    def get_market_snapshot(self) -> MarketSnapshot:
        spot, vix = self.get_spot_vix()
        chain, expiry_data = self.fetch_option_chain()
        expiry_text, expiry_timestamp = self._expiry_from_expiry_data(expiry_data)
        rows = self.normalize_chain(chain, expiry_text, expiry_timestamp)
        rows = self.enrich_quotes(rows)

        expiry_dt = self._parse_expiry_date(expiry_text)
        if expiry_dt:
            dte = max(0, (expiry_dt.date() - now_ist().date()).days)
        else:
            dte = 0

        return MarketSnapshot(
            spot=spot,
            vix=vix,
            expiry=expiry_text,
            expiry_timestamp=expiry_timestamp,
            dte=dte,
            chain=rows,
        )
