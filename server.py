#!/usr/bin/env python3
"""
NEXFX AI - خادم أسعار عام قابل للاستضافة

- لا يحتاج أي مكتبة خارجية (Python 3.9+ فقط).
- لا يحتاج أي مفتاح API ولا حساب.
- لا يتصل بأي منصة تداول ولا ينفذ أي صفقة.
- يستخدم HOST/PORT من بيئة الاستضافة؛ لا يحتوي على مفاتيح أو تنفيذ صفقات.

التشغيل:   python server.py
الفحص:     افتح http://127.0.0.1:8787/api/ticker
"""
import json
import os
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import date, timedelta, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", os.environ.get("NEXFX_PORT", "8787")))

# المواقع المسموح لها تطلب البيانات من هذا الخادم (CORS)
ALLOWED_ORIGINS = {
    "https://muhammetdabul.github.io",
    f"http://127.0.0.1:{PORT}",
    f"http://localhost:{PORT}",
}
for extra in os.environ.get("NEXFX_ORIGINS", "").split(","):
    extra = extra.strip()
    if extra.startswith("https://") or extra.startswith("http://localhost") or extra.startswith("http://127.0.0.1"):
        ALLOWED_ORIGINS.add(extra.rstrip("/"))

# حماية من هجمات DNS-rebinding: لا نقبل إلا هذه القيم في ترويسة Host
ALLOWED_HOSTS = {
    item.strip().lower() for item in os.environ.get("NEXFX_HOSTS", "").split(",") if item.strip()
}
ALLOWED_HOSTS.update({f"127.0.0.1:{PORT}", f"localhost:{PORT}"})

RATE_LIMIT = 60          # عدد الطلبات المسموح لكل عنوان
RATE_WINDOW = 60         # خلال كم ثانية
UPSTREAM_TIMEOUT = 6     # مهلة الاتصال بمصادر الأسعار
MAX_UPSTREAM_BYTES = 1_000_000

CACHE_TTL_CRYPTO = 20    # ثانية
CACHE_TTL_FOREX = 900    # ثانية (أسعار البنك المركزي الأوروبي يومية)
CACHE_TTL_METALS = 60    # ثانية (مصادر الذهب والفضة تُحدّث عادة كل 30-60 ثانية)


# ---------------------------------------------------------------- أدوات

def fetch_json(url):
    req = Request(url, headers={"User-Agent": "NEXFX-AI-local/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=UPSTREAM_TIMEOUT) as resp:
        data = resp.read(MAX_UPSTREAM_BYTES + 1)
    if len(data) > MAX_UPSTREAM_BYTES:
        raise ValueError("upstream response too large")
    return json.loads(data)


_cache = {}
_cache_lock = threading.Lock()


def cached(key, ttl, producer):
    """يرجّع القيمة المخزّنة، وإذا فشل المصدر يرجّع آخر قيمة ناجحة (مع علامة stale)."""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit["ts"] < ttl:
            return hit["value"]
    try:
        value = producer()
    except Exception as exc:  # noqa: BLE001 - نطبع السبب ونكمل
        sys.stderr.write(f"[warn] {key}: {type(exc).__name__}: {exc}\n")
        with _cache_lock:
            hit = _cache.get(key)
        if hit:
            stale = {k: (dict(v, stale=True) if v else v) for k, v in hit["value"].items()}
            return stale
        return {}
    with _cache_lock:
        _cache[key] = {"ts": now, "value": value}
    return value


# ---------------------------------------------------------------- مصادر الأسعار

def crypto_prices():
    out = {}
    # المصدر 1: بيانات Binance العامة (بدون مفتاح)
    try:
        symbols = quote('["BTCUSDT","ETHUSDT"]')
        data = fetch_json(f"https://data-api.binance.vision/api/v3/ticker/24hr?symbols={symbols}")
        by_symbol = {d["symbol"]: d for d in data}
        for name, sym in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
            d = by_symbol.get(sym)
            if d:
                out[name] = {
                    "price": float(d["lastPrice"]),
                    "change_pct": float(d["priceChangePercent"]),
                    "source": "binance-public",
                }
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[warn] binance public: {type(exc).__name__}: {exc}\n")

    # المصدر 2 (احتياطي): CoinGecko المجاني
    if len(out) < 2:
        try:
            data = fetch_json(
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true"
            )
            for name, cid in (("BTC", "bitcoin"), ("ETH", "ethereum")):
                if name not in out and cid in data:
                    out[name] = {
                        "price": float(data[cid]["usd"]),
                        "change_pct": float(data[cid]["usd_24h_change"]),
                        "source": "coingecko",
                    }
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[warn] coingecko: {type(exc).__name__}: {exc}\n")

    if not out:
        raise RuntimeError("no crypto source reachable")
    return out


def _parse_provider_time(value):
    """Parse an ISO-8601 provider timestamp and return an aware UTC datetime."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing provider timestamp")
    stamp = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(stamp)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _validated_metal(price_value, timestamp, source, stale=False):
    """Accept only positive, finite, recently observed real market prices."""
    price = float(price_value)
    if not (price > 0 and price < 10_000_000):
        raise ValueError("invalid metal price")
    as_of = _parse_provider_time(timestamp)
    age = (datetime.now(timezone.utc) - as_of).total_seconds()
    if age < -60 or age > 300:
        raise ValueError("metal price timestamp outside freshness window")
    return {
        "price": price,
        "source": source,
        "as_of": as_of.isoformat(timespec="seconds"),
        **({"stale": True} if stale else {}),
    }


def metals_prices():
    """Primary Gold-API.com per symbol; XAUS is queried only for missing symbols."""
    out = {}
    for name, symbol in (("GOLD", "XAU"), ("SILVER", "XAG")):
        try:
            data = fetch_json(f"https://api.gold-api.com/price/{symbol}")
            out[name] = _validated_metal(
                data["price"], data.get("updatedAt"), "gold-api.com"
            )
        except Exception as exc:  # noqa: BLE001 - fallback independently per symbol
            sys.stderr.write(f"[warn] gold-api {symbol}: {type(exc).__name__}: {exc}\n")

    missing = [name for name in ("GOLD", "SILVER") if name not in out]
    if missing:
        try:
            backup = fetch_json("https://xaus.com/api/v1/spot?compact=1")
            state = backup.get("data_state") or {}
            status = state.get("status", "fresh")
            backup_stale = bool(backup.get("stale")) or status == "stale"
            backup_time = backup.get("price_as_of") or backup.get("updated_at") or state.get("as_of")
            if status not in ("fresh", "stale"):
                raise ValueError("XAUS reports unavailable data")
            for name, field in (("GOLD", "spot_usd_oz"), ("SILVER", "silver_usd_oz")):
                if name in missing:
                    try:
                        out[name] = _validated_metal(
                            backup[field], backup_time, "xaus.com", stale=backup_stale
                        )
                    except Exception as exc:  # noqa: BLE001 - one missing metal must not hide the other
                        sys.stderr.write(f"[warn] xaus {name}: {type(exc).__name__}: {exc}\n")
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[warn] xaus backup: {type(exc).__name__}: {exc}\n")

    if not out:
        raise RuntimeError("no fresh gold or silver source reachable")
    return out


def forex_prices():
    end = date.today()
    start = end - timedelta(days=10)
    data = fetch_json(f"https://api.frankfurter.dev/v1/{start}..{end}?base=EUR&symbols=USD,GBP")
    rates = data["rates"]
    days = sorted(rates)
    if len(days) < 2:
        raise RuntimeError("not enough forex history")

    def pair(day):
        usd, gbp = float(rates[day]["USD"]), float(rates[day]["GBP"])
        return usd, usd / gbp  # EUR/USD ، GBP/USD

    eur_now, gbp_now = pair(days[-1])
    eur_prev, gbp_prev = pair(days[-2])
    meta = {"source": "ECB via frankfurter (daily reference)", "as_of": days[-1]}
    return {
        "EUR/USD": dict(meta, price=round(eur_now, 5), change_pct=(eur_now / eur_prev - 1) * 100),
        "GBP/USD": dict(meta, price=round(gbp_now, 5), change_pct=(gbp_now / gbp_prev - 1) * 100),
    }


TICKER_ORDER = ["GOLD", "SILVER", "BTC", "ETH", "EUR/USD", "GBP/USD"]


def build_ticker():
    items = {name: None for name in TICKER_ORDER}
    items.update(cached("crypto", CACHE_TTL_CRYPTO, crypto_prices))
    items.update(cached("forex", CACHE_TTL_FOREX, forex_prices))
    items.update(cached("metals", CACHE_TTL_METALS, metals_prices))
    return {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "items": items,
        "notes": {
            "GOLD": "سعر فوري مرجعي بالدولار لكل أونصة تروي؛ المصدر الأساسي Gold-API.com والاحتياطي XAUS",
            "SILVER": "سعر فوري مرجعي بالدولار لكل أونصة تروي؛ المصدر الأساسي Gold-API.com والاحتياطي XAUS",
            "EUR/USD": "سعر مرجعي يومي، ليس لحظياً",
            "GBP/USD": "سعر مرجعي يومي، ليس لحظياً",
        },
    }


# ---------------------------------------------------------------- الخادم

_hits = defaultdict(deque)
_hits_lock = threading.Lock()


def rate_limited(ip):
    now = time.time()
    with _hits_lock:
        q = _hits[ip]
        while q and now - q[0] > RATE_WINDOW:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            return True
        q.append(now)
        return False


class Handler(BaseHTTPRequestHandler):
    server_version = "NEXFX"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    # ---- مساعدات
    def _send(self, status, payload=None, origin=None, extra=None):
        body = b"" if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _guard(self):
        """يرجّع (ok, origin). يرد بالخطأ المناسب ويرجّع ok=False عند الرفض."""
        host = (self.headers.get("Host") or "").lower()
        if host not in ALLOWED_HOSTS:
            self._send(403, {"error": "forbidden host"})
            return False, None
        origin = self.headers.get("Origin")
        if origin is not None and origin not in ALLOWED_ORIGINS:
            self._send(403, {"error": "origin not allowed"})
            return False, None
        if rate_limited(self.client_address[0]):
            self._send(429, {"error": "too many requests"}, origin, {"Retry-After": str(RATE_WINDOW)})
            return False, None
        return True, origin

    # ---- الطرق
    def do_OPTIONS(self):
        ok, origin = self._guard()
        if not ok:
            return
        self._send(
            204,
            None,
            origin,
            {
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Private-Network": "true",
                "Access-Control-Max-Age": "600",
            },
        )

    def do_GET(self):
        ok, origin = self._guard()
        if not ok:
            return
        path = urlparse(self.path).path
        if path == "/health":
            self._send(200, {"ok": True}, origin, {"Access-Control-Allow-Private-Network": "true"})
        elif path == "/api/ticker":
            self._send(200, build_ticker(), origin, {"Access-Control-Allow-Private-Network": "true"})
        else:
            self._send(404, {"error": "not found"}, origin)


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"NEXFX AI price server listening on {HOST}:{PORT}")
    print("المواقع المسموحة:", ", ".join(sorted(ALLOWED_ORIGINS)))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nتم الإيقاف.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
