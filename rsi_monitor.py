#!/usr/bin/env python3
"""Screen USDT-M perpetual futures for overbought RSI and mail the hits.

Defaults to Gate's public futures API because Binance returns HTTP 451 for the
US IPs GitHub Actions runners use; DATA_SOURCE=binance still works behind a proxy.
Stdlib only, so CI needs no dependency install.
"""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from html import escape
from pathlib import Path


def env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


def env_float(name: str, default: float) -> float:
    try:
        return float(env(name, str(default)))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(float(env(name, str(default))))
    except ValueError:
        return default


DATA_SOURCE = env("DATA_SOURCE", "gate").lower()
SOURCES = {
    "gate": {
        "api": env("GATE_FUTURES_API_BASE", "https://api.gateio.ws/api/v4/futures/usdt"),
        "label": "Gate USDT 永续",
        "trade_url": "https://www.gate.cn/futures/trade/usdt/",
        "interval_map": {"1M": "30d"},
    },
    "binance": {
        "api": env("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com") + "/fapi/v1",
        "label": "Binance USDT 永续",
        "trade_url": "https://www.binance.com/zh-CN/futures/",
        "interval_map": {},
    },
}
if DATA_SOURCE not in SOURCES:
    raise SystemExit(f"DATA_SOURCE 只支持 {sorted(SOURCES)}，当前为 {DATA_SOURCE!r}")
SRC = SOURCES[DATA_SOURCE]
API_BASE = SRC["api"].rstrip("/")

RSI_PERIOD = env_int("RSI_PERIOD", 14)
RSI_THRESHOLD = env_float("RSI_THRESHOLD", 80.0)
RSI_INTERVALS = [i.strip() for i in env("RSI_INTERVALS", "4h,1d").split(",") if i.strip()]
MATCH_MODE = env("MATCH_MODE", "any").lower()  # any=任一周期超阈值即推送，all=所有周期都要超阈值
# 币安 K 线接口 limit<100 时权重为 1，去掉未收盘的一根仍有 98 根，足够 Wilder RSI 收敛
KLINE_LIMIT = env_int("KLINE_LIMIT", 99)
MIN_QUOTE_VOLUME = env_float("MIN_QUOTE_VOLUME", 100_000)
REQUIRE_POSITIVE_FUNDING = env("REQUIRE_POSITIVE_FUNDING", "1") not in ("0", "false", "no")
REQUIRE_ON_BINANCE = env("REQUIRE_ON_BINANCE", "1") not in ("0", "false", "no")
BINANCE_SPOT_API = env("BINANCE_SPOT_BASE_URL", "https://data-api.binance.vision/api/v3").rstrip("/")
COOLDOWN_HOURS = env_float("ALERT_COOLDOWN_HOURS", 6)
STATE_FILE = Path(env("STATE_FILE", "state/last_alerts.json"))
REQUESTS_PER_SECOND = env_float("REQUESTS_PER_SECOND", 10)
WORKERS = env_int("WORKERS", 8)
TOP_N_LOG = env_int("TOP_N_LOG", 20)

VALID_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}

_rate_lock = threading.Lock()
_next_request_at = [0.0]


def log(message: str) -> None:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


# Windows GBK 控制台遇到 ·/≥ 等字符会抛 UnicodeEncodeError，换成 ? 继续跑
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")


def http_json(url: str, params: dict | None = None) -> object:
    query = urllib.parse.urlencode(params or {})
    url = f"{url}?{query}" if query else url
    path = url.split("?")[0].rsplit("/", 2)[-1]
    last_error = ""
    for attempt in range(1, 6):
        _throttle()
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "rsi-screen/1.0"})
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:200]
            last_error = f"HTTP {exc.code} {body}"
            if exc.code in (418, 429):
                wait = int(exc.headers.get("Retry-After") or 30)
                log(f"限频 {exc.code}，等待 {wait}s")
                time.sleep(wait + 1)
            elif exc.code in (403, 451) and DATA_SOURCE == "binance":
                last_error += (
                    " | 币安对受限地区 IP 会返回 403/451，GitHub Actions 的美国 Runner 常被拦。"
                    "换 DATA_SOURCE=gate，或把 BINANCE_FUTURES_BASE_URL 指向自建反代"
                )
                break
            elif 500 <= exc.code < 600:
                time.sleep(min(2**attempt, 30))
            else:
                break
        except (OSError, json.JSONDecodeError) as exc:
            # OSError 覆盖 URLError、连接重置和 ssl.SSLError（后者不是 URLError 子类，
            # 早先漏掉它会让一条坏 TLS 记录直接崩掉整个定时任务）
            last_error = str(exc) or type(exc).__name__
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"{path} 请求失败: {last_error}")


def _throttle() -> None:
    if REQUESTS_PER_SECOND <= 0:
        return
    interval = 1.0 / REQUESTS_PER_SECOND
    with _rate_lock:
        now = time.monotonic()
        wait = _next_request_at[0] - now
        _next_request_at[0] = max(now, _next_request_at[0]) + interval
    if wait > 0:
        time.sleep(wait)


def wilder_rsi(closes: list[float], period: int) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for index in range(1, period + 1):
        delta = closes[index] - closes[index - 1]
        gains += max(delta, 0.0)
        losses += max(-delta, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period
    for index in range(period + 1, len(closes)):
        delta = closes[index] - closes[index - 1]
        avg_gain = (avg_gain * (period - 1) + max(delta, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-delta, 0.0)) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


@dataclass
class Row:
    symbol: str
    base: str
    last_price: float
    change_percent: float
    quote_volume: float
    funding_rate: float | None = None
    funding_interval: int = 28800
    rsi: dict[str, float | None] = field(default_factory=dict)

    @property
    def max_rsi(self) -> float:
        values = [v for v in self.rsi.values() if v is not None]
        return max(values) if values else 0.0

    @property
    def funding_text(self) -> str:
        if self.funding_rate is None:
            return "-"
        hours = max(1, round(self.funding_interval / 3600))
        return f"{self.funding_rate * 100:+.4f}%/{hours}h"

    def triggered(self, threshold: float, mode: str) -> bool:
        values = [v for v in self.rsi.values() if v is not None]
        if not values:
            return False
        return any(v > threshold for v in values) if mode == "any" else all(
            v > threshold for v in values
        )


def binance_spot_bases() -> set[str] | None:
    """币安现货上线的基数币种，用来剔除 Gate 独有的标的。

    币安合约接口本身被地区封锁，只能用同域的现货镜像代替：极少数"只有合约、没有现货"
    的新币会被误剔，需要时把 REQUIRE_ON_BINANCE 设为 0。
    """
    try:
        info = http_json(f"{BINANCE_SPOT_API}/exchangeInfo")
    except RuntimeError as exc:
        log(f"币安现货列表取不到，跳过“币安是否上线”过滤: {exc}")
        return None
    return {
        item["baseAsset"]
        for item in info["symbols"]
        if item.get("status") == "TRADING" and item.get("quoteAsset") == "USDT"
    }


def load_universe() -> tuple[list[str], dict[str, Row]]:
    if DATA_SOURCE == "gate":
        contracts = http_json(f"{API_BASE}/contracts")
        quotes = {item["contract"]: item for item in http_json(f"{API_BASE}/tickers")}
        rows = {}
        for item in contracts:
            name = item["name"]
            # Gate 的 USDT-M 列表里混着股票/指数/外汇永续（contract_type 非空），只留加密货币
            if not (
                item.get("status") == "trading"
                and not item.get("in_delisting")
                and not item.get("is_pre_market")
                and not item.get("contract_type")
                and name.endswith("_USDT")
                and name in quotes
            ):
                continue
            ticker = quotes[name]
            rate = item.get("funding_rate") or item.get("funding_rate_indicative")
            rows[name] = Row(
                symbol=name,
                base=name[: -len("_USDT")],
                last_price=float(ticker["last"] or 0),
                change_percent=float(ticker.get("change_percentage") or 0),
                quote_volume=float(ticker.get("volume_24h_quote") or 0),
                funding_rate=float(rate) if rate not in (None, "") else None,
                funding_interval=int(item.get("funding_interval") or 28800),
            )
    else:
        tradable = {
            item["symbol"]: item["baseAsset"]
            for item in http_json(f"{API_BASE}/exchangeInfo")["symbols"]
            if item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
        }
        funding = {
            item["symbol"]: item.get("lastFundingRate")
            for item in http_json(f"{API_BASE}/premiumIndex")
        }
        rows = {}
        for item in http_json(f"{API_BASE}/ticker/24hr"):
            symbol = item["symbol"]
            if symbol not in tradable:
                continue
            rate = funding.get(symbol)
            rows[symbol] = Row(
                symbol=symbol,
                base=tradable[symbol],
                last_price=float(item["lastPrice"] or 0),
                change_percent=float(item.get("priceChangePercent") or 0),
                quote_volume=float(item.get("quoteVolume") or 0),
                funding_rate=float(rate) if rate not in (None, "") else None,
            )
    if not rows:
        raise SystemExit(f"{SRC['label']} 未返回任何可交易合约，接口可能变更")
    return apply_filters(rows)


def apply_filters(rows: dict[str, Row]) -> tuple[list[str], dict[str, Row]]:
    kept = [row for row in rows.values() if row.quote_volume >= MIN_QUOTE_VOLUME]
    log(f"{SRC['label']} {len(rows)} 个合约 → 成交额≥{MIN_QUOTE_VOLUME:,.0f} 剩 {len(kept)} 个")
    listed = binance_spot_bases() if REQUIRE_ON_BINANCE else None
    if listed is not None:
        dropped = [row for row in kept if row.base not in listed]
        kept = [row for row in kept if row.base in listed]
        log(f"剔除币安未上线 {len(dropped)} 个: {' '.join(row.base for row in dropped[:10])}"
            f"{' …' if len(dropped) > 10 else ''}")
    if REQUIRE_POSITIVE_FUNDING:
        dropped = [row for row in kept if not (row.funding_rate or 0) > 0]
        kept = [row for row in kept if (row.funding_rate or 0) > 0]
        log(f"剔除费率≤0 {len(dropped)} 个")
    return sorted(row.symbol for row in kept), rows


def fetch_closes(symbol: str, interval: str) -> list[float]:
    """收盘价，按时间升序，最后一根尚未收盘。"""
    iv = SRC["interval_map"].get(interval, interval)
    if DATA_SOURCE == "gate":
        bars = http_json(f"{API_BASE}/candlesticks", {"contract": symbol, "interval": iv, "limit": KLINE_LIMIT})
        return [float(bar["c"]) for bar in bars]
    bars = http_json(f"{API_BASE}/klines", {"symbol": symbol, "interval": iv, "limit": KLINE_LIMIT})
    return [float(bar[4]) for bar in bars]


def fetch_rsi(symbol: str) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for interval in RSI_INTERVALS:
        try:
            closes = fetch_closes(symbol, interval)
            # 最后一根通常尚未收盘，用未收盘数据会导致 RSI 抖动和重复告警
            closed = closes[:-1] if len(closes) > RSI_PERIOD + 1 else closes
            result[interval] = wilder_rsi(closed, RSI_PERIOD)
        except (RuntimeError, KeyError, TypeError, ValueError) as exc:
            log(f"{symbol} {interval} 取数失败: {exc}")
            result[interval] = None
    return result


def screen(symbols: list[str], tickers: dict[str, Row]) -> list[Row]:
    rows: list[Row] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for symbol, rsi in pool.map(lambda s: (s, fetch_rsi(s)), symbols):
            row = tickers[symbol]
            row.rsi = rsi
            rows.append(row)
    return sorted(rows, key=lambda r: r.max_rsi, reverse=True)


def load_state() -> dict[str, str]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, str]) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=COOLDOWN_HOURS * 4)
    state = {
        key: value
        for key, value in state.items()
        if datetime.fromisoformat(value) > cutoff
    }
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")


def in_cooldown(state: dict[str, str], row: Row) -> bool:
    for interval in RSI_INTERVALS:
        value = row.rsi.get(interval)
        key = f"{row.symbol}:{interval}"
        stamp = state.get(key)
        if value is not None and value > RSI_THRESHOLD and stamp:
            if datetime.now(timezone.utc) - datetime.fromisoformat(stamp) < timedelta(hours=COOLDOWN_HOURS):
                return True
    return False


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def filter_summary() -> str:
    parts = [f"成交额≥{MIN_QUOTE_VOLUME:,.0f}"]
    if REQUIRE_POSITIVE_FUNDING:
        parts.append("费率>0")
    if REQUIRE_ON_BINANCE:
        parts.append("币安已上线")
    return " · ".join(parts)


def render_text(rows: list[Row], generated: datetime) -> str:
    lines = [
        f"{SRC['label']} RSI>{RSI_THRESHOLD:g}（{', '.join(RSI_INTERVALS)}，{generated:%Y-%m-%d %H:%M} UTC）",
        f"附加条件：{filter_summary()}",
        "",
    ]
    for row in rows:
        rsi_text = "  ".join(f"{i}={row.rsi[i]:.1f}" for i in RSI_INTERVALS if row.rsi.get(i) is not None)
        lines.append(
            f"{row.symbol}: {fmt_price(row.last_price)}  {row.change_percent:+.2f}%  {rsi_text}  "
            f"费率{row.funding_text}  成交额{row.quote_volume / 1e6:.1f}M"
        )
    lines += ["", "基于已收盘 K 线，Wilder RSI；费率为正表示多头向空头支付。仅供参考，不构成投资建议。"]
    return "\n".join(lines)


def render_html(rows: list[Row], generated: datetime) -> str:
    th = 'padding:6px 10px;background:#f2f3f5;text-align:'
    header = (
        f'<th style="{th}left">币种</th>'
        f'<th style="{th}right">最新价</th>'
        f'<th style="{th}right">24h</th>'
        + "".join(f'<th style="{th}right">RSI({escape(i)})</th>' for i in RSI_INTERVALS)
        + f'<th style="{th}right">资金费率</th>'
        + f'<th style="{th}right">24h成交额</th>'
    )
    body = []
    for row in rows:
        cells = ""
        for interval in RSI_INTERVALS:
            value = row.rsi.get(interval)
            if value is None:
                cells += '<td style="padding:6px 10px;text-align:right">-</td>'
                continue
            color = "#c0392b" if value > RSI_THRESHOLD else "#222222"
            cells += f'<td style="padding:6px 10px;text-align:right;color:{color}">{value:.1f}</td>'
        url = SRC["trade_url"] + escape(row.symbol)
        funding_color = "#b8860b" if (row.funding_rate or 0) > 0 else "#2e7d32"
        body.append(
            '<tr>'
            f'<td style="padding:6px 10px"><a href="{url}">{escape(row.symbol)}</a></td>'
            f'<td style="padding:6px 10px;text-align:right">{escape(fmt_price(row.last_price))}</td>'
            f'<td style="padding:6px 10px;text-align:right">{row.change_percent:+.2f}%</td>'
            f'{cells}'
            f'<td style="padding:6px 10px;text-align:right;color:{funding_color}">{escape(row.funding_text)}</td>'
            f'<td style="padding:6px 10px;text-align:right">{row.quote_volume / 1e6:.1f}M</td>'
            '</tr>'
        )
    note = "基于已收盘 K 线，Wilder RSI；费率为正表示多头向空头支付。仅供参考，不构成投资建议。"
    intro = (
        f"周期 {escape(', '.join(RSI_INTERVALS))} · RSI 周期 {RSI_PERIOD} · 阈值 {RSI_THRESHOLD:g} · "
        f"匹配 {escape(MATCH_MODE)} · {escape(filter_summary())} · "
        f"{generated:%Y-%m-%d %H:%M} UTC · 共 {len(rows)} 个"
    )
    return (
        "<div style='font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#222'>"
        f"<h3 style='margin:0 0 4px'>{escape(SRC['label'])} 超买监控</h3>"
        f"<p style='margin:0 0 14px;color:#666666;font-size:13px'>{intro}</p>"
        "<table cellspacing=0 cellpadding=0 style='border-collapse:collapse;font-size:13px'>"
        f"<thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"
        f"<p style='color:#999999;font-size:12px;margin-top:14px'>{note}</p></div>"
    )


def send_mail(rows: list[Row], generated: datetime) -> None:
    host = env("SMTP_HOST")
    user = env("SMTP_USER")
    password = env("SMTP_PASSWORD")
    to = [addr.strip() for addr in env("MAIL_TO", user).split(",") if addr.strip()]
    if not (host and user and password and to):
        raise SystemExit("缺少邮件配置：SMTP_HOST / SMTP_USER / SMTP_PASSWORD / MAIL_TO")
    port = env_int("SMTP_PORT", 465)

    subject = f"[RSI>{RSI_THRESHOLD:g}] {len(rows)} 个合约超买：" + ", ".join(r.symbol for r in rows[:8])
    if len(rows) > 8:
        subject += f" 等{len(rows)}个"
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = formataddr((env("SMTP_FROM_NAME", "RSI Monitor"), user))
    message["To"] = ", ".join(to)
    message.set_content(render_text(rows, generated))
    message.add_alternative(render_html(rows, generated), subtype="html")

    if port == 465:
        server = smtplib.SMTP_SSL(host, port, timeout=60)
    else:
        server = smtplib.SMTP(host, port, timeout=60)
    with server:
        if port != 465:
            server.ehlo()
            server.starttls()
            server.ehlo()
        server.login(user, password)
        server.send_message(message, from_addr=user, to_addrs=to)
    log(f"邮件已发送给 {len(to)} 个收件人")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance futures RSI screener")
    parser.add_argument("--dry-run", action="store_true", help="只打印结果，不发邮件、不写 state")
    parser.add_argument("--max-symbols", type=int, default=0, help="只扫描前 N 个，用于本地验证")
    parser.add_argument("--probe", action="store_true", help="只测各行情端点连通性，用于定位地区封锁")
    return parser.parse_args()


PROBE_TARGETS = [
    ("Binance 合约 fapi", "https://fapi.binance.com/fapi/v1/time"),
    ("Binance 合约 fapi1", "https://fapi1.binance.com/fapi/v1/time"),
    ("Binance 现货 vision", "https://data-api.binance.vision/api/v3/time"),
    ("Binance.US 合约", "https://fapi.binance.us/fapi/v1/time"),
    ("Bybit 合约", "https://api.bybit.com/v5/market/time"),
    ("Gate 合约", "https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT"),
    ("Bitget 合约", "https://api.bitget.com/api/v2/mix/market/time?productType=usdt-futures"),
    ("MEXC 合约", "https://contract.mexc.com/api/v1/contract/detail"),
    ("OKX 合约", "https://www.okx.com/api/v5/public/time"),
    ("KuCoin 合约", "https://api-futures.kucoin.com/api/v1/timestamp"),
    ("Hyperliquid", "https://api.hyperliquid.xyz/info"),
    ("CoinGecko", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"),
    # 候选源的 K 线形状，确认可达后照这个写适配器
    ("vision 4h K线", "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=2"),
    ("Gate 4h K线", "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=BTC_USDT&interval=4h&limit=2"),
    ("Bybit 4h K线", "https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=240&limit=2"),
]


def probe_url(url: str) -> tuple[str, int, str]:
    started = time.monotonic()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "rsi-screen/1.0"})
        with urllib.request.urlopen(request, timeout=12) as response:
            return str(response.status), int((time.monotonic() - started) * 1000), response.read(160).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code}", int((time.monotonic() - started) * 1000), exc.read(160).decode("utf-8", "replace")
    except Exception as exc:  # 超时/DNS/TLS 都归到这里，只为打印诊断结果
        return type(exc).__name__, int((time.monotonic() - started) * 1000), str(exc)[:120]


def probe_endpoints() -> int:
    egress = "取不到（该服务被拦）"
    for url in ("https://api.ipify.org", "https://ifconfig.me/ip"):
        status, _, body = probe_url(url)
        if status == "200" and len(body) < 40:
            egress = body.strip()
            break
    print(f"出口 IP: {egress}\n")
    print(f"{'端点':<20} {'状态':<12} {'延迟':>7}  响应")
    for name, url in PROBE_TARGETS:
        status, ms, snippet = probe_url(url)
        print(f"{name:<20} {status:<12} {ms:>6}ms  {snippet[:70].replace(chr(10), ' ')}")
    return 0


def main() -> int:
    args = parse_args()
    if args.probe:
        return probe_endpoints()
    bad = [i for i in RSI_INTERVALS if i not in VALID_INTERVALS]
    if bad:
        raise SystemExit(f"不支持的周期: {bad}，可选 {sorted(VALID_INTERVALS)}")
    if not RSI_INTERVALS:
        raise SystemExit("RSI_INTERVALS 不能为空")

    started = time.monotonic()
    symbols, tickers = load_universe()
    if args.max_symbols:
        symbols = sorted(symbols, key=lambda s: tickers[s].quote_volume, reverse=True)[: args.max_symbols]
    rows = screen(symbols, tickers)

    for row in rows[:TOP_N_LOG]:
        detail = "  ".join(f"{i}={row.rsi[i]:.1f}" for i in RSI_INTERVALS if row.rsi.get(i) is not None)
        log(f"{row.symbol:<16} {detail:<24} {row.change_percent:+7.2f}%  {row.funding_text:>14}  {row.quote_volume / 1e6:8.1f}M")

    hits = [r for r in rows if r.triggered(RSI_THRESHOLD, MATCH_MODE)]
    generated = datetime.now(timezone.utc)
    log(f"扫描 {len(rows)} 个合约，命中 {len(hits)} 个，用时 {time.monotonic() - started:.0f}s")

    if not hits:
        return 0

    state = load_state()
    fresh = [r for r in hits if not in_cooldown(state, r)]
    skipped = len(hits) - len(fresh)
    if skipped:
        log(f"{skipped} 个在 {COOLDOWN_HOURS:g}h 冷却期内，不再重复推送")
    if not fresh:
        return 0
    if args.dry_run:
        print(render_text(fresh, generated))
        return 0

    send_mail(fresh, generated)
    for row in fresh:
        for interval, value in row.rsi.items():
            if value is not None and value > RSI_THRESHOLD:
                state[f"{row.symbol}:{interval}"] = generated.isoformat()
    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
