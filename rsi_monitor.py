#!/usr/bin/env python3
"""Screen Binance USDT-M perpetual futures for overbought RSI and mail the hits.

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


BASE_URL = env("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
RSI_PERIOD = env_int("RSI_PERIOD", 14)
RSI_THRESHOLD = env_float("RSI_THRESHOLD", 80.0)
RSI_INTERVALS = [i.strip() for i in env("RSI_INTERVALS", "4h,1d").split(",") if i.strip()]
MATCH_MODE = env("MATCH_MODE", "any").lower()  # any=任一周期超阈值即推送，all=所有周期都要超阈值
# 币安 K 线接口 limit<100 时权重为 1，去掉未收盘的一根仍有 98 根，足够 Wilder RSI 收敛
KLINE_LIMIT = env_int("KLINE_LIMIT", 99)
QUOTE_ASSETS = [q.strip().upper() for q in env("QUOTE_ASSETS", "USDT").split(",") if q.strip()]
MIN_QUOTE_VOLUME = env_float("MIN_QUOTE_VOLUME", 1_000_000)
COOLDOWN_HOURS = env_float("ALERT_COOLDOWN_HOURS", 6)
STATE_FILE = Path(env("STATE_FILE", "state/last_alerts.json"))
REQUESTS_PER_SECOND = env_float("REQUESTS_PER_SECOND", 5)
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


def http_json(path: str, params: dict | None = None) -> object:
    query = urllib.parse.urlencode(params or {})
    url = f"{BASE_URL}{path}?{query}" if query else BASE_URL + path
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
            elif exc.code in (403, 451):
                last_error += (
                    " | 币安对受限地区 IP 会返回 403/451，GitHub Actions 的美国 Runner 常被拦。"
                    "把 BINANCE_FUTURES_BASE_URL 指向自建反代，或给该 Secret 配 HTTPS_PROXY"
                )
                break
            elif 500 <= exc.code < 600:
                time.sleep(min(2**attempt, 30))
            else:
                break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = str(exc)
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
    last_price: float
    change_percent: float
    quote_volume: float
    rsi: dict[str, float | None] = field(default_factory=dict)

    @property
    def max_rsi(self) -> float:
        values = [v for v in self.rsi.values() if v is not None]
        return max(values) if values else 0.0

    def triggered(self, threshold: float, mode: str) -> bool:
        values = [v for v in self.rsi.values() if v is not None]
        if not values:
            return False
        return any(v > threshold for v in values) if mode == "any" else all(
            v > threshold for v in values
        )


def load_universe() -> tuple[list[str], dict[str, Row]]:
    info = http_json("/fapi/v1/exchangeInfo")
    symbols = [
        item["symbol"]
        for item in info["symbols"]
        if item.get("status") == "TRADING"
        and item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") in QUOTE_ASSETS
    ]
    tickers = {}
    for item in http_json("/fapi/v1/ticker/24hr"):
        if item["symbol"] in symbols:
            tickers[item["symbol"]] = Row(
                symbol=item["symbol"],
                last_price=float(item["lastPrice"]),
                change_percent=float(item.get("priceChangePercent") or 0.0),
                quote_volume=float(item.get("quoteVolume") or 0.0),
            )
    liquid = [s for s in symbols if s in tickers and tickers[s].quote_volume >= MIN_QUOTE_VOLUME]
    log(f"合约 {len(symbols)} 个，24h 成交额≥{MIN_QUOTE_VOLUME:,.0f} 的候选 {len(liquid)} 个")
    return liquid, tickers


def fetch_rsi(symbol: str) -> dict[str, float | None]:
    closes: dict[str, float | None] = {}
    for interval in RSI_INTERVALS:
        try:
            candles = http_json(
                "/fapi/v1/klines",
                {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT},
            )
            # 最后一根通常尚未收盘，用未收盘数据会导致 RSI 抖动和重复告警
            bars = candles[:-1] if len(candles) > RSI_PERIOD + 1 else candles
            closes[interval] = wilder_rsi([float(bar[4]) for bar in bars], RSI_PERIOD)
        except RuntimeError as exc:
            log(f"{symbol} {interval} 取数失败: {exc}")
            closes[interval] = None
    return closes


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


def render_text(rows: list[Row], generated: datetime) -> str:
    lines = [f"RSI>{RSI_THRESHOLD:g} 筛选结果（{', '.join(RSI_INTERVALS)}，{generated:%Y-%m-%d %H:%M} UTC）", ""]
    for row in rows:
        rsi_text = "  ".join(f"{i}={row.rsi[i]:.1f}" for i in RSI_INTERVALS if row.rsi.get(i) is not None)
        lines.append(
            f"{row.symbol}: {fmt_price(row.last_price)}  {row.change_percent:+.2f}%  {rsi_text}  "
            f"成交额{row.quote_volume / 1e6:.1f}M"
        )
    lines += ["", "基于已收盘 K 线，Wilder RSI，仅供参考，不构成投资建议。"]
    return "\n".join(lines)


def render_html(rows: list[Row], generated: datetime) -> str:
    th = 'padding:6px 10px;background:#f2f3f5;text-align:'
    header = (
        f'<th style="{th}left">币种</th>'
        f'<th style="{th}right">最新价</th>'
        f'<th style="{th}right">24h</th>'
        + "".join(f'<th style="{th}right">RSI({escape(i)})</th>' for i in RSI_INTERVALS)
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
        url = "https://www.binance.com/zh-CN/futures/" + escape(row.symbol)
        body.append(
            '<tr>'
            f'<td style="padding:6px 10px"><a href="{url}">{escape(row.symbol)}</a></td>'
            f'<td style="padding:6px 10px;text-align:right">{escape(fmt_price(row.last_price))}</td>'
            f'<td style="padding:6px 10px;text-align:right">{row.change_percent:+.2f}%</td>'
            f'{cells}'
            f'<td style="padding:6px 10px;text-align:right">{row.quote_volume / 1e6:.1f}M</td>'
            '</tr>'
        )
    note = "基于已收盘 K 线，Wilder RSI。仅供参考，不构成投资建议。"
    intro = (
        f"周期 {escape(', '.join(RSI_INTERVALS))} · RSI 周期 {RSI_PERIOD} · 阈值 {RSI_THRESHOLD:g} · "
        f"匹配 {escape(MATCH_MODE)} · {generated:%Y-%m-%d %H:%M} UTC · 共 {len(rows)} 个"
    )
    return (
        "<div style='font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#222'>"
        "<h3 style='margin:0 0 4px'>Binance 合约超买监控</h3>"
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
        log(f"{row.symbol:<16} {detail:<24} {row.change_percent:+7.2f}%  {row.quote_volume / 1e6:8.1f}M")

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
