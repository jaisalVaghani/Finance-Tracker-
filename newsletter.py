#!/usr/bin/env python3
"""
newsletter.py — Finance Tracker Daily Market Brief
Sends a beautiful HTML email every weekday at 8:00 AM IST.

Usage:
  python newsletter.py           # Run scheduler (sends 8 AM IST, Mon–Fri)
  python newsletter.py --test    # Send one email immediately (test setup)
"""

import os
import sys
import smtplib
import time
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    import schedule
except ImportError:
    print("Missing dependency. Run:  pip install schedule")
    sys.exit(1)

# ── reuse data helpers from app.py (no HTTP round-trip needed) ────────────────
from app import (
    _fetch_index,
    _fetch_commodity,
    _fetch_finance_news,
    _fetch_pf_mixed,
    _fetch_news,
    _INDEX_MAP,
    _INTL_MAP,
    _NEWS_URLS,
)

# ─────────────────────────────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))

# Maps _INDEX_MAP keys → _NEWS_URLS keys
_INDEX_NEWS_KEY = {
    "NIFTY 50":         "Nifty 50",
    "NIFTY MIDCAP 150": "Nifty Midcap 150",
    "NIFTY SMLCAP 250": "Nifty Smallcap 250",
}


def _now_ist() -> str:
    return datetime.now(IST).strftime("%H:%M:%S IST")


# ─────────────────────────── env validation ──────────────────────────────────

def check_env_vars() -> bool:
    required = ("GMAIL_ADDRESS", "GMAIL_APP_PASSWORD", "RECIPIENT_EMAIL")
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        print("❌  Missing environment variables:")
        for v in missing:
            print(f"    {v}")
        print("\nSee setup_newsletter.txt for setup instructions.")
        return False
    return True


# ─────────────────────────── data fetching ───────────────────────────────────

def fetch_all_data() -> dict:
    print(f"[{_now_ist()}] Fetching market data …")
    data: dict = {
        "indices":      {},
        "commodities":  {},
        "intl":         {},
        "finance_news": [],
        "pf_insights":  [],
    }

    # NSE indices + top news headline as 1-line reason
    for name, symbol in _INDEX_MAP.items():
        try:
            d = _fetch_index(name, symbol)
            reason = ""
            news_key = _INDEX_NEWS_KEY.get(name)
            if news_key and news_key in _NEWS_URLS:
                try:
                    articles = _fetch_news(_NEWS_URLS[news_key], 1)
                    if articles:
                        raw = articles[0].get("title", "")
                        # Strip trailing "  - Source Name" attribution
                        idx = raw.rfind(" - ")
                        reason = raw[:idx].strip() if idx > 0 else raw
                except Exception:
                    pass
            d["reason"] = reason
            data["indices"][name] = d
        except Exception as exc:
            print(f"  ⚠  Index {name}: {exc}")

    # MCX commodities
    for key, (nm, et_sym, yf_sym) in {
        "GOLD":   ("MCX Gold",   "GOLD",   "GC=F"),
        "SILVER": ("MCX Silver", "SILVER", "SI=F"),
    }.items():
        try:
            data["commodities"][key] = _fetch_commodity(nm, et_sym, yf_sym)
        except Exception as exc:
            print(f"  ⚠  Commodity {key}: {exc}")

    # International markets
    for name, info in _INTL_MAP.items():
        try:
            d = _fetch_index(name, info["symbol"])
            d["currSym"] = info["currSym"]
            data["intl"][name] = d
        except Exception as exc:
            print(f"  ⚠  Intl {name}: {exc}")

    # Finance & Economy news (top 5)
    try:
        data["finance_news"] = _fetch_finance_news(5)
    except Exception as exc:
        print(f"  ⚠  Finance news: {exc}")

    # Personal Finance insights (top 3)
    try:
        data["pf_insights"] = _fetch_pf_mixed(3)
    except Exception as exc:
        print(f"  ⚠  PF insights: {exc}")

    print(f"[{_now_ist()}] ✓ Data ready.")
    return data


# ─────────────────────────── HTML helpers ────────────────────────────────────

def _col(p) -> str:
    """Text color for a % change value."""
    if p is None:
        return "#8b949e"
    return "#3fb950" if p >= 0 else "#f85149"


def _bg(p) -> str:
    """Badge background for a % change value."""
    if p is None:
        return "rgba(139,148,158,.12)"
    return "rgba(63,185,80,.14)" if p >= 0 else "rgba(248,81,73,.14)"


def _arrow(p) -> str:
    """▲ 1.23% or ▼ 1.23% or — for None."""
    if p is None:
        return "—"
    return f"{'▲' if p >= 0 else '▼'} {abs(p):.2f}%"


def _num(n, dec: int = 2) -> str:
    """Format a number with commas, or — for None."""
    if n is None:
        return "—"
    return f"{n:,.{dec}f}"


def _section(title: str, body: str) -> str:
    return f"""
  <div style="margin-bottom:32px;">
    <div style="font-size:10.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;
                color:#8b949e;padding-bottom:9px;border-bottom:1px solid #30363d;margin-bottom:16px;">
      {title}
    </div>
    {body}
  </div>"""


# ─────────────────────────── card builders ───────────────────────────────────

def _index_card(d: dict) -> str:
    p      = d.get("pChange")
    col    = _col(p)
    reason = d.get("reason", "")
    reason_html = (
        f'<div style="margin-top:7px;font-size:11.5px;color:#8b949e;line-height:1.45;">'
        f'💬 {reason}</div>'
    ) if reason else ""
    return f"""
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                padding:14px 18px;margin-bottom:10px;">
      <div style="display:flex;align-items:center;justify-content:space-between;
                  gap:10px;flex-wrap:wrap;">
        <span style="font-size:13.5px;font-weight:700;color:#e6edf3;">{d.get("name","")}</span>
        <div style="display:flex;align-items:center;gap:10px;">
          <span style="font-size:19px;font-weight:800;color:{col};">₹{_num(d.get("last"))}</span>
          <span style="font-size:12px;font-weight:700;color:{col};background:{_bg(p)};
                       padding:3px 10px;border-radius:20px;">{_arrow(p)}</span>
        </div>
      </div>
      {reason_html}
      <div style="margin-top:9px;display:flex;gap:16px;font-size:11px;color:#8b949e;flex-wrap:wrap;">
        <span>Open <b style="color:#e6edf3;">₹{_num(d.get("open"))}</b></span>
        <span>High <b style="color:#3fb950;">₹{_num(d.get("high"))}</b></span>
        <span>Low  <b style="color:#f85149;">₹{_num(d.get("low"))}</b></span>
        <span>Prev <b style="color:#e6edf3;">₹{_num(d.get("prevClose"))}</b></span>
      </div>
    </div>"""


def _intl_card(name: str, d: dict) -> str:
    p    = d.get("pChange")
    col  = _col(p)
    curr = d.get("currSym", "")
    return f"""
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                padding:12px 18px;margin-bottom:10px;">
      <div style="display:flex;align-items:center;justify-content:space-between;
                  gap:10px;flex-wrap:wrap;">
        <span style="font-size:13.5px;font-weight:700;color:#e6edf3;">{name}</span>
        <div style="display:flex;align-items:center;gap:10px;">
          <span style="font-size:17px;font-weight:800;color:{col};">{curr}{_num(d.get("last"))}</span>
          <span style="font-size:12px;font-weight:700;color:{col};background:{_bg(p)};
                       padding:3px 10px;border-radius:20px;">{_arrow(p)}</span>
        </div>
      </div>
      <div style="margin-top:7px;display:flex;gap:16px;font-size:11px;color:#8b949e;flex-wrap:wrap;">
        <span>Open <b style="color:#e6edf3;">{curr}{_num(d.get("open"))}</b></span>
        <span>High <b style="color:#3fb950;">{curr}{_num(d.get("high"))}</b></span>
        <span>Low  <b style="color:#f85149;">{curr}{_num(d.get("low"))}</b></span>
        <span>Prev <b style="color:#e6edf3;">{curr}{_num(d.get("prevClose"))}</b></span>
      </div>
    </div>"""


def _comm_card(d: dict) -> str:
    p      = d.get("pChange")
    col    = _col(p)
    source = d.get("source", "")
    src_html = (
        f'<span style="font-size:10px;color:#8b949e;margin-top:4px;display:block;">Source: {source}</span>'
    ) if source else ""
    return f"""
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                padding:14px 18px;margin-bottom:10px;">
      <div style="display:flex;align-items:center;justify-content:space-between;
                  gap:10px;flex-wrap:wrap;">
        <span style="font-size:13.5px;font-weight:700;color:#e6edf3;">{d.get("name","")}</span>
        <div style="display:flex;align-items:center;gap:10px;">
          <span style="font-size:19px;font-weight:800;color:{col};">₹{_num(d.get("current"))}</span>
          <span style="font-size:12px;font-weight:700;color:{col};background:{_bg(p)};
                       padding:3px 10px;border-radius:20px;">{_arrow(p)}</span>
        </div>
      </div>
      {src_html}
    </div>"""


def _news_block(articles: list) -> str:
    if not articles:
        return '<p style="color:#8b949e;font-size:12px;padding:8px 0;">No recent news available.</p>'
    items = articles[:5]
    rows  = []
    for i, a in enumerate(items):
        title   = a.get("title", "")
        source  = a.get("source", "")
        link    = a.get("link", "#")
        cat     = a.get("category", "")
        is_last = i == len(items) - 1
        border  = "" if is_last else "border-bottom:1px solid #21262d;"
        cat_html = (
            f'<span style="font-size:10px;font-weight:700;background:rgba(88,166,255,.15);'
            f'color:#58a6ff;padding:1px 6px;border-radius:3px;margin-right:5px;">{cat}</span>'
        ) if cat else ""
        src_html = (
            f'<span style="font-size:10px;font-weight:600;color:#8b949e;'
            f'background:rgba(139,148,158,.12);padding:1px 5px;border-radius:3px;">{source}</span>'
        ) if source else ""
        rows.append(f"""
        <div style="padding:11px 0;{border}">
          <a href="{link}" target="_blank"
             style="color:#e6edf3;text-decoration:none;font-size:13px;font-weight:600;
                    line-height:1.45;display:block;margin-bottom:5px;">{title}</a>
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
            {cat_html}{src_html}
          </div>
        </div>""")
    inner = "".join(rows)
    return (
        f'<div style="background:#161b22;border:1px solid #30363d;'
        f'border-radius:10px;padding:4px 18px;">{inner}</div>'
    )


def _pf_block(articles: list) -> str:
    if not articles:
        return '<p style="color:#8b949e;font-size:12px;padding:8px 0;">No recent insights available.</p>'
    items = articles[:3]
    rows  = []
    for i, a in enumerate(items):
        title   = a.get("title", "")
        source  = a.get("source", "")
        link    = a.get("link", "#")
        stype   = a.get("source_type", "")
        is_last = i == len(items) - 1
        border  = "" if is_last else "border-bottom:1px solid #21262d;"
        if stype == "reddit":
            src_style = "background:rgba(63,185,80,.12);color:#3fb950;"
        elif stype == "blog":
            src_style = "background:rgba(227,179,65,.12);color:#e3b341;"
        else:
            src_style = "background:rgba(88,166,255,.12);color:#58a6ff;"
        src_html = (
            f'<span style="font-size:10px;font-weight:700;padding:1px 6px;'
            f'border-radius:3px;{src_style}">{source}</span>'
        ) if source else ""
        rows.append(f"""
        <div style="padding:11px 0;{border}">
          <a href="{link}" target="_blank"
             style="color:#e6edf3;text-decoration:none;font-size:13px;font-weight:600;
                    line-height:1.45;display:block;margin-bottom:5px;">{title}</a>
          {src_html}
        </div>""")
    inner = "".join(rows)
    return (
        f'<div style="background:#161b22;border:1px solid #30363d;'
        f'border-radius:10px;padding:4px 18px;">{inner}</div>'
    )


# ─────────────────────────── full email HTML ─────────────────────────────────

def build_email(data: dict, date_str: str) -> str:
    indices_html = "".join(_index_card(d) for d in data["indices"].values())
    comm_html    = "".join(_comm_card(d)  for d in data["commodities"].values())
    intl_html    = "".join(_intl_card(n, d) for n, d in data["intl"].items())
    no_data      = '<p style="color:#8b949e;font-size:12px;">Data unavailable.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Morning Market Brief</title>
</head>
<body style="margin:0;padding:0;background:#0d1117;color:#e6edf3;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:640px;margin:0 auto;padding:24px 16px 48px;">

    <!-- ── HEADER ────────────────────────────────────────────────────── -->
    <div style="background:#161b22;border:1px solid #30363d;border-radius:12px;
                padding:26px 24px;margin-bottom:30px;text-align:center;">
      <div style="font-size:11px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;
                  color:#58a6ff;margin-bottom:8px;">Finance Tracker</div>
      <div style="font-size:24px;font-weight:800;color:#e6edf3;letter-spacing:-.02em;">
        📊 Morning Market Brief
      </div>
      <div style="margin-top:6px;font-size:13px;color:#8b949e;">{date_str}</div>
      <div style="margin-top:16px;border-top:1px solid #30363d;padding-top:14px;
                  font-size:11.5px;color:#8b949e;display:flex;justify-content:center;
                  gap:16px;flex-wrap:wrap;">
        <span>📊 NSE Indices</span>
        <span style="color:#30363d;">·</span>
        <span>🪙 MCX Commodities</span>
        <span style="color:#30363d;">·</span>
        <span>🌍 Global Markets</span>
        <span style="color:#30363d;">·</span>
        <span>📰 Finance News</span>
        <span style="color:#30363d;">·</span>
        <span>💡 Personal Finance</span>
      </div>
    </div>

    {_section("📊 NSE Market Indices", indices_html or no_data)}
    {_section("🪙 Commodities · MCX", comm_html or no_data)}
    {_section("🌍 International Markets", intl_html or no_data)}
    {_section("📰 Finance &amp; Economy News", _news_block(data.get("finance_news", [])))}
    {_section("💡 Personal Finance &amp; Insights", _pf_block(data.get("pf_insights", [])))}

    <!-- ── FOOTER ────────────────────────────────────────────────────── -->
    <div style="text-align:center;padding-top:20px;border-top:1px solid #30363d;">
      <div style="font-size:11px;color:#8b949e;line-height:1.8;">
        Data: Yahoo Finance · ET Markets · Google News RSS<br>
        Sent every weekday at <b>8:00 AM IST</b> by Finance Tracker<br>
        <a href="http://localhost:5000"
           style="color:#58a6ff;text-decoration:none;font-weight:600;">
          Open Live Dashboard →
        </a>
      </div>
    </div>

  </div>
</body>
</html>"""


# ─────────────────────────── send via Gmail SMTP ─────────────────────────────

def send_email(html: str, subject: str) -> bool:
    sender    = os.environ["GMAIL_ADDRESS"]
    password  = os.environ["GMAIL_APP_PASSWORD"]
    recipient = os.environ["RECIPIENT_EMAIL"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Finance Tracker <{sender}>"
    msg["To"]      = recipient
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, recipient, msg.as_string())
        print(f"[{_now_ist()}] ✅  Email sent → {recipient}")
        return True
    except smtplib.SMTPAuthenticationError:
        print(
            f"[{_now_ist()}] ❌  Authentication failed.\n"
            "    Use your Gmail App Password (16 chars), not your regular password.\n"
            "    See setup_newsletter.txt for instructions."
        )
        return False
    except Exception as exc:
        print(f"[{_now_ist()}] ❌  Send failed: {exc}")
        return False


# ─────────────────────────── newsletter job ──────────────────────────────────

def run_newsletter() -> None:
    if not check_env_vars():
        return

    now_ist  = datetime.now(IST)
    date_str = now_ist.strftime("%A, %d %B %Y")

    data = fetch_all_data()

    # Subject: "📊 Morning Market Brief | 08 May 2025 | Nifty 24,350.50 +0.42%"
    nifty   = data["indices"].get("NIFTY 50", {})
    price   = _num(nifty.get("last"))
    pct     = nifty.get("pChange")
    pct_str = (("+" if pct >= 0 else "") + f"{pct:.2f}%") if pct is not None else "—"
    subject = (
        f"📊 Morning Market Brief | {now_ist.strftime('%d %b %Y')} | "
        f"Nifty {price} {pct_str}"
    )

    html = build_email(data, date_str)
    send_email(html, subject)


# ─────────────────────────── scheduler (8 AM IST, Mon–Fri) ──────────────────

_last_sent_date: object = None   # prevents double-send within the same minute


def _tick() -> None:
    global _last_sent_date
    now = datetime.now(IST)
    if now.weekday() >= 5:                    # Skip Saturday (5) and Sunday (6)
        return
    if now.hour == 8 and now.minute == 0:
        today = now.date()
        if _last_sent_date != today:
            _last_sent_date = today
            run_newsletter()


def main() -> None:
    print("╔══════════════════════════════════════════╗")
    print("║  Finance Tracker — Daily Newsletter      ║")
    print("║  Sends every weekday at 8:00 AM IST      ║")
    print("╚══════════════════════════════════════════╝")
    print(f"\nCurrent IST : {datetime.now(IST).strftime('%A, %d %b %Y  %H:%M:%S')}")
    print()

    if not check_env_vars():
        sys.exit(1)

    schedule.every(1).minutes.do(_tick)
    print("✅  Scheduler active. Waiting for 8:00 AM IST (Mon–Fri).")
    print("    Press Ctrl+C to stop.\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ─────────────────────────── entry point ─────────────────────────────────────

if __name__ == "__main__":
    if "--test" in sys.argv:
        print("🧪  Test mode — sending one email now …\n")
        run_newsletter()
    else:
        main()
