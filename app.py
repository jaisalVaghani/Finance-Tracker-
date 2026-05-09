import math
import os
import time as _time
from datetime import datetime, timezone, timedelta

import anthropic
import feedparser
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# ─────────────────────────── index symbols ──────────────────────────────

_INDEX_MAP = {
    "NIFTY 50":          "^NSEI",
    "NIFTY MIDCAP 150":  "NIFTYMIDCAP150.NS",
    "NIFTY SMLCAP 250":  "^CNX200",
}

_INTL_MAP = {
    "NASDAQ":  {"symbol": "^IXIC", "currSym": "$"},
    "S&P 500": {"symbol": "^GSPC", "currSym": "$"},
    "KOSPI":   {"symbol": "^KS11", "currSym": "₩"},
    "TAIEX":   {"symbol": "^TWII", "currSym": "NT$"},
}

_INDIAN_MARKETS = {"Nifty 50", "Nifty Midcap 150", "Nifty Smallcap 250", "MCX Gold", "MCX Silver"}

# ─────────────────────────── ET Markets commodity scraping ───────────────

_ET_COMM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://economictimes.indiatimes.com/",
}

_et_spot_cache: dict[str, tuple[float, float, float]] = {}  # sym -> (price, prev, ts)


def _fetch_et_spot(sym: str) -> tuple[float, float]:
    """Return (current_inr, prev_inr) from ET Markets. Cached 5 min."""
    now = _time.time()
    cached = _et_spot_cache.get(sym)
    if cached:
        price, prev, ts = cached
        if now - ts < 300:
            return price, prev
    url = f"https://economictimes.indiatimes.com/commoditysummary/symbol-{sym}.cms"
    r = requests.get(url, headers=_ET_COMM_HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    price = float(soup.select_one("span.commodityPrice").get_text(strip=True))
    prev  = float(soup.select_one("span.data.previousClosePrice").get_text(strip=True))
    _et_spot_cache[sym] = (price, prev, now)
    app.logger.info("ET spot %s: %.2f (prev %.2f)", sym, price, prev)
    return price, prev


# ─────────────────────────── yfinance fallback (commodity) ───────────────

# yf symbol → ET symbol
_ET_SYM_MAP = {"GC=F": "GOLD", "SI=F": "SILVER"}


def _usd_inr() -> float:
    try:
        return float(yf.Ticker("USDINR=X").fast_info.last_price or 84.0)
    except Exception:
        return 84.0


def _gold_inr_fallback(usd_per_oz: float) -> float:
    """Gold per 10g INR — only used if ET Markets is unreachable."""
    return (usd_per_oz / 31.1035) * 10 * _usd_inr() * 1.155


def _silver_inr_fallback(usd_per_oz: float) -> float:
    """Silver per kg INR — only used if ET Markets is unreachable."""
    return usd_per_oz * 32.1507 * _usd_inr()


_YF_FALLBACK_CONV = {
    "GC=F": _gold_inr_fallback,
    "SI=F": _silver_inr_fallback,
}

# ─────────────────────────── data helpers ────────────────────────────────

def _intraday(symbol: str) -> tuple[list, list]:
    try:
        hist = yf.Ticker(symbol).history(period="1d", interval="1m")
        if hist.empty:
            return [], []
        times  = [ts.strftime("%H:%M") for ts in hist.index]
        prices = [round(float(p), 2) for p in hist["Close"]]
        return times, prices
    except Exception as exc:
        app.logger.warning("intraday %s: %s", symbol, exc)
        return [], []


def clean_value(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _fetch_index(name: str, symbol: str) -> dict:
    tkr   = yf.Ticker(symbol)
    daily = tkr.history(period="5d", interval="1d")
    fi    = tkr.fast_info

    last  = clean_value(round(float(fi.last_price     or 0), 2))
    prev  = clean_value(round(float(fi.previous_close or 0), 2))

    bar   = daily.iloc[-1] if not daily.empty else None
    high  = clean_value(round(float(bar["High"]), 2)) if bar is not None else None
    low   = clean_value(round(float(bar["Low"]),  2)) if bar is not None else None
    open_ = clean_value(round(float(bar["Open"]), 2)) if bar is not None else None

    change  = round((last or 0) - (prev or 0), 2)
    pchange = clean_value(round(change / prev * 100, 2)) if prev else 0.0

    times, prices = _intraday(symbol)

    return {
        "name":      name,
        "last":      last,
        "high":      high,
        "low":       low,
        "open":      open_,
        "prevClose": prev,
        "change":    change,
        "pChange":   pchange,
        "chart":     {"times": times, "prices": prices},
    }


def _fetch_commodity(name: str, et_sym: str, yf_sym: str) -> dict:
    try:
        current_inr, prev_inr = _fetch_et_spot(et_sym)
        source = "ET Markets"
    except Exception as exc:
        app.logger.warning("ET spot %s failed (%s), using yfinance fallback", et_sym, exc)
        conv = _YF_FALLBACK_CONV[yf_sym]
        fi = yf.Ticker(yf_sym).fast_info
        current_inr = round(conv(float(fi.last_price or 0)), 2)
        prev_inr    = round(conv(float(fi.previous_close or 0)), 2)
        source = "yfinance"

    pchange = round((current_inr - prev_inr) / prev_inr * 100, 2) if prev_inr else 0.0

    times, raw = _intraday(yf_sym)
    prices: list[float] = []
    if raw:
        yf_last = float(raw[-1])
        scale = current_inr / yf_last if yf_last else 1.0
        prices = [round(float(p) * scale, 2) for p in raw]

    return {
        "name":    name,
        "current": round(current_inr, 2),
        "pChange": pchange,
        "times":   times,
        "prices":  prices,
        "source":  source,
    }


# ─────────────────────────── news ───────────────────────────────────────

_NEWS_URLS: dict[str, str] = {
    "Nifty 50":           "https://news.google.com/rss/search?q=Nifty+50+sensex+today&hl=en-IN&gl=IN&ceid=IN:en",
    "Nifty Midcap 150":   "https://news.google.com/rss/search?q=Nifty+midcap+india+today&hl=en-IN&gl=IN&ceid=IN:en",
    "Nifty Smallcap 250": "https://news.google.com/rss/search?q=Nifty+smallcap+india+today&hl=en-IN&gl=IN&ceid=IN:en",
    "MCX Gold":           "https://news.google.com/rss/search?q=gold+price+india+MCX+today&hl=en-IN&gl=IN&ceid=IN:en",
    "MCX Silver":         "https://news.google.com/rss/search?q=silver+price+india+MCX+today&hl=en-IN&gl=IN&ceid=IN:en",
    "NASDAQ":             "https://news.google.com/rss/search?q=Nasdaq+today+rise+fall+reason&hl=en&gl=US&ceid=US:en",
    "S&P 500":            "https://news.google.com/rss/search?q=S%26P+500+today+market+reason&hl=en&gl=US&ceid=US:en",
    "KOSPI":              "https://news.google.com/rss/search?q=KOSPI+Korea+stock+market+today&hl=en&gl=US&ceid=US:en",
    "TAIEX":              "https://news.google.com/rss/search?q=Taiwan+TAIEX+stock+market+today&hl=en&gl=US&ceid=US:en",
}

_PRIORITY_DOMAINS = [
    "economictimes.indiatimes.com",
    "livemint.com",
    "moneycontrol.com",
    "business-standard.com",
    "ndtvprofit.com",
]


def _source_rank(source: str) -> int:
    s = source.lower()
    for i, domain in enumerate(_PRIORITY_DOMAINS):
        keyword = domain.split(".")[0].replace("-", "")
        if keyword in s or domain in s:
            return i
    return len(_PRIORITY_DOMAINS)


def _fetch_news(url: str, count: int = 3) -> list[dict]:
    feed   = feedparser.parse(url)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    raw: list[dict] = []
    for e in feed.entries:
        pt = e.get("published_parsed")
        if pt:
            try:
                pub_dt = datetime(*pt[:6], tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
            except Exception:
                pass
        source = ""
        src_obj = e.get("source") or {}
        if hasattr(src_obj, "get"):
            source = src_obj.get("title", "")
        if not source:
            title = e.get("title", "")
            idx = title.rfind(" - ")
            if idx > 0:
                source = title[idx + 3:].strip()
        raw.append({
            "title":     e.get("title", ""),
            "link":      e.get("link", "#"),
            "published": e.get("published", ""),
            "source":    source,
        })
    raw.sort(key=lambda a: _source_rank(a["source"]))
    return raw[:count]


# Cache ET article for 5 min so all 5 cards on a page share one fetch
_et_article_cache: tuple[str, str, float] = ("", "", 0.0)

_ET_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}
_ET_BASE = "https://economictimes.indiatimes.com"
_ET_LISTING = f"{_ET_BASE}/markets/stocks/news"


def _fetch_et_article_text() -> tuple[str, str]:
    """Return (article_text, article_url) from the top ET markets story."""
    global _et_article_cache
    text, url, ts = _et_article_cache
    if text and (_time.time() - ts) < 300:
        return text, url

    try:
        resp = requests.get(_ET_LISTING, headers=_ET_HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        article_url = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "articleshow" in href and "/markets/" in href:
                article_url = href if href.startswith("http") else _ET_BASE + href
                break

        if not article_url:
            return "", ""

        art_resp = requests.get(article_url, headers=_ET_HEADERS, timeout=10)
        art_soup = BeautifulSoup(art_resp.text, "html.parser")

        body = (
            art_soup.find("div", class_="artText")
            or art_soup.find("div", {"class": "Normal"})
            or art_soup.find("div", {"class": "article_content"})
            or art_soup.find("article")
        )
        paras = body.find_all("p") if body else art_soup.find_all("p")
        texts = [p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 40]
        article_text = " ".join(texts[:3])

        _et_article_cache = (article_text, article_url, _time.time())
        return article_text, article_url
    except Exception as exc:
        app.logger.warning("ET article fetch: %s", exc)
        return "", ""


def _ai_summary(name: str, pchange: float) -> tuple[str, str]:
    """Return (2-line AI summary, article_url) from top ET article."""
    try:
        article_text, article_url = _fetch_et_article_text()
        if not article_text:
            return "", ""
        prompt = (
            f"In 2 lines, what is the main reason Indian markets moved today "
            f"based on this: {article_text}"
        )
        msg = anthropic.Anthropic().messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip(), article_url
    except Exception as exc:
        app.logger.warning("AI summary failed: %s", exc)
        return "", ""


# ─────────────────────────── chart endpoint ──────────────────────────────

_PERIOD_MAP: dict[str, tuple[str, str]] = {
    "1D": ("1d",  "1m"),
    "1W": ("7d",  "1h"),
    "1M": ("1mo", "1d"),
    "6M": ("6mo", "1d"),
    "1Y": ("1y",  "1wk"),
    "5Y": ("5y",  "1wk"),
}

_TIME_FMT: dict[str, str] = {
    "1D": "%H:%M",
    "1W": "%a %H:%M",
    "1M": "%d %b",
    "6M": "%d %b",
    "1Y": "%b '%y",
    "5Y": "%b '%y",
}


# ─────────────────────────── routes ─────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/indices")
def api_indices():
    try:
        data = {}
        for name, symbol in _INDEX_MAP.items():
            try:
                data[name] = _fetch_index(name, symbol)
            except Exception as exc:
                app.logger.warning("Index %s (%s): %s", name, symbol, exc)
        return jsonify({"ok": True, "data": data})
    except Exception as exc:
        app.logger.error("/api/indices: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/commodities")
def api_commodities():
    try:
        gold   = _fetch_commodity("MCX Gold",   "GOLD", "GC=F")
        silver = _fetch_commodity("MCX Silver", "SILVER", "SI=F")
        return jsonify({"ok": True, "data": {"GOLD": gold, "SILVER": silver}})
    except Exception as exc:
        app.logger.error("/api/commodities: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/chart")
def api_chart():
    symbol = request.args.get("symbol", "").strip()
    period = request.args.get("period", "1D").upper()
    if not symbol or period not in _PERIOD_MAP:
        return jsonify({"ok": False, "error": "invalid params"}), 400
    try:
        yf_period, yf_interval = _PERIOD_MAP[period]
        ts_fmt = _TIME_FMT[period]
        closes = yf.Ticker(symbol).history(
            period=yf_period, interval=yf_interval
        )["Close"].dropna()
        if closes.empty:
            return jsonify({"ok": True, "data": {"times": [], "prices": [], "pChange": 0.0}})
        times = [ts.strftime(ts_fmt) for ts in closes.index]
        et_sym = _ET_SYM_MAP.get(symbol)
        if et_sym:
            et_current, et_prev = _fetch_et_spot(et_sym)
            yf_last = float(closes.iloc[-1])
            scale = et_current / yf_last if yf_last else 1.0
            prices = [round(float(p) * scale, 2) for p in closes]
            if period == "1D" and et_prev:
                pchange = round((et_current - et_prev) / et_prev * 100, 2)
            else:
                pchange = round((prices[-1] - prices[0]) / prices[0] * 100, 2) if prices[0] else 0.0
        else:
            prices = [round(float(p), 2) for p in closes]
            pchange = round((prices[-1] - prices[0]) / prices[0] * 100, 2) if prices[0] else 0.0
        return jsonify({"ok": True, "data": {"times": times, "prices": prices, "pChange": pchange}})
    except Exception as exc:
        app.logger.error("/api/chart %s %s: %s", symbol, period, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/intl")
def api_intl():
    try:
        data = {}
        for name, info in _INTL_MAP.items():
            try:
                d = _fetch_index(name, info["symbol"])
                d["currSym"] = info["currSym"]
                data[name] = d
            except Exception as exc:
                app.logger.warning("Intl %s: %s", name, exc)
        return jsonify({"ok": True, "data": data})
    except Exception as exc:
        app.logger.error("/api/intl: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/news")
def api_news():
    name = request.args.get("q", "Nifty 50")
    try:
        pchange = float(request.args.get("pchange", "0"))
    except ValueError:
        pchange = 0.0
    url      = _NEWS_URLS.get(name, _NEWS_URLS["Nifty 50"])
    articles = _fetch_news(url)
    if name in _INDIAN_MARKETS:
        summary, article_url = _ai_summary(name, pchange)
    else:
        summary, article_url = "", ""
    return jsonify({"ok": True, "data": {
        "summary": summary, "article_url": article_url, "articles": articles
    }})


# ─────────────────────────── market buzz ─────────────────────────────────

_BUZZ_CACHE: dict = {"data": None, "ts": 0.0}
_BUZZ_TTL   = 900   # 15 minutes

_FINANCE_NEWS_URLS = [
    "https://news.google.com/rss/search?q=india+economy+RBI+inflation+finance&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=india+tax+GST+budget+government+policy&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=india+ETF+mutual+fund+SEBI+investment&hl=en-IN&gl=IN&ceid=IN:en",
]

_GLOBAL_NEWS_URLS = [
    "https://news.google.com/rss/search?q=global+economy+federal+reserve+inflation&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=wall+street+stocks+dow+nasdaq+sp500+market&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=china+europe+world+economy+trade+geopolitics&hl=en&gl=US&ceid=US:en",
]

_FINANCE_PRIORITY_SRCS = [
    "economic times", "economictimes",
    "mint", "livemint",
    "business standard", "business-standard",
    "bloomberg",
    "reuters",
]

_FINANCE_CATEGORIES = [
    ("🏦 RBI",     ["rbi", "repo rate", "reserve bank", "monetary policy", "rbi governor"]),
    ("💰 Tax",     ["tax", "gst", "income tax", "tds", "direct tax", "itr"]),
    ("📊 ETF/MF",  ["etf", "mutual fund", "sip", "sebi", "nfo", "nav"]),
    ("🏛️ Policy", ["policy", "government", "ministry", "parliament", "finance minister", "regulation"]),
]


def _categorize_article(title: str) -> str:
    t = title.lower()
    for label, keywords in _FINANCE_CATEGORIES:
        if any(kw in t for kw in keywords):
            return label
    return ""


def _finance_src_rank(source: str) -> int:
    s = source.lower()
    for i, kw in enumerate(_FINANCE_PRIORITY_SRCS):
        if kw in s:
            return i
    return len(_FINANCE_PRIORITY_SRCS)

_REDDIT_FEEDS = [
    ("https://www.reddit.com/r/personalfinanceindia/new/.rss", "r/personalfinanceindia"),
    ("https://www.reddit.com/r/IndiaInvestments/new/.rss",     "r/IndiaInvestments"),
    ("https://www.reddit.com/r/IndiaTax/new/.rss",             "r/IndiaTax"),
]

_BLOG_FEEDS = [
    ("https://alphaideas.in/feed/",            "Alpha Ideas"),
    ("https://freefincal.com/feed/",            "Freefincal"),
    ("https://www.safalniveshak.com/feed/",     "Safal Niveshak"),
    ("https://capitalmind.in/feed/",            "Capitalmind"),
]

_PF_KEYWORDS = frozenset([
    "mutual fund", "sip", "tax", "investment", "insurance",
    "fd", "ppf", "nps", "elss", "loan", "saving", "salary", "itr", "portfolio",
])

_REDDIT_UA = "Mozilla/5.0 (compatible; finance-dashboard/1.0)"


def _fetch_finance_news(count: int = 8) -> list[dict]:
    """Fetch finance & economy news from 3 topic feeds.
    Deduplicates, tags categories, prioritizes ET/Mint/BS/Bloomberg/Reuters."""
    now_utc   = datetime.now(timezone.utc)
    cutoff_48 = now_utc - timedelta(hours=48)
    cutoff_7d = now_utc - timedelta(days=7)

    def _parse(cutoff):
        items: list[dict] = []
        seen: set[str] = set()
        for feed_url in _FINANCE_NEWS_URLS:
            try:
                feed = feedparser.parse(feed_url)
                for e in feed.entries:
                    pt = e.get("published_parsed")
                    pub_dt = None
                    if pt:
                        try:
                            pub_dt = datetime(*pt[:6], tzinfo=timezone.utc)
                            if pub_dt < cutoff:
                                continue
                        except Exception:
                            pass
                    raw_title = e.get("title", "")
                    source = ""
                    idx = raw_title.rfind(" - ")
                    if idx > 0:
                        source = raw_title[idx + 3:].strip()
                        title  = raw_title[:idx].strip()
                    else:
                        title = raw_title
                    if len(title) < 20:
                        continue
                    prefix = title[:40].lower()
                    if prefix in seen:
                        continue
                    seen.add(prefix)
                    items.append({
                        "title":     title[:150],
                        "source":    source,
                        "link":      e.get("link", "#"),
                        "published": e.get("published", ""),
                        "category":  _categorize_article(title),
                        "_pub_dt":   pub_dt,
                        "_src_rank": _finance_src_rank(source),
                    })
            except Exception as exc:
                app.logger.warning("Finance RSS %s: %s", feed_url, exc)
        items.sort(key=lambda x: (
            x["_src_rank"],
            -(x["_pub_dt"].timestamp() if x["_pub_dt"] else 0),
        ))
        return items

    items = _parse(cutoff_48)
    if len(items) < 3:
        items = _parse(cutoff_7d)

    for item in items:
        item.pop("_pub_dt", None)
        item.pop("_src_rank", None)
    return items[:count]


def _fetch_rss_articles(urls: list[str], count: int = 6) -> list[dict]:
    """Fetch articles from RSS feeds, deduplicate, sort newest first.
    Prefers articles within 48h; falls back to 7 days if fewer than 3 found."""
    now_utc   = datetime.now(timezone.utc)
    cutoff_48 = now_utc - timedelta(hours=48)
    cutoff_7d = now_utc - timedelta(days=7)

    def _parse_entries(cutoff):
        all_items: list[dict] = []
        seen: set[str] = set()
        for feed_url in urls:
            try:
                feed = feedparser.parse(feed_url)
                for e in feed.entries:
                    pt = e.get("published_parsed")
                    pub_dt = None
                    if pt:
                        try:
                            pub_dt = datetime(*pt[:6], tzinfo=timezone.utc)
                            if pub_dt < cutoff:
                                continue
                        except Exception:
                            pass
                    raw_title = e.get("title", "")
                    source = ""
                    idx = raw_title.rfind(" - ")
                    if idx > 0:
                        source = raw_title[idx + 3:].strip()
                        title  = raw_title[:idx].strip()
                    else:
                        title = raw_title
                    if len(title) < 20:
                        continue
                    prefix = title[:40].lower()
                    if prefix in seen:
                        continue
                    seen.add(prefix)
                    all_items.append({
                        "title":     title[:150],
                        "source":    source,
                        "link":      e.get("link", "#"),
                        "published": e.get("published", ""),
                        "_pub_dt":   pub_dt,
                    })
            except Exception as exc:
                app.logger.warning("RSS feed %s: %s", feed_url, exc)
        all_items.sort(
            key=lambda x: x["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return all_items

    items = _parse_entries(cutoff_48)
    if len(items) < 3:
        items = _parse_entries(cutoff_7d)

    for item in items:
        item.pop("_pub_dt", None)
    return items[:count]


def _fetch_pf_mixed(count: int = 8) -> list[dict]:
    """Fetch personal finance content from Reddit RSS + Indian finance blogs."""
    cutoff_reddit = datetime.now(timezone.utc) - timedelta(hours=48)
    cutoff_blog   = datetime.now(timezone.utc) - timedelta(days=7)
    all_items: list[dict] = []
    seen: set[str] = set()

    for feed_url, label in _REDDIT_FEEDS:
        try:
            feed = feedparser.parse(
                feed_url,
                request_headers={"User-Agent": _REDDIT_UA},
            )
            for e in feed.entries:
                title = e.get("title", "").strip()
                if len(title) < 10:
                    continue
                if not any(kw in title.lower() for kw in _PF_KEYWORDS):
                    continue
                pt = e.get("published_parsed")
                pub_dt = None
                if pt:
                    try:
                        pub_dt = datetime(*pt[:6], tzinfo=timezone.utc)
                        if pub_dt < cutoff_reddit:
                            continue
                    except Exception:
                        pass
                prefix = title[:40].lower()
                if prefix in seen:
                    continue
                seen.add(prefix)
                all_items.append({
                    "title":       title[:150],
                    "source":      label,
                    "source_type": "reddit",
                    "link":        e.get("link", "#"),
                    "published":   e.get("published", ""),
                    "_pub_dt":     pub_dt,
                })
        except Exception as exc:
            app.logger.warning("Reddit RSS %s: %s", feed_url, exc)

    for feed_url, label in _BLOG_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for e in feed.entries:
                pt = e.get("published_parsed") or e.get("updated_parsed")
                pub_dt = None
                if pt:
                    try:
                        pub_dt = datetime(*pt[:6], tzinfo=timezone.utc)
                        if pub_dt < cutoff_blog:
                            continue
                    except Exception:
                        pass
                title = e.get("title", "").strip()
                if len(title) < 10:
                    continue
                prefix = title[:40].lower()
                if prefix in seen:
                    continue
                seen.add(prefix)
                all_items.append({
                    "title":       title[:150],
                    "source":      label,
                    "source_type": "blog",
                    "link":        e.get("link", "#"),
                    "published":   e.get("published") or e.get("updated", ""),
                    "_pub_dt":     pub_dt,
                })
        except Exception as exc:
            app.logger.warning("Blog RSS %s: %s", feed_url, exc)

    all_items.sort(
        key=lambda x: x["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    for item in all_items:
        item.pop("_pub_dt", None)
    return all_items[:count]


@app.route("/api/buzz")
def api_buzz():
    now = _time.time()
    if _BUZZ_CACHE["data"] and now - _BUZZ_CACHE["ts"] < _BUZZ_TTL:
        return jsonify({"ok": True, "data": _BUZZ_CACHE["data"]})
    try:
        data = {
            "finance_news": _fetch_finance_news(8),
            "pf_insights":  _fetch_pf_mixed(8),
            "global_news":  _fetch_rss_articles(_GLOBAL_NEWS_URLS, 6),
            "ts":           now,
        }
        _BUZZ_CACHE.update({"data": data, "ts": now})
        return jsonify({"ok": True, "data": data})
    except Exception as exc:
        app.logger.error("/api/buzz: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
