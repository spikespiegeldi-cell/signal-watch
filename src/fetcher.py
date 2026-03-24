#!/usr/bin/env python3
"""
Signal Watch — Stage 1: Data Fetcher
Fetches daily values from 28 upstream sources and updates 30-day rolling baselines.

Usage:
  python src/fetcher.py          # normal run
  python src/fetcher.py --test   # verify all source connections
"""

import os
import sys
import json
import yaml
import logging
import requests
import feedparser
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
BASELINES_PATH = BASE_DIR / "data" / "baselines.json"
HISTORY_DIR = BASE_DIR / "data" / "history"

with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

TODAY = date.today().isoformat()
HEADERS = {"User-Agent": "SignalWatch/1.0 (research; github.com/signal-watch)"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ── Baseline helpers ──────────────────────────────────────────────────────────

def load_baselines() -> dict:
    if BASELINES_PATH.exists():
        return json.loads(BASELINES_PATH.read_text())
    return {}


def save_baselines(b: dict):
    BASELINES_PATH.write_text(json.dumps(b, indent=2))


def update_rolling(b: dict, key: str, value: float, window: int = 30) -> float:
    """Append value to rolling window and return new average."""
    entry = b.get(key, {"values": [], "dates": []})
    vs = entry["values"] + [value]
    ds = entry["dates"] + [TODAY]
    vs, ds = vs[-window:], ds[-window:]
    avg = round(sum(vs) / len(vs), 4)
    b[key] = {"values": vs, "dates": ds, "avg30": avg}
    return avg


def get_avg(b: dict, key: str) -> Optional[float]:
    e = b.get(key)
    return e["avg30"] if e else None


def record(b: dict, key: str, value: Optional[float]) -> tuple:
    """Update baseline if value is available; return (today, avg30)."""
    if value is None:
        return None, get_avg(b, key)
    avg = update_rolling(b, key, value)
    return round(value, 4), avg


# ── Tech chain ────────────────────────────────────────────────────────────────

def fetch_arxiv(keywords: list) -> Optional[int]:
    """Count arXiv papers matching keywords submitted in the past 24h."""
    try:
        q = " OR ".join(f'"{kw}"' for kw in keywords)
        yd = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        td = date.today().strftime("%Y%m%d")
        url = (
            f"http://export.arxiv.org/api/query"
            f"?search_query=all:{requests.utils.quote(q)}"
            f"&submittedDate=[{yd}0000+TO+{td}2359]&max_results=1"
        )
        r = requests.get(url, timeout=30, headers=HEADERS)
        root = ET.fromstring(r.text)
        ns = {"os": "http://a9.com/-/spec/opensearch/1.1/"}
        total = root.find("os:totalResults", ns)
        return int(total.text) if total is not None else 0
    except Exception as e:
        log.warning(f"arXiv: {e}")
        return None


def fetch_hackernews(keywords: list) -> Optional[int]:
    """Count HN stories matching keywords via Algolia API."""
    try:
        count = 0
        for kw in keywords:
            r = requests.get(
                f"https://hn.algolia.com/api/v1/search"
                f"?query={requests.utils.quote(kw)}&tags=story&hitsPerPage=1",
                timeout=15
            ).json()
            count += r.get("nbHits", 0)
        return count
    except Exception as e:
        log.warning(f"HN: {e}")
        return None


def fetch_patents(companies: list) -> Optional[int]:
    """Count recent patent applications for tracked companies via USPTO PatentsView API."""
    try:
        count = 0
        yesterday = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")  # wider window for USPTO lag
        today_str = date.today().strftime("%Y-%m-%d")
        for company in companies[:3]:
            payload = {
                "q": {
                    "_and": [
                        {"_gte": {"patent_date": yesterday}},
                        {"_lte": {"patent_date": today_str}},
                        {"_contains": {"assignee_organization": company}}
                    ]
                },
                "f": ["patent_id"],
                "o": {"per_page": 1}
            }
            r = requests.post(
                "https://api.patentsview.org/patents/query",
                json=payload, timeout=20
            )
            if r.ok:
                count += r.json().get("total_patent_count", 0)
        return count
    except Exception as e:
        log.warning(f"Patents: {e}")
        return None


def fetch_crunchbase_tech() -> Optional[int]:
    """Count tech/AI funding news from Crunchbase RSS (past 24h)."""
    try:
        feed = feedparser.parse("https://news.crunchbase.com/feed/")
        cutoff = datetime.utcnow() - timedelta(days=1)
        keywords = ["funding", "raises", "seed", "Series A", "AI", "artificial intelligence", "machine learning"]
        recent = []
        for e in feed.entries:
            try:
                pub = datetime(*e.published_parsed[:6])
                text = e.get("title", "") + " " + e.get("summary", "")
                if pub > cutoff and any(k.lower() in text.lower() for k in keywords):
                    recent.append(e)
            except Exception:
                pass
        return len(recent)
    except Exception as e:
        log.warning(f"Crunchbase tech: {e}")
        return None


# ── Economy chain ─────────────────────────────────────────────────────────────

def fetch_fred(series_id: str) -> Optional[float]:
    """Fetch latest observation for a FRED data series."""
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        log.warning("FRED_API_KEY not set")
        return None
    try:
        r = requests.get(
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={api_key}&file_type=json"
            f"&sort_order=desc&limit=5",
            timeout=15
        ).json()
        for obs in r.get("observations", []):
            if obs["value"] != ".":
                return float(obs["value"])
        return None
    except Exception as e:
        log.warning(f"FRED {series_id}: {e}")
        return None


def fetch_marine_proxy() -> Optional[int]:
    """Shipping congestion proxy: count recent news articles mentioning Shanghai port."""
    try:
        feed = feedparser.parse(
            "https://news.google.com/rss/search"
            "?q=Shanghai+port+shipping+congestion&hl=en&gl=US&ceid=US:en"
        )
        cutoff = datetime.utcnow() - timedelta(days=7)
        recent = []
        for e in feed.entries:
            try:
                pub = datetime(*e.published_parsed[:6])
                if pub > cutoff:
                    recent.append(e)
            except Exception:
                pass
        return len(recent)
    except Exception as e:
        log.warning(f"Marine proxy: {e}")
        return None


def fetch_jobs_proxy(keywords: list) -> Optional[int]:
    """Job market proxy via Indeed RSS feed."""
    try:
        total = 0
        for kw in keywords[:2]:
            feed = feedparser.parse(
                f"https://www.indeed.com/rss?q={requests.utils.quote(kw)}&sort=date"
            )
            total += len(feed.entries)
        return total
    except Exception as e:
        log.warning(f"Jobs proxy: {e}")
        return None


# ── Biotech chain ─────────────────────────────────────────────────────────────

def fetch_nih(keywords: list) -> Optional[int]:
    """Count NIH grants matching keywords in the current fiscal year."""
    try:
        payload = {
            "criteria": {
                "advanced_text_search": {
                    "operator": "advanced",
                    "search_field": "all",
                    "search_text": " OR ".join(keywords)
                },
                "fiscal_years": [date.today().year]
            },
            "offset": 0,
            "limit": 1
        }
        r = requests.post(
            "https://api.reporter.nih.gov/v2/projects/search",
            json=payload, timeout=30
        ).json()
        return r.get("meta", {}).get("total", 0)
    except Exception as e:
        log.warning(f"NIH: {e}")
        return None


def fetch_biorxiv() -> Optional[int]:
    """Count bioRxiv preprints submitted in the past 24h."""
    try:
        yd = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        td = date.today().strftime("%Y-%m-%d")
        r = requests.get(
            f"https://api.biorxiv.org/details/biorxiv/{yd}/{td}/0/json",
            timeout=30
        ).json()
        msgs = r.get("messages", [{}])
        total = msgs[0].get("total", 0) if msgs else 0
        return int(total)
    except Exception as e:
        log.warning(f"bioRxiv: {e}")
        return None


def fetch_clinicaltrials(keywords: list) -> Optional[int]:
    """Count ClinicalTrials.gov studies started in the past 30 days matching keywords."""
    try:
        q = " OR ".join(keywords)
        cutoff = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://clinicaltrials.gov/api/v2/studies"
            f"?query.term={requests.utils.quote(q)}"
            f"&filter.advanced=AREA[StartDate]RANGE[{cutoff},MAX]"
            f"&pageSize=1",
            timeout=30, headers=HEADERS
        ).json()
        return r.get("totalCount", 0)
    except Exception as e:
        log.warning(f"ClinicalTrials: {e}")
        return None


def fetch_sec_efts(form_type: str, query: str) -> Optional[int]:
    """Count SEC EDGAR filings of a given type via full-text search."""
    try:
        yd = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        td = date.today().strftime("%Y-%m-%d")
        r = requests.get(
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q={requests.utils.quote(query)}"
            f"&dateRange=custom&startdt={yd}&enddt={td}"
            f"&forms={form_type}",
            timeout=30,
            headers={"User-Agent": "SignalWatch/1.0 research@example.com"}
        )
        return r.json().get("hits", {}).get("total", {}).get("value", 0)
    except Exception as e:
        log.warning(f"SEC EFTS {form_type}: {e}")
        return None


# ── Social chain ──────────────────────────────────────────────────────────────

def fetch_reddit(subreddits: list) -> Optional[float]:
    """Count posts across tracked subreddits in the past 24h."""
    client_id = os.environ.get("REDDIT_CLIENT_ID", "")
    client_secret = os.environ.get("REDDIT_SECRET", "")
    if not client_id or not client_secret:
        log.warning("Reddit credentials not set")
        return None
    try:
        token_r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": "SignalWatch/1.0"},
            timeout=15
        ).json()
        token = token_r.get("access_token")
        if not token:
            return None
        hdrs = {"User-Agent": "SignalWatch/1.0", "Authorization": f"bearer {token}"}
        total = 0
        cutoff = datetime.utcnow() - timedelta(hours=24)
        for sub in subreddits[:5]:
            name = sub.replace("r/", "")
            posts = requests.get(
                f"https://oauth.reddit.com/r/{name}/new?limit=25",
                headers=hdrs, timeout=15
            ).json()
            children = posts.get("data", {}).get("children", [])
            total += sum(
                1 for p in children
                if datetime.utcfromtimestamp(p["data"]["created_utc"]) > cutoff
            )
        return float(total)
    except Exception as e:
        log.warning(f"Reddit: {e}")
        return None


def fetch_trends(terms: list) -> Optional[float]:
    """Get average Google Trends interest score for tracked terms."""
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
        batch = terms[:5]
        pt.build_payload(batch, timeframe="now 7-d")
        df = pt.interest_over_time()
        if df.empty:
            return 0.0
        return float(df[batch].iloc[-1].mean())
    except Exception as e:
        log.warning(f"Google Trends: {e}")
        return None


def fetch_kickstarter() -> Optional[int]:
    """Count new Kickstarter projects from the past 24h via Atom feed."""
    try:
        feed = feedparser.parse("https://www.kickstarter.com/projects/feed.atom")
        cutoff = datetime.utcnow() - timedelta(hours=24)
        recent = []
        for e in feed.entries:
            try:
                pub = datetime(*e.published_parsed[:6])
                if pub > cutoff:
                    recent.append(e)
            except Exception:
                pass
        return len(recent)
    except Exception as e:
        log.warning(f"Kickstarter: {e}")
        return None


def fetch_amazon_movers() -> Optional[int]:
    """Scrape Amazon Movers & Shakers books page for item count."""
    try:
        hdrs = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        r = requests.get(
            "https://www.amazon.com/gp/movers-and-shakers/books/",
            headers=hdrs, timeout=30
        )
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("li.zg-item-immersion") or soup.select("[data-asin]")
        return len(items) if items else None
    except Exception as e:
        log.warning(f"Amazon Movers: {e}")
        return None


# ── Geopolitics chain ─────────────────────────────────────────────────────────

def fetch_commodity_price(commodity: str) -> Optional[float]:
    """Scrape commodity spot price from TradingEconomics."""
    try:
        slug = commodity.lower()
        url = f"https://tradingeconomics.com/commodity/{slug}"
        hdrs = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        r = requests.get(url, headers=hdrs, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for sel in ["#p", ".display-data", "[id='p']", ".act-price", "span#p"]:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True).replace(",", "").replace("$", "").strip()
                try:
                    return float(text)
                except ValueError:
                    pass
        # Fallback: parse price from page title or meta description
        import re
        meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
        if meta:
            nums = re.findall(r"\b\d+(?:\.\d+)?\b", meta.get("content", ""))
            if nums:
                return float(nums[0])
        return None
    except Exception as e:
        log.warning(f"Commodity {commodity}: {e}")
        return None


def fetch_eu_consultations() -> Optional[int]:
    """Count currently open EU Better Regulation consultations."""
    try:
        r = requests.get(
            "https://ec.europa.eu/info/law/better-regulation/brpapi/searchInitiatives"
            "?status=OPEN&size=1&page=0",
            timeout=30, headers=HEADERS
        )
        return r.json().get("totalElements", 0)
    except Exception as e:
        log.warning(f"EU consultations: {e}")
        return None


def fetch_congress_hearings() -> Optional[int]:
    """Count congressional committee meetings in the past 7 days via Congress.gov API."""
    try:
        params = {
            "fromDateTime": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z"),
            "toDateTime": datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z"),
            "limit": 20,
            "format": "json"
        }
        api_key = os.environ.get("CONGRESS_API_KEY", "")
        if api_key:
            params["api_key"] = api_key
        r = requests.get(
            "https://api.congress.gov/v3/committee-meeting",
            params=params, timeout=30, headers=HEADERS
        )
        return len(r.json().get("committeeMeetings", []))
    except Exception as e:
        log.warning(f"Congress: {e}")
        return None


# ── Corporate chain ───────────────────────────────────────────────────────────

def fetch_sec_form4(companies: list) -> Optional[int]:
    """Count SEC Form 4 insider filings for watchlist companies."""
    query = " OR ".join(f'"{c}"' for c in companies[:15])
    return fetch_sec_efts("4", query)


def fetch_sec_8k(companies: list) -> Optional[int]:
    """Count SEC 8-K material event filings for watchlist companies."""
    query = " OR ".join(f'"{c}"' for c in companies[:15])
    return fetch_sec_efts("8-K", query)


# ── Energy chain ──────────────────────────────────────────────────────────────

def fetch_arpa_e() -> Optional[int]:
    """Count ARPA-E press releases from the past 30 days."""
    try:
        # Try RSS first
        feed = feedparser.parse("https://arpa-e.energy.gov/news-and-media/press-releases/feed")
        if feed.entries:
            cutoff = datetime.utcnow() - timedelta(days=30)
            recent = []
            for e in feed.entries:
                try:
                    pub = datetime(*e.published_parsed[:6])
                    if pub > cutoff:
                        recent.append(e)
                except Exception:
                    pass
            return len(recent)
        # Fallback: scrape press releases page
        r = requests.get(
            "https://arpa-e.energy.gov/news-and-media/press-releases",
            timeout=30, headers=HEADERS
        )
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("article") or soup.select(".views-row") or soup.select(".node--type-press-release")
        return len(items) if items else None
    except Exception as e:
        log.warning(f"ARPA-E: {e}")
        return None


def fetch_arxiv_physics() -> Optional[int]:
    """Count arXiv physics.app-ph preprints submitted in the past 24h."""
    try:
        yd = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        td = date.today().strftime("%Y%m%d")
        url = (
            f"http://export.arxiv.org/api/query"
            f"?search_query=cat:physics.app-ph"
            f"&submittedDate=[{yd}0000+TO+{td}2359]&max_results=1"
        )
        r = requests.get(url, timeout=30, headers=HEADERS)
        root = ET.fromstring(r.text)
        ns = {"os": "http://a9.com/-/spec/opensearch/1.1/"}
        total = root.find("os:totalResults", ns)
        return int(total.text) if total is not None else 0
    except Exception as e:
        log.warning(f"arXiv physics: {e}")
        return None


def fetch_crunchbase_energy() -> Optional[int]:
    """Count energy-sector funding news from Crunchbase RSS (past 24h)."""
    try:
        feed = feedparser.parse("https://news.crunchbase.com/feed/")
        cutoff = datetime.utcnow() - timedelta(days=1)
        keywords = [
            "energy", "battery", "solar", "wind", "nuclear",
            "lithium", "EV", "clean energy", "grid", "storage"
        ]
        recent = []
        for e in feed.entries:
            try:
                pub = datetime(*e.published_parsed[:6])
                text = e.get("title", "") + " " + e.get("summary", "")
                if pub > cutoff and any(k.lower() in text.lower() for k in keywords):
                    recent.append(e)
            except Exception:
                pass
        return len(recent)
    except Exception as e:
        log.warning(f"Crunchbase energy: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    test_mode = "--test" in sys.argv
    if test_mode:
        log.info("=== TEST MODE: verifying source connections ===")

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / "data").mkdir(parents=True, exist_ok=True)

    b = load_baselines()
    cfg = CONFIG
    result = {"date": TODAY, "chains": {}}

    # ── Tech ──────────────────────────────────────────────────────────────────
    log.info("Fetching Tech chain...")
    t_arxiv,  t_arxiv_avg  = record(b, "tech_arxiv",      fetch_arxiv(cfg["tech_chain"]["arxiv_keywords"]))
    t_hn,     t_hn_avg     = record(b, "tech_hn",         fetch_hackernews(cfg["tech_chain"]["hacker_news_keywords"]))
    t_pat,    t_pat_avg    = record(b, "tech_patents",    fetch_patents(cfg["tech_chain"]["patent_companies"]))
    t_cb,     t_cb_avg     = record(b, "tech_crunchbase", fetch_crunchbase_tech())
    result["chains"]["tech"] = {
        "arxiv":       {"today": t_arxiv,  "avg30": t_arxiv_avg,  "label": "arXiv AI papers (24h)"},
        "hacker_news": {"today": t_hn,     "avg30": t_hn_avg,     "label": "HN stories matching keywords"},
        "patents":     {"today": t_pat,    "avg30": t_pat_avg,    "label": "USPTO patent filings (24h)"},
        "crunchbase":  {"today": t_cb,     "avg30": t_cb_avg,     "label": "Tech funding news items (24h)"},
    }

    # ── Economy ───────────────────────────────────────────────────────────────
    log.info("Fetching Economy chain...")
    e_yield, e_yield_avg  = record(b, "econ_yield",  fetch_fred(cfg["economy_chain"]["fred_series"]["yield_curve"]))
    e_box,   e_box_avg    = record(b, "econ_box",    fetch_fred(cfg["economy_chain"]["fred_series"]["box_production"]))
    e_mar,   e_mar_avg    = record(b, "econ_marine", fetch_marine_proxy())
    e_jobs,  e_jobs_avg   = record(b, "econ_jobs",   fetch_jobs_proxy(cfg["economy_chain"]["linkedin_keywords"]))
    result["chains"]["economy"] = {
        "yield_curve":    {"today": e_yield, "avg30": e_yield_avg, "label": "FRED T10Y2Y yield spread"},
        "box_production": {"today": e_box,   "avg30": e_box_avg,   "label": "FRED cardboard box production index"},
        "marine_traffic": {"today": e_mar,   "avg30": e_mar_avg,   "label": "Shanghai port congestion news (7d)"},
        "linkedin_jobs":  {"today": e_jobs,  "avg30": e_jobs_avg,  "label": "Job market proxy (Indeed RSS)"},
    }

    # ── Biotech ───────────────────────────────────────────────────────────────
    log.info("Fetching Biotech chain...")
    bio_nih,  bio_nih_avg  = record(b, "bio_nih",    fetch_nih(cfg["biotech_chain"]["nih_keywords"]))
    bio_bxiv, bio_bxiv_avg = record(b, "bio_biorxiv", fetch_biorxiv())
    bio_ct,   bio_ct_avg   = record(b, "bio_clin",   fetch_clinicaltrials(cfg["biotech_chain"]["clinicaltrials_keywords"]))
    bio_sec,  bio_sec_avg  = record(b, "bio_sec",    fetch_sec_efts("S-1", " OR ".join(cfg["biotech_chain"]["sec_biotech_keywords"])))
    result["chains"]["biotech"] = {
        "nih_reporter":   {"today": bio_nih,  "avg30": bio_nih_avg,  "label": "NIH grants this FY"},
        "biorxiv":        {"today": bio_bxiv, "avg30": bio_bxiv_avg, "label": "bioRxiv preprints (24h)"},
        "clinical_trials":{"today": bio_ct,   "avg30": bio_ct_avg,   "label": "ClinicalTrials.gov active trials (30d)"},
        "sec_s1_biotech": {"today": bio_sec,  "avg30": bio_sec_avg,  "label": "SEC S-1 biotech filings (24h)"},
    }

    # ── Social ────────────────────────────────────────────────────────────────
    log.info("Fetching Social chain...")
    soc_reddit, soc_reddit_avg = record(b, "soc_reddit",  fetch_reddit(cfg["social_chain"]["subreddits"]))
    soc_trends, soc_trends_avg = record(b, "soc_trends",  fetch_trends(cfg["social_chain"]["google_trends_terms"]))
    soc_ks,     soc_ks_avg     = record(b, "soc_ks",      fetch_kickstarter())
    soc_amz,    soc_amz_avg    = record(b, "soc_amazon",  fetch_amazon_movers())
    result["chains"]["social"] = {
        "reddit":        {"today": soc_reddit, "avg30": soc_reddit_avg, "label": "Tracked subreddit post velocity (24h)"},
        "google_trends": {"today": soc_trends, "avg30": soc_trends_avg, "label": "Google Trends avg interest (7d)"},
        "kickstarter":   {"today": soc_ks,     "avg30": soc_ks_avg,     "label": "New Kickstarter projects (24h)"},
        "amazon_movers": {"today": soc_amz,    "avg30": soc_amz_avg,    "label": "Amazon Movers & Shakers item count"},
    }

    # ── Geopolitics ───────────────────────────────────────────────────────────
    log.info("Fetching Geopolitics chain...")
    geo_comms = cfg["geopolitics_chain"]["commodities"]
    geo_prices = [fetch_commodity_price(c) for c in geo_comms[:3]]
    valid_prices = [p for p in geo_prices if p is not None]
    avg_price = sum(valid_prices) / len(valid_prices) if valid_prices else None
    geo_price_today, geo_price_avg = record(b, "geo_commodities", avg_price)
    geo_mar2, geo_mar2_avg   = record(b, "geo_marine",  fetch_marine_proxy())
    geo_eu,   geo_eu_avg     = record(b, "geo_eu",      fetch_eu_consultations())
    geo_cong, geo_cong_avg   = record(b, "geo_congress", fetch_congress_hearings())
    result["chains"]["geopolitics"] = {
        "commodities":      {"today": geo_price_today, "avg30": geo_price_avg, "label": f"Avg price: {', '.join(geo_comms[:3])}"},
        "marine_traffic":   {"today": geo_mar2,  "avg30": geo_mar2_avg,  "label": "Shipping congestion news (7d)"},
        "eu_consultations": {"today": geo_eu,    "avg30": geo_eu_avg,    "label": "EU open consultations"},
        "congress_hearings":{"today": geo_cong,  "avg30": geo_cong_avg,  "label": "Congressional hearings (7d)"},
    }

    # ── Corporate ─────────────────────────────────────────────────────────────
    log.info("Fetching Corporate chain...")
    watchlist = cfg["corporate_chain"]["company_watchlist"]
    corp_f4,   corp_f4_avg   = record(b, "corp_form4",   fetch_sec_form4(watchlist))
    corp_jobs, corp_jobs_avg = record(b, "corp_jobs",    fetch_jobs_proxy(watchlist))
    corp_pat,  corp_pat_avg  = record(b, "corp_patents", fetch_patents(watchlist))
    corp_8k,   corp_8k_avg   = record(b, "corp_8k",      fetch_sec_8k(watchlist))
    result["chains"]["corporate"] = {
        "sec_form4":  {"today": corp_f4,   "avg30": corp_f4_avg,   "label": "SEC Form 4 insider filings (24h)"},
        "jobs_proxy": {"today": corp_jobs, "avg30": corp_jobs_avg, "label": "Job posting proxy for watchlist"},
        "patents":    {"today": corp_pat,  "avg30": corp_pat_avg,  "label": "Patent filings by watchlist cos (24h)"},
        "sec_8k":     {"today": corp_8k,   "avg30": corp_8k_avg,   "label": "SEC 8-K filings by watchlist (24h)"},
    }

    # ── Energy ────────────────────────────────────────────────────────────────
    log.info("Fetching Energy chain...")
    en_arpa,  en_arpa_avg  = record(b, "en_arpa",        fetch_arpa_e())
    en_phys,  en_phys_avg  = record(b, "en_physics",     fetch_arxiv_physics())
    en_comms = cfg["energy_chain"]["commodities"]
    en_prices = [fetch_commodity_price(c) for c in en_comms[:3]]
    valid_en = [p for p in en_prices if p is not None]
    avg_en = sum(valid_en) / len(valid_en) if valid_en else None
    en_price_today, en_price_avg = record(b, "en_commodities", avg_en)
    en_cb,    en_cb_avg    = record(b, "en_crunchbase",  fetch_crunchbase_energy())
    result["chains"]["energy"] = {
        "arpa_e":           {"today": en_arpa,        "avg30": en_arpa_avg,  "label": "ARPA-E press releases (30d)"},
        "arxiv_physics":    {"today": en_phys,        "avg30": en_phys_avg,  "label": "arXiv physics.app-ph preprints (24h)"},
        "energy_commodities":{"today": en_price_today,"avg30": en_price_avg, "label": f"Avg price: {', '.join(en_comms[:3])}"},
        "crunchbase_energy":{"today": en_cb,          "avg30": en_cb_avg,    "label": "Energy funding news (24h)"},
    }

    # ── Save outputs ──────────────────────────────────────────────────────────
    save_baselines(b)

    out_path = HISTORY_DIR / f"raw_data_{TODAY}.json"
    out_path.write_text(json.dumps(result, indent=2))
    log.info(f"Raw data saved → {out_path}")

    if test_mode:
        log.info("=== Source connection summary ===")
        total_connected = 0
        total_sources = 0
        for chain_name, sources in result["chains"].items():
            connected = sum(1 for v in sources.values() if v["today"] is not None)
            total = len(sources)
            total_connected += connected
            total_sources += total
            status = "OK" if connected == total else f"PARTIAL ({connected}/{total})"
            log.info(f"  {chain_name:15s}: {status}")
        log.info(f"  TOTAL: {total_connected}/{total_sources} sources connected")

    return result


if __name__ == "__main__":
    main()
