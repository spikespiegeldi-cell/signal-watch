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

def fetch_arxiv(keywords: list) -> Optional[float]:
    """7-day moving average of arXiv papers matching keywords (total past 7 days / 7)."""
    try:
        q = " OR ".join(f'"{kw}"' for kw in keywords)
        start = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        td = date.today().strftime("%Y%m%d")
        url = (
            f"http://export.arxiv.org/api/query"
            f"?search_query=all:{requests.utils.quote(q)}"
            f"&submittedDate=[{start}0000+TO+{td}2359]&max_results=1"
        )
        r = requests.get(url, timeout=30, headers=HEADERS)
        root = ET.fromstring(r.text)
        ns = {"os": "http://a9.com/-/spec/opensearch/1.1/"}
        total = root.find("os:totalResults", ns)
        count = int(total.text) if total is not None else 0
        return round(count / 7, 1)
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


def fetch_patents(companies: list) -> Optional[float]:
    """7-day moving average of patent applications for tracked companies (total past 7 days / 7)."""
    try:
        count = 0
        start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        today_str = date.today().strftime("%Y-%m-%d")
        for company in companies[:3]:
            payload = {
                "q": {
                    "_and": [
                        {"_gte": {"patent_date": start}},
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
        return round(count / 7, 1)
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


def fetch_layoffs_fyi(sectors: list) -> tuple:
    """Count layoff news mentions for tracked sectors via Google News RSS (24h window).

    layoffs.fyi embeds its data via JavaScript / Airtable and is not
    directly scrapeable, so this function uses Google News RSS as a
    proxy: it searches for "<sector> layoffs" news items published in
    the last 24 hours and counts distinct events as a RISING/FLAT/DROPPING
    proxy signal.

    Returns (event_count, summary_str).
    event_count = number of distinct layoff news items in the last 24 hours.
    summary_str = per-sector breakdown for secondary_signals.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(hours=24)
        per_sector = {}

        for sector in sectors:
            query = requests.utils.quote(f"{sector} layoffs")
            url = (
                f"https://news.google.com/rss/search"
                f"?q={query}&hl=en-US&gl=US&ceid=US:en"
            )
            try:
                feed = feedparser.parse(url)
                count = 0
                for entry in feed.entries:
                    # Parse published time
                    published = entry.get("published_parsed") or entry.get("updated_parsed")
                    if published is None:
                        count += 1  # include if we can't parse date
                        continue
                    entry_dt = datetime(*published[:6])
                    if entry_dt >= cutoff:
                        count += 1
                per_sector[sector] = count
            except Exception as e:
                log.warning(f"Layoffs news ({sector}): {e}")
                per_sector[sector] = None

        valid_counts = [v for v in per_sector.values() if v is not None]
        total = sum(valid_counts) if valid_counts else None

        parts = []
        for sector, cnt in per_sector.items():
            parts.append(f"{sector}: {cnt if cnt is not None else 'N/A'} items")
        summary = "Layoff news (24h): " + "; ".join(parts) if parts else "N/A"

        return float(total) if total is not None else None, summary

    except Exception as e:
        log.warning(f"Layoffs news: {e}")
        return None, f"N/A (fetch error: {str(e)[:60]})"


def fetch_adzuna_sector_hiring(sectors: dict, app_id: str, app_key: str, b: dict) -> tuple:
    """Query Adzuna for total live job postings per sector using keyword search.

    sectors: dict of {keyword_search_terms: display_label}
    The key is passed as Adzuna's `what` (keyword) parameter — no category slugs needed.
    Returns (total_today, summary_str).
    """
    if not app_id or not app_key:
        log.warning("Adzuna credentials not set — skipping sector hiring signal. "
                    "Register at https://developer.adzuna.com/ to enable this source.")
        return None, "N/A (adzuna credentials not configured — register at https://developer.adzuna.com/)"

    parts = []
    total = 0
    successful = 0

    for search_terms, display_label in sectors.items():
        try:
            params = {
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": 1,
                "what": search_terms,
            }
            r = requests.get(
                "https://api.adzuna.com/v1/api/jobs/us/search/1",
                params=params, timeout=15,
                headers={**HEADERS, "Accept": "application/json"}
            )
            r.raise_for_status()
            count = r.json().get("count", 0)

            baseline_key = f"adzuna_sector_{display_label.lower().replace('/', '_').replace(' ', '_')}"
            avg = update_rolling(b, baseline_key, float(count))
            pct_chg = round((count - avg) / avg * 100, 1) if avg else 0.0
            sign = "+" if pct_chg >= 0 else ""
            parts.append(f"{display_label}: {count:,} postings (avg30: {avg:,.0f}, {sign}{pct_chg}%)")
            total += count
            successful += 1
        except Exception as e:
            parts.append(f"{display_label}: [skipped: {str(e)[:60]}]")
            log.warning(f"Adzuna sector '{display_label}': {e}")

    summary = "; ".join(parts) if parts else "N/A"
    total_val = float(total) if successful > 0 else None
    return total_val, summary


def fetch_jobs_per_company(ats_map: dict, keywords: list, b: dict) -> tuple:
    """Fetch keyword-matched job postings per company via Greenhouse, Lever, or Adzuna.

    Returns (total_today, per_company_str).
    total_today is the sum of matched counts for companies that responded.
    per_company_str is a line-per-company breakdown for secondary_signals.
    """
    adzuna_app_id  = os.environ.get("ADZUNA_APP_ID", "")
    adzuna_app_key = os.environ.get("ADZUNA_APP_KEY", "")
    kws_lower = [kw.lower() for kw in keywords]

    parts = []
    total = 0
    successful = 0

    for company, entry in ats_map.items():
        provider = entry.get("provider", "adzuna")
        slug     = entry.get("slug", "")
        count    = None
        skip_reason = None

        if provider == "greenhouse":
            try:
                url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
                r = requests.get(url, timeout=15, headers=HEADERS)
                r.raise_for_status()
                jobs = r.json().get("jobs", [])
                matched = 0
                for job in jobs:
                    title = job.get("title", "").lower()
                    dept  = (job.get("departments") or [{}])[0].get("name", "").lower()
                    if any(kw in title or kw in dept for kw in kws_lower):
                        matched += 1
                count = matched
            except Exception as e:
                skip_reason = str(e)[:80]

        elif provider == "lever":
            try:
                url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
                r = requests.get(url, timeout=15, headers=HEADERS)
                r.raise_for_status()
                jobs = r.json()
                if not isinstance(jobs, list):
                    jobs = []
                matched = 0
                for job in jobs:
                    text = job.get("text", "").lower()
                    team = (job.get("categories") or {}).get("team", "").lower()
                    if any(kw in text or kw in team for kw in kws_lower):
                        matched += 1
                count = matched
            except Exception as e:
                skip_reason = str(e)[:80]

        elif provider == "adzuna":
            if not adzuna_app_id or not adzuna_app_key:
                skip_reason = "adzuna credentials not configured"
            else:
                try:
                    params = {
                        "app_id": adzuna_app_id,
                        "app_key": adzuna_app_key,
                        "employer": company,
                        "what": " ".join(keywords[:3]),
                        "results_per_page": 50,
                    }
                    r = requests.get(
                        "https://api.adzuna.com/v1/api/jobs/us/search/1",
                        params=params, timeout=15, headers=HEADERS
                    )
                    r.raise_for_status()
                    count = r.json().get("count", 0)
                except Exception as e:
                    skip_reason = str(e)[:80]
        else:
            skip_reason = f"unknown provider {provider}"

        slug_key     = company.lower().replace(" ", "_").replace("/", "_")
        baseline_key = f"jobs_{slug_key}"

        if count is not None:
            avg = update_rolling(b, baseline_key, float(count))
            total     += count
            successful += 1
            parts.append(f"{company}: {count} (avg30: {avg:.1f})")
        else:
            avg = get_avg(b, baseline_key)
            avg_str = f"{avg:.1f}" if avg is not None else "N/A"
            parts.append(f"{company}: [skipped: {skip_reason}] (avg30: {avg_str})")

    per_company_str = "\n".join(parts) if parts else "N/A"
    total_val = float(total) if successful > 0 else None
    return total_val, per_company_str


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


def fetch_biorxiv() -> Optional[float]:
    """7-day moving average of bioRxiv preprints (total past 7 days / 7)."""
    try:
        start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        td = date.today().strftime("%Y-%m-%d")
        r = requests.get(
            f"https://api.biorxiv.org/details/biorxiv/{start}/{td}/0/json",
            timeout=30
        ).json()
        msgs = r.get("messages", [{}])
        total = msgs[0].get("total", 0) if msgs else 0
        return round(int(total) / 7, 1)
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

def fetch_bluesky(keywords: list) -> Optional[int]:
    """Count Bluesky posts matching keywords in the past 24h. No auth required."""
    try:
        since = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        total = 0
        for kw in keywords[:5]:
            r = requests.get(
                "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
                params={"q": kw, "limit": 1, "since": since},
                timeout=15, headers=HEADERS
            ).json()
            total += r.get("hitsTotal", 0)
        return total
    except Exception as e:
        log.warning(f"Bluesky: {e}")
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


def fetch_arxiv_physics() -> Optional[float]:
    """7-day moving average of arXiv physics.app-ph preprints (total past 7 days / 7)."""
    try:
        start = (date.today() - timedelta(days=7)).strftime("%Y%m%d")
        td = date.today().strftime("%Y%m%d")
        url = (
            f"http://export.arxiv.org/api/query"
            f"?search_query=cat:physics.app-ph"
            f"&submittedDate=[{start}0000+TO+{td}2359]&max_results=1"
        )
        r = requests.get(url, timeout=30, headers=HEADERS)
        root = ET.fromstring(r.text)
        ns = {"os": "http://a9.com/-/spec/opensearch/1.1/"}
        total = root.find("os:totalResults", ns)
        count = int(total.text) if total is not None else 0
        return round(count / 7, 1)
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


# ── Secondary signals (Yahoo Finance) ────────────────────────────────────────

def fetch_short_interest_all(ticker_map: dict, b: dict) -> str:
    """Fetch short interest % of float for each listed watchlist ticker via yfinance.
    Issue 2: tracks skipped tickers with reasons.
    Issue 3: falls back to sharesShort/sharesOutstanding, then shortRatio if shortPercentOfFloat is None.
    Returns a formatted string summarising today vs 30d average per ticker."""
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed — skipping short interest")
        return "N/A (yfinance not installed)"

    parts = []
    skipped = []
    for company, ticker in ticker_map.items():
        try:
            info = yf.Ticker(ticker).info
            raw_pct = info.get("shortPercentOfFloat")
            is_ratio = False

            # Issue 3: fallback chain when shortPercentOfFloat is None
            if raw_pct is None:
                shares_short = info.get("sharesShort") or 0
                shares_out = info.get("sharesOutstanding")
                if shares_out and shares_out > 0 and shares_short:
                    raw_pct = shares_short / shares_out  # compute as fraction, same unit
                elif info.get("shortRatio") is not None:
                    raw_pct = info.get("shortRatio")
                    is_ratio = True
                else:
                    skipped.append(f"{ticker} short interest data unavailable")
                    continue

            if not is_ratio:
                today_val, avg_val = record(b, f"secondary_short_{ticker}", round(float(raw_pct) * 100, 2))
                if today_val is None:
                    skipped.append(f"{ticker} no data returned")
                    continue
                if avg_val and avg_val > 0:
                    chg = round((today_val - avg_val) / avg_val * 100)
                    direction = f"+{chg}%" if chg >= 0 else f"{chg}%"
                    parts.append(f"{ticker}: {today_val:.1f}% float (avg {avg_val:.1f}%, {direction})")
                else:
                    parts.append(f"{ticker}: {today_val:.1f}% float (no baseline yet)")
            else:
                # shortRatio (days-to-cover) — record separately, note as ratio not percentage
                today_val, avg_val = record(b, f"secondary_short_{ticker}", round(float(raw_pct), 2))
                if today_val is None:
                    skipped.append(f"{ticker} no data returned")
                    continue
                if avg_val and avg_val > 0:
                    chg = round((today_val - avg_val) / avg_val * 100)
                    direction = f"+{chg}%" if chg >= 0 else f"{chg}%"
                    parts.append(f"{ticker}: short ratio {today_val:.1f}d (avg {avg_val:.1f}d, {direction})")
                else:
                    parts.append(f"{ticker}: short ratio {today_val:.1f}d (no baseline yet)")

        except Exception as e:
            log.warning(f"Short interest {ticker}: {e}")
            skipped.append(f"{ticker} {str(e)[:60]}")

    result_str = "; ".join(parts)
    if skipped:
        result_str += f" [skipped: {', '.join(skipped)}]"
    return result_str if result_str else "N/A (no data returned)"


def fetch_options_activity_all(ticker_map: dict, b: dict) -> str:
    """Fetch call/put open interest ratio for each listed watchlist ticker via yfinance.
    Uses the nearest 3 option expiration dates for a stable ratio.
    Issue 2: tracks skipped tickers with reasons.
    Returns a formatted string summarising today vs 30d average per ticker."""
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed — skipping options activity")
        return "N/A (yfinance not installed)"

    parts = []
    skipped = []
    for company, ticker in ticker_map.items():
        try:
            t = yf.Ticker(ticker)
            expirations = t.options
            if not expirations:
                skipped.append(f"{ticker} no options listed")
                continue
            total_call_oi = 0
            total_put_oi = 0
            for exp in expirations[:3]:
                chain = t.option_chain(exp)
                total_call_oi += chain.calls["openInterest"].fillna(0).sum()
                total_put_oi += chain.puts["openInterest"].fillna(0).sum()
            if total_put_oi == 0:
                skipped.append(f"{ticker} no put open interest")
                continue
            ratio = round(total_call_oi / total_put_oi, 2)
            today_val, avg_val = record(b, f"secondary_options_{ticker}", ratio)
            if today_val is None:
                skipped.append(f"{ticker} no data returned")
                continue
            if avg_val and avg_val > 0:
                chg = round((today_val - avg_val) / avg_val * 100)
                direction = f"+{chg}%" if chg >= 0 else f"{chg}%"
                parts.append(f"{ticker}: C/P {today_val:.2f} (avg {avg_val:.2f}, {direction})")
            else:
                parts.append(f"{ticker}: C/P {today_val:.2f} (no baseline yet)")
        except Exception as e:
            log.warning(f"Options activity {ticker}: {e}")
            skipped.append(f"{ticker} {str(e)[:60]}")

    result_str = "; ".join(parts)
    if skipped:
        result_str += f" [skipped: {', '.join(skipped)}]"
    return result_str if result_str else "N/A (no data returned)"


def fetch_form4_per_company(ticker_map: dict) -> str:
    """Fetch per-company insider transactions from yfinance for the last 7 days.
    Purchases by executives/directors are Signal B evidence; sales are not.
    Returns a formatted string with one entry per ticker."""
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        log.warning("yfinance/pandas not installed — skipping Form 4 per-company")
        return "N/A (yfinance not installed)"

    cutoff = date.today() - timedelta(days=7)
    parts = []
    for company, ticker in ticker_map.items():
        try:
            df = yf.Ticker(ticker).insider_transactions
            if df is None or (hasattr(df, "empty") and df.empty):
                parts.append(f"{ticker}: data unavailable")
                continue

            # Detect date column (varies by yfinance version)
            date_col = next((c for c in ["Start Date", "Date", "Transaction Date"] if c in df.columns), None)
            if date_col is None:
                parts.append(f"{ticker}: data unavailable (unrecognised schema)")
                continue

            # Detect transaction type column
            type_col = next((c for c in ["Transaction", "Type"] if c in df.columns), None)

            # Parse dates and filter to last 7 days
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            recent = df[df[date_col].dt.date >= cutoff]

            if recent.empty:
                parts.append(f"{ticker}: no insider transactions (7d)")
                continue

            # Filter for purchases only — sales are not Signal B evidence
            if type_col:
                purchases = recent[
                    recent[type_col].astype(str).str.lower().str.contains(r"buy|purchase", na=False)
                ]
            else:
                # No type column: use Text column as fallback
                text_col = "Text" if "Text" in recent.columns else None
                if text_col:
                    purchases = recent[
                        recent[text_col].astype(str).str.lower().str.contains(r"buy|purchase", na=False)
                    ]
                else:
                    purchases = pd.DataFrame()

            if purchases.empty:
                parts.append(f"{ticker}: no insider purchases (7d)")
                continue

            # Most recent purchase
            row = purchases.sort_values(date_col, ascending=False).iloc[0]
            tx_date = row[date_col].strftime("%Y-%m-%d")
            position = str(row.get("Position", row.get("Title", "Insider"))).strip()
            insider_name = str(row.get("Insider", row.get("Name", ""))).strip()
            shares_raw = row.get("Shares", row.get("shares", None))

            try:
                shares_fmt = f"{int(float(shares_raw)):,}"
            except (TypeError, ValueError):
                shares_fmt = None

            who = f"{position} {insider_name}".strip()
            if shares_fmt:
                parts.append(f"{ticker}: {who} bought {shares_fmt} shares on {tx_date}")
            else:
                parts.append(f"{ticker}: {who} insider purchase on {tx_date} (share count N/A)")

        except Exception as e:
            log.warning(f"Form4 per-company {ticker}: {e}")
            parts.append(f"{ticker}: data unavailable ({str(e)[:60]})")

    return "; ".join(parts) if parts else "N/A (no data returned)"


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
    adzuna_app_id  = os.environ.get("ADZUNA_APP_ID", "")
    adzuna_app_key = os.environ.get("ADZUNA_APP_KEY", "")

    e_yield, e_yield_avg  = record(b, "econ_yield",  fetch_fred(cfg["economy_chain"]["fred_series"]["yield_curve"]))
    e_box,   e_box_avg    = record(b, "econ_box",    fetch_fred(cfg["economy_chain"]["fred_series"]["box_production"]))
    e_mar,   e_mar_avg    = record(b, "econ_marine", fetch_marine_proxy())

    layoffs_today, layoffs_summary_str = fetch_layoffs_fyi(
        cfg["economy_chain"].get("layoffs_fyi_sectors", []))
    e_layoffs, e_layoffs_avg = record(b, "econ_layoffs", layoffs_today)

    hiring_today, sector_hiring_summary_str = fetch_adzuna_sector_hiring(
        cfg["economy_chain"].get("adzuna_sector_categories", {}),
        adzuna_app_id, adzuna_app_key, b)
    e_hiring, e_hiring_avg = record(b, "econ_hiring", hiring_today)

    result["chains"]["economy"] = {
        "yield_curve":    {"today": e_yield,   "avg30": e_yield_avg,   "label": "FRED T10Y2Y yield spread"},
        "box_production": {"today": e_box,     "avg30": e_box_avg,     "label": "FRED cardboard box production index"},
        "layoffs":        {"today": e_layoffs, "avg30": e_layoffs_avg, "label": "Sector layoff news items (24h Google News)"},
        "sector_hiring":  {"today": e_hiring,  "avg30": e_hiring_avg,  "label": "Sector job posting volume (Adzuna)"},
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
    soc_bsky,   soc_bsky_avg   = record(b, "soc_bluesky", fetch_bluesky(cfg["social_chain"]["bluesky_keywords"]))
    soc_trends, soc_trends_avg = record(b, "soc_trends",  fetch_trends(cfg["social_chain"]["google_trends_terms"]))
    soc_ks,     soc_ks_avg     = record(b, "soc_ks",      fetch_kickstarter())
    soc_amz,    soc_amz_avg    = record(b, "soc_amazon",  fetch_amazon_movers())
    result["chains"]["social"] = {
        "bluesky":       {"today": soc_bsky,   "avg30": soc_bsky_avg,   "label": "Bluesky posts matching keywords (24h)"},
        "google_trends": {"today": soc_trends, "avg30": soc_trends_avg, "label": "Google Trends avg interest (7d)"},
        "kickstarter":   {"today": soc_ks,     "avg30": soc_ks_avg,     "label": "New Kickstarter projects (24h)"},
        "amazon_movers": {"today": soc_amz,    "avg30": soc_amz_avg,    "label": "Amazon Movers & Shakers item count"},
    }

    # ── Geopolitics ───────────────────────────────────────────────────────────
    log.info("Fetching Geopolitics chain...")
    geo_comms = cfg["geopolitics_chain"]["commodities"]
    geo_prices = [fetch_commodity_price(c) for c in geo_comms]
    # Track each commodity against its own 30d baseline, then report % that are rising.
    # This avoids cancellation from incomparable price units (USD/barrel vs USD/oz etc).
    rising_count = 0
    valid_count = 0
    for commodity, price in zip(geo_comms, geo_prices):
        if price is None:
            continue
        key = f"geo_comm_{commodity.replace('-', '_')}"
        today_val, avg_val = record(b, key, price)
        valid_count += 1
        if avg_val and avg_val > 0 and today_val >= avg_val:
            rising_count += 1
    pct_rising = round(rising_count / valid_count * 100) if valid_count > 0 else None
    geo_price_today, geo_price_avg = record(b, "geo_commodities", pct_rising)
    geo_mar2, geo_mar2_avg   = record(b, "geo_marine",  fetch_marine_proxy())
    geo_eu,   geo_eu_avg     = record(b, "geo_eu",      fetch_eu_consultations())
    geo_cong, geo_cong_avg   = record(b, "geo_congress", fetch_congress_hearings())
    result["chains"]["geopolitics"] = {
        "commodities":      {"today": geo_price_today, "avg30": geo_price_avg, "label": f"Avg price: {', '.join(geo_comms)}"},
        "marine_traffic":   {"today": geo_mar2,  "avg30": geo_mar2_avg,  "label": "Shipping congestion news (7d)"},
        "eu_consultations": {"today": geo_eu,    "avg30": geo_eu_avg,    "label": "EU open consultations"},
        "congress_hearings":{"today": geo_cong,  "avg30": geo_cong_avg,  "label": "Congressional hearings (7d)"},
    }

    # ── Corporate ─────────────────────────────────────────────────────────────
    log.info("Fetching Corporate chain...")
    watchlist = cfg["corporate_chain"]["company_watchlist"]
    ats_map   = cfg["corporate_chain"].get("ats_map", {})
    # Merge jobs_keywords with arxiv and NIH keywords (deduplicated, order-preserving)
    _raw_kws = (
        cfg["corporate_chain"].get("jobs_keywords", []) +
        cfg["tech_chain"].get("arxiv_keywords", []) +
        cfg["biotech_chain"].get("nih_keywords", [])
    )
    jobs_keywords = list(dict.fromkeys(_raw_kws))

    corp_f4,   corp_f4_avg   = record(b, "corp_form4",   fetch_sec_form4(watchlist))
    corp_jobs_today, corp_jobs_str = fetch_jobs_per_company(ats_map, jobs_keywords, b)
    corp_jobs, corp_jobs_avg = record(b, "corp_jobs", corp_jobs_today)
    corp_pat,  corp_pat_avg  = record(b, "corp_patents", fetch_patents(watchlist))
    corp_8k,   corp_8k_avg   = record(b, "corp_8k",      fetch_sec_8k(watchlist))
    result["chains"]["corporate"] = {
        "sec_form4":        {"today": corp_f4,   "avg30": corp_f4_avg,   "label": "SEC Form 4 insider filings (24h)"},
        "jobs_per_company": {"today": corp_jobs, "avg30": corp_jobs_avg, "label": "Per-company job postings (keyword-matched)"},
        "patents":          {"today": corp_pat,  "avg30": corp_pat_avg,  "label": "Patent filings by watchlist cos (24h)"},
        "sec_8k":           {"today": corp_8k,   "avg30": corp_8k_avg,   "label": "SEC 8-K filings by watchlist (24h)"},
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

    # ── Secondary signals (Yahoo Finance) ─────────────────────────────────────
    ticker_map = cfg["corporate_chain"].get("ticker_map", {})
    if ticker_map:
        log.info("Fetching secondary signals (short interest + options + Form 4 per-company) via Yahoo Finance...")
        short_interest_str = fetch_short_interest_all(ticker_map, b)
        options_str = fetch_options_activity_all(ticker_map, b)
        form4_per_company_str = fetch_form4_per_company(ticker_map)
        log.info(f"  Short interest: {short_interest_str[:300]}")
        log.info(f"  Options C/P:    {options_str[:300]}")
        log.info(f"  Form4 per-co:   {form4_per_company_str[:300]}")
    else:
        short_interest_str = "N/A (no ticker_map configured)"
        options_str = "N/A (no ticker_map configured)"
        form4_per_company_str = "N/A (no ticker_map configured)"

    result["secondary_signals"] = {
        "short_interest": short_interest_str,
        "options_cp_ratio": options_str,
        "form4_per_company": form4_per_company_str,
        "jobs_per_company": corp_jobs_str,
        "layoffs_summary": layoffs_summary_str,
        "sector_hiring_summary": sector_hiring_summary_str,
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
