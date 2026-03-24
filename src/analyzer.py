#!/usr/bin/env python3
"""
Signal Watch — Stage 2: Analyzer
Reads raw fetched data, calls Claude Haiku for cross-chain analysis,
and saves structured JSON output.

Usage: python src/analyzer.py
"""

import os
import json
import yaml
import logging
import anthropic
from datetime import date
from pathlib import Path
from jinja2 import Template

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
HISTORY_DIR = BASE_DIR / "data" / "history"
OUTPUT_DIR = BASE_DIR / "output"
PROMPTS_DIR = BASE_DIR / "prompts"

with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

TODAY = date.today().isoformat()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_raw_data() -> dict:
    path = HISTORY_DIR / f"raw_data_{TODAY}.json"
    if not path.exists():
        raise FileNotFoundError(f"Raw data not found: {path}. Run fetcher.py first.")
    return json.loads(path.read_text())


def val(raw: dict, chain: str, source: str, key: str = "today") -> str:
    v = raw["chains"].get(chain, {}).get(source, {}).get(key)
    return str(v) if v is not None else "N/A (source silent)"


def render_user_prompt(raw: dict) -> str:
    template_text = (PROMPTS_DIR / "user_prompt_template.txt").read_text()
    cfg = CONFIG
    ctx = {
        "DATE": raw["date"],
        # Tech
        "TECH_KEYWORDS": ", ".join(cfg["tech_chain"]["arxiv_keywords"]),
        "ARXIV_TODAY":   val(raw, "tech", "arxiv"),
        "ARXIV_AVG":     val(raw, "tech", "arxiv", "avg30"),
        "HN_TODAY":      val(raw, "tech", "hacker_news"),
        "HN_AVG":        val(raw, "tech", "hacker_news", "avg30"),
        "PATENT_TODAY":  val(raw, "tech", "patents"),
        "PATENT_AVG":    val(raw, "tech", "patents", "avg30"),
        "CB_TODAY":      val(raw, "tech", "crunchbase"),
        "CB_AVG":        val(raw, "tech", "crunchbase", "avg30"),
        # Economy
        "YIELD_TODAY":   val(raw, "economy", "yield_curve"),
        "YIELD_AVG":     val(raw, "economy", "yield_curve", "avg30"),
        "BOX_TODAY":     val(raw, "economy", "box_production"),
        "BOX_AVG":       val(raw, "economy", "box_production", "avg30"),
        "MARINE_TODAY":  val(raw, "economy", "marine_traffic"),
        "MARINE_AVG":    val(raw, "economy", "marine_traffic", "avg30"),
        "JOBS_TODAY":    val(raw, "economy", "linkedin_jobs"),
        "JOBS_AVG":      val(raw, "economy", "linkedin_jobs", "avg30"),
        # Biotech
        "BIO_KEYWORDS":  ", ".join(cfg["biotech_chain"]["nih_keywords"]),
        "NIH_TODAY":     val(raw, "biotech", "nih_reporter"),
        "NIH_AVG":       val(raw, "biotech", "nih_reporter", "avg30"),
        "BIORXIV_TODAY": val(raw, "biotech", "biorxiv"),
        "BIORXIV_AVG":   val(raw, "biotech", "biorxiv", "avg30"),
        "CLIN_TODAY":    val(raw, "biotech", "clinical_trials"),
        "CLIN_AVG":      val(raw, "biotech", "clinical_trials", "avg30"),
        "SEC_BIO_TODAY": val(raw, "biotech", "sec_s1_biotech"),
        "SEC_BIO_AVG":   val(raw, "biotech", "sec_s1_biotech", "avg30"),
        # Social
        "SUBREDDITS":    ", ".join(cfg["social_chain"]["subreddits"]),
        "TRENDS_TERMS":  ", ".join(cfg["social_chain"]["google_trends_terms"]),
        "REDDIT_TODAY":  val(raw, "social", "reddit"),
        "REDDIT_AVG":    val(raw, "social", "reddit", "avg30"),
        "TRENDS_TODAY":  val(raw, "social", "google_trends"),
        "TRENDS_AVG":    val(raw, "social", "google_trends", "avg30"),
        "KS_TODAY":      val(raw, "social", "kickstarter"),
        "KS_AVG":        val(raw, "social", "kickstarter", "avg30"),
        "AMZ_TODAY":     val(raw, "social", "amazon_movers"),
        "AMZ_AVG":       val(raw, "social", "amazon_movers", "avg30"),
        # Geopolitics
        "GEO_COMMODITIES": ", ".join(cfg["geopolitics_chain"]["commodities"]),
        "GEO_COMM_TODAY":  val(raw, "geopolitics", "commodities"),
        "GEO_COMM_AVG":    val(raw, "geopolitics", "commodities", "avg30"),
        "GEO_MAR_TODAY":   val(raw, "geopolitics", "marine_traffic"),
        "GEO_MAR_AVG":     val(raw, "geopolitics", "marine_traffic", "avg30"),
        "EU_TODAY":        val(raw, "geopolitics", "eu_consultations"),
        "EU_AVG":          val(raw, "geopolitics", "eu_consultations", "avg30"),
        "CONG_TODAY":      val(raw, "geopolitics", "congress_hearings"),
        "CONG_AVG":        val(raw, "geopolitics", "congress_hearings", "avg30"),
        # Corporate
        "WATCHLIST":      ", ".join(cfg["corporate_chain"]["company_watchlist"]),
        "CORP_F4_TODAY":  val(raw, "corporate", "sec_form4"),
        "CORP_F4_AVG":    val(raw, "corporate", "sec_form4", "avg30"),
        "CORP_JOBS_TODAY":val(raw, "corporate", "jobs_proxy"),
        "CORP_JOBS_AVG":  val(raw, "corporate", "jobs_proxy", "avg30"),
        "CORP_PAT_TODAY": val(raw, "corporate", "patents"),
        "CORP_PAT_AVG":   val(raw, "corporate", "patents", "avg30"),
        "CORP_8K_TODAY":  val(raw, "corporate", "sec_8k"),
        "CORP_8K_AVG":    val(raw, "corporate", "sec_8k", "avg30"),
        # Energy
        "EN_COMMODITIES": ", ".join(cfg["energy_chain"]["commodities"]),
        "EN_ARPA_TODAY":  val(raw, "energy", "arpa_e"),
        "EN_ARPA_AVG":    val(raw, "energy", "arpa_e", "avg30"),
        "EN_PHYS_TODAY":  val(raw, "energy", "arxiv_physics"),
        "EN_PHYS_AVG":    val(raw, "energy", "arxiv_physics", "avg30"),
        "EN_COMM_TODAY":  val(raw, "energy", "energy_commodities"),
        "EN_COMM_AVG":    val(raw, "energy", "energy_commodities", "avg30"),
        "EN_CB_TODAY":    val(raw, "energy", "crunchbase_energy"),
        "EN_CB_AVG":      val(raw, "energy", "crunchbase_energy", "avg30"),
    }
    return Template(template_text).render(**ctx)


def call_claude(system: str, user: str) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8192,
        temperature=0,
        system=system,
        messages=[{"role": "user", "content": user}]
    )
    text = response.content[0].text.strip()
    log.info(f"Response: {len(text)} chars, stop_reason={response.stop_reason}")

    # Strip markdown fences if Claude wrapped the JSON anyway
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        text = text.strip()

    # Find JSON object boundaries robustly
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        text = text[start:end]

    if response.stop_reason == "max_tokens":
        log.error("Response was truncated (max_tokens reached) — JSON will be incomplete")

    return json.loads(text)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = load_raw_data()
    system_prompt = (PROMPTS_DIR / "system_prompt.txt").read_text()
    user_prompt = render_user_prompt(raw)

    log.info("Calling Claude Haiku for analysis...")
    analysis = call_claude(system_prompt, user_prompt)

    out_path = OUTPUT_DIR / f"analysis_{TODAY}.json"
    out_path.write_text(json.dumps(analysis, indent=2))
    log.info(f"Analysis saved → {out_path}")

    # Log convergence summary
    for chain in analysis.get("chains", []):
        conv = chain.get("convergence", 0)
        conf = chain.get("confidence", "—")
        log.info(f"  {chain['name']:15s}: convergence={conv}  confidence={conf}")

    top = analysis.get("top_alert")
    if top:
        log.info(f"TOP ALERT: {top}")

    return analysis


if __name__ == "__main__":
    main()
