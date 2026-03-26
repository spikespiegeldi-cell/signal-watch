#!/usr/bin/env python3
"""
Signal Watch — Stage 2: Analyzer
Reads raw fetched data, calls Claude Sonnet for cross-chain analysis,
saves structured JSON output, and maintains the convergence log.

Usage: python src/analyzer.py
"""

import os
import json
import re
import yaml
import logging
import anthropic
from datetime import date, timedelta
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

# Signal key order per chain — must match fetcher.py output order
CHAIN_SIGNAL_KEYS = {
    "Tech":        ["arxiv", "hacker_news", "patents", "crunchbase"],
    "Economy":     ["yield_curve", "box_production", "layoffs", "sector_hiring"],
    "Biotech":     ["nih_reporter", "biorxiv", "clinical_trials", "sec_s1_biotech"],
    "Social":      ["bluesky", "google_trends", "kickstarter", "amazon_movers"],
    "Geopolitics": ["commodities", "marine_traffic", "eu_consultations", "congress_hearings"],
    "Corporate":   ["sec_form4", "jobs_per_company", "patents", "sec_8k"],
    "Energy":      ["arpa_e", "arxiv_physics", "energy_commodities", "crunchbase_energy"],
}

# Hypothesis pairs for sequential alert detection
SEQUENTIAL_HYPOTHESES = [
    ("Biotech",     "Corporate", 21, "Biotech leads Corporate by up to 21 days"),
    ("Tech",        "Corporate", 21, "Tech leads Corporate by up to 21 days"),
    ("Geopolitics", "Energy",    14, "Geopolitics leads Energy by up to 14 days"),
    ("Geopolitics", "Economy",   14, "Geopolitics leads Economy by up to 14 days"),
    ("Tech",        "Biotech",   30, "Tech leads Biotech by up to 30 days"),
]

# Common English words filtered out of capitalised-phrase entity extraction
_COMMON_CAPS = {
    "The", "This", "That", "These", "Those", "With", "From", "Into", "Over",
    "Under", "Above", "Below", "Between", "Through", "During", "Before", "After",
    "While", "Since", "Until", "Although", "Because", "However", "Therefore",
    "Furthermore", "Additionally", "Moreover", "Nevertheless", "Consequently",
    "Rising", "Falling", "Dropping", "Growing", "Declining", "Increasing",
    "Decreasing", "Flat", "Strong", "Weak", "High", "Low", "New", "Key",
    "Major", "Minor", "Large", "Small", "First", "Second", "Third",
    "Signal", "Source", "Value", "Level", "Rate", "Index",
    "Market", "Sector", "Industry", "Company", "Chain",
    "United", "States", "Federal", "National", "Global", "International",
    "Corp", "Inc", "Ltd",
}


# ── Data loading ───────────────────────────────────────────────────────────────

def load_raw_data() -> dict:
    path = HISTORY_DIR / f"raw_data_{TODAY}.json"
    if not path.exists():
        raise FileNotFoundError(f"Raw data not found: {path}. Run fetcher.py first.")
    return json.loads(path.read_text())


def val(raw: dict, chain: str, source: str, key: str = "today") -> str:
    v = raw["chains"].get(chain, {}).get(source, {}).get(key)
    return str(v) if v is not None else "N/A (source silent)"


# ── Entity extraction ──────────────────────────────────────────────────────────

def _extract_entities(conclusion: str, cfg: dict) -> list:
    """Extract named entities from a chain conclusion using pattern matching only."""
    entities = set()

    # 1. Company names from corporate watchlist
    for company in cfg.get("corporate_chain", {}).get("company_watchlist", []):
        if company.lower() in conclusion.lower():
            entities.add(company)

    # 2. Tech / drug keyword terms
    for kw_list in [
        cfg.get("tech_chain", {}).get("arxiv_keywords", []),
        cfg.get("biotech_chain", {}).get("nih_keywords", []),
        cfg.get("biotech_chain", {}).get("clinicaltrials_keywords", []),
    ]:
        for kw in kw_list:
            if kw.lower() in conclusion.lower():
                entities.add(kw)

    # 3. Capitalised 2–3 word phrases not composed entirely of common words
    for match in re.finditer(
        r'\b([A-Z][A-Za-z0-9\-]+(?:\s+[A-Z][A-Za-z0-9\-]+){1,2})\b', conclusion
    ):
        phrase = match.group(1)
        if not all(w in _COMMON_CAPS for w in phrase.split()):
            entities.add(phrase)

    return sorted(entities)


# ── Convergence log helpers ────────────────────────────────────────────────────

def _load_log_history(log_path: Path, n: int = 30) -> list:
    """Read the last n records from the convergence log."""
    if not log_path.exists():
        return []
    lines = log_path.read_text().strip().splitlines()
    records = []
    for line in lines[-n:]:
        try:
            records.append(json.loads(line))
        except Exception:
            pass
    return records


def _detect_sequential_alerts(chains_today: dict, entities_today: dict,
                               history: list, today_str: str) -> list:
    """Check all hypothesis pairs and return sequential alerts firing today."""
    today = date.fromisoformat(today_str)
    alerts = []

    for chain_a, chain_b, max_lag, hypothesis in SEQUENTIAL_HYPOTHESES:
        chain_b_conv = chains_today.get(chain_b, {}).get("convergence", 0)
        if chain_b_conv < 2:
            continue

        for record in reversed(history):   # most recent first
            try:
                record_date = date.fromisoformat(record.get("date", ""))
            except ValueError:
                continue
            lag = (today - record_date).days
            if lag < 1 or lag > max_lag:
                continue

            chain_a_conv = record.get("chains", {}).get(chain_a, {}).get("convergence", 0)
            if chain_a_conv >= 3:
                alert = {
                    "type": "sequential",
                    "chain_a": chain_a,
                    "chain_b": chain_b,
                    "chain_a_fired_date": record["date"],
                    "chain_a_convergence": chain_a_conv,
                    "chain_b_convergence_today": chain_b_conv,
                    "days_lag": lag,
                    "hypothesis": hypothesis,
                }
                # Entity overlap between historical chain_a and today's chain_b
                hist_ents_a = {e.lower() for e in record.get("entities", {}).get(chain_a, [])}
                curr_ents_b = {e.lower() for e in entities_today.get(chain_b, [])}
                overlap = sorted(hist_ents_a & curr_ents_b)
                if overlap:
                    alert["entity_overlap"] = overlap
                alerts.append(alert)
                break  # Only the most recent match per hypothesis pair

    return alerts


def _write_log_record(record: dict, log_path: Path, today: str):
    """Append record to log. Overwrites the last line if it is already today's record."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":"))

    if log_path.exists():
        content = log_path.read_text()
        lines = content.splitlines()
        if lines:
            try:
                if json.loads(lines[-1]).get("date") == today:
                    lines[-1] = line
                    log_path.write_text("\n".join(lines) + "\n")
                    return
            except Exception:
                pass
        with log_path.open("a") as f:
            f.write(line + "\n")
    else:
        log_path.write_text(line + "\n")


def append_convergence_log(analysis: dict, log_path: Path) -> list:
    """Build and append a structured convergence record. Returns today's sequential_alerts."""
    today = analysis.get("date", TODAY)
    cfg = CONFIG

    chains_record = {}
    entities_record = {}

    for chain in analysis.get("chains", []):
        name = chain["name"]
        convergence = chain.get("convergence", 0)
        confidence = chain.get("confidence", "LOW")

        # Map signals positionally to short keys
        keys = CHAIN_SIGNAL_KEYS.get(name, [])
        signals = {
            (keys[i] if i < len(keys) else f"signal_{i}"): sig.get("status", "SILENT")
            for i, sig in enumerate(chain.get("signals", []))
        }

        chains_record[name] = {
            "convergence": convergence,
            "signals": signals,
            "confidence": confidence,
        }
        entities_record[name] = (
            _extract_entities(chain.get("conclusion", ""), cfg)
            if convergence >= 2 else []
        )

    top_alert = analysis.get("top_alert")
    top_alert_record = {
        "fired": top_alert is not None,
        "alert_chains": analysis.get("alert_chains", []),
    }

    history = _load_log_history(log_path, n=30)
    sequential_alerts = _detect_sequential_alerts(
        chains_record, entities_record, history, today
    )

    record = {
        "date": today,
        "chains": chains_record,
        "top_alert": top_alert_record,
        "sequential_alerts": sequential_alerts,
        "entities": entities_record,
    }

    _write_log_record(record, log_path, today)

    total = len(log_path.read_text().strip().splitlines())
    log.info(f"Convergence log updated → data/convergence_log.jsonl ({total} records total)")

    return sequential_alerts


def _load_prev_sequential_alerts(log_path: Path) -> list:
    """Return sequential alerts from the most recent past log record (not today)."""
    for record in reversed(_load_log_history(log_path, n=10)):
        if record.get("date") != TODAY:
            return record.get("sequential_alerts", [])
    return []


# ── Prompt rendering ───────────────────────────────────────────────────────────

def render_user_prompt(raw: dict, seq_alerts: list = None) -> str:
    template_text = (PROMPTS_DIR / "user_prompt_template.txt").read_text()
    cfg = CONFIG
    seq_alerts = seq_alerts or []
    seq_alert_str = "; ".join(
        "{} → {} ({}d lag{})".format(
            a["chain_a"], a["chain_b"], a["days_lag"],
            ", entities: " + ", ".join(a["entity_overlap"]) if a.get("entity_overlap") else ""
        )
        for a in seq_alerts
    ) if seq_alerts else "none"

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
        "LAYOFFS_TODAY": val(raw, "economy", "layoffs"),
        "LAYOFFS_AVG":   val(raw, "economy", "layoffs", "avg30"),
        "HIRING_TODAY":  val(raw, "economy", "sector_hiring"),
        "HIRING_AVG":    val(raw, "economy", "sector_hiring", "avg30"),
        "LAYOFFS_SUMMARY":       raw.get("secondary_signals", {}).get("layoffs_summary",       "N/A (not fetched)"),
        "SECTOR_HIRING_SUMMARY": raw.get("secondary_signals", {}).get("sector_hiring_summary", "N/A (not fetched)"),
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
        "BLUESKY_KEYWORDS": ", ".join(cfg["social_chain"]["bluesky_keywords"]),
        "TRENDS_TERMS":  ", ".join(cfg["social_chain"]["google_trends_terms"]),
        "BLUESKY_TODAY": val(raw, "social", "bluesky"),
        "BLUESKY_AVG":   val(raw, "social", "bluesky", "avg30"),
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
        "CORP_JOBS_TODAY":val(raw, "corporate", "jobs_per_company"),
        "CORP_JOBS_AVG":  val(raw, "corporate", "jobs_per_company", "avg30"),
        "JOBS_PER_COMPANY": raw.get("secondary_signals", {}).get("jobs_per_company", "N/A (not fetched)"),
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
        # Secondary signals (Yahoo Finance)
        "SHORT_INTEREST":     raw.get("secondary_signals", {}).get("short_interest",     "N/A (not fetched)"),
        "OPTIONS_CP_RATIO":   raw.get("secondary_signals", {}).get("options_cp_ratio",   "N/A (not fetched)"),
        "FORM4_PER_COMPANY":  raw.get("secondary_signals", {}).get("form4_per_company",  "N/A (not fetched)"),
        # Sequential alerts (from previous day's log — informs Claude of active propagation)
        "SEQUENTIAL_ALERTS": seq_alert_str,
    }
    return Template(template_text).render(**ctx)


# ── Claude call ────────────────────────────────────────────────────────────────

def call_claude(system: str, user: str) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16000,
        temperature=0,
        system=system,
        messages=[{"role": "user", "content": user}]
    )
    text = response.content[0].text.strip()
    log.info(f"Claude response: {len(text)} chars, stop_reason={response.stop_reason}")

    if response.stop_reason == "max_tokens":
        log.error("Response truncated at max_tokens — output may be incomplete")

    # Strip markdown fences
    if "```" in text:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if match:
            text = match.group(1).strip()

    # Find outermost JSON object
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        text = text[start:end]

    return json.loads(text)


# ── Raw data enrichment ────────────────────────────────────────────────────────

def enrich_with_raw(analysis: dict, raw: dict) -> dict:
    """Inject today/avg30 values from raw_data into each signal using positional matching."""
    raw_chains = raw.get("chains", {})
    for chain in analysis.get("chains", []):
        chain_name = chain.get("name", "")
        raw_chain = raw_chains.get(chain_name.lower(), {})
        keys = CHAIN_SIGNAL_KEYS.get(chain_name, [])
        for i, signal in enumerate(chain.get("signals", [])):
            raw_entry = raw_chain.get(keys[i], {}) if i < len(keys) else {}
            signal["today"] = raw_entry.get("today", "N/A") if raw_entry else "N/A"
            signal["avg30"] = raw_entry.get("avg30", "N/A") if raw_entry else "N/A"
    return analysis


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = load_raw_data()
    system_prompt = (PROMPTS_DIR / "system_prompt.txt").read_text()

    # Load previous sequential alerts from log to inform Claude
    log_path = BASE_DIR / "data" / "convergence_log.jsonl"
    prev_seq_alerts = _load_prev_sequential_alerts(log_path)
    if prev_seq_alerts:
        log.info(f"Loaded {len(prev_seq_alerts)} previous sequential alert(s) for prompt context")

    user_prompt = render_user_prompt(raw, prev_seq_alerts)

    log.info("Calling Claude Sonnet for analysis...")
    analysis = call_claude(system_prompt, user_prompt)
    analysis = enrich_with_raw(analysis, raw)

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

    # Append convergence log and detect today's sequential alerts
    seq_alerts = append_convergence_log(analysis, log_path)

    if seq_alerts:
        analysis["sequential_alerts"] = seq_alerts
        out_path.write_text(json.dumps(analysis, indent=2))
        for alert in seq_alerts:
            overlap_str = (
                f", entity overlap: {alert['entity_overlap']}"
                if alert.get("entity_overlap") else ""
            )
            log.info(
                f"SEQUENTIAL ALERT: {alert['chain_a']} → {alert['chain_b']} "
                f"({alert['days_lag']} day lag{overlap_str})"
            )

    return analysis


if __name__ == "__main__":
    main()
