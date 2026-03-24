#!/usr/bin/env python3
"""
Signal Watch — Stage 3: Emailer
Reads the daily analysis JSON and sends an HTML digest via SendGrid.

Usage: python src/emailer.py
"""

import os
import json
import yaml
import logging
from datetime import date
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
OUTPUT_DIR = BASE_DIR / "output"
TEMPLATES_DIR = BASE_DIR / "templates"

with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

TODAY = date.today().isoformat()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_analysis() -> dict:
    path = OUTPUT_DIR / f"analysis_{TODAY}.json"
    if not path.exists():
        raise FileNotFoundError(f"Analysis not found: {path}. Run analyzer.py first.")
    return json.loads(path.read_text())


def should_send(analysis: dict) -> bool:
    min_conv = CONFIG["email"]["only_alert_on_convergence"]
    return any(chain["convergence"] >= min_conv for chain in analysis.get("chains", []))


def build_subject(analysis: dict) -> str:
    top = analysis.get("top_alert")
    firing = [c["name"] for c in analysis.get("chains", []) if c["convergence"] >= 3]
    if top:
        short = top[:60] + "..." if len(top) > 60 else top
        return f"Signal Watch {TODAY} — {short}"
    elif firing:
        names = ", ".join(firing)
        return f"Signal Watch {TODAY} — {len(firing)} chain(s) firing: {names}"
    else:
        return f"Signal Watch {TODAY} — Daily digest"


def get_dashboard_url() -> str:
    repo = CONFIG["dashboard"]["github_pages_repo"]
    parts = repo.split("/")
    if len(parts) == 2:
        return f"https://{parts[0]}.github.io/{parts[1]}"
    return "#"


def render_email(analysis: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template("email.html")
    return template.render(
        analysis=analysis,
        date=TODAY,
        dashboard_url=get_dashboard_url(),
        min_convergence=CONFIG["email"]["only_alert_on_convergence"]
    )


def send_email(subject: str, html_content: str):
    api_key = os.environ.get("SENDGRID_API_KEY", "")
    if not api_key:
        log.warning("SENDGRID_API_KEY not set — skipping send (preview saved)")
        return
    message = Mail(
        from_email=CONFIG["email"]["from"],
        to_emails=CONFIG["email"]["to"],
        subject=subject,
        html_content=html_content
    )
    sg = SendGridAPIClient(api_key)
    response = sg.send(message)
    log.info(f"Email sent to {CONFIG['email']['to']} — HTTP {response.status_code}")


def main():
    analysis = load_analysis()

    if not should_send(analysis):
        min_conv = CONFIG["email"]["only_alert_on_convergence"]
        log.info(f"No chains reach convergence >= {min_conv} — skipping email")
        return

    subject = build_subject(analysis)
    html = render_email(analysis)

    # Always save preview
    preview_path = OUTPUT_DIR / f"email_preview_{TODAY}.html"
    preview_path.write_text(html)
    log.info(f"Email preview saved → {preview_path}")

    send_email(subject, html)


if __name__ == "__main__":
    main()
