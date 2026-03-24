#!/usr/bin/env python3
"""
Signal Watch — Stage 4: Publisher
Renders the live dashboard from analysis JSON and deploys to GitHub Pages.

Usage: python src/publisher.py
"""

import json
import yaml
import logging
import subprocess
from datetime import date
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
OUTPUT_DIR = BASE_DIR / "output"
TEMPLATES_DIR = BASE_DIR / "templates"
DOCS_DIR = BASE_DIR / "docs"

with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

TODAY = date.today().isoformat()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_analysis(date_str: str = None) -> dict:
    path = OUTPUT_DIR / f"analysis_{date_str or TODAY}.json"
    if not path.exists():
        raise FileNotFoundError(f"Analysis not found: {path}. Run analyzer.py first.")
    return json.loads(path.read_text())


def load_history(days: int = 30) -> list:
    """Load last N days of analysis files, summarised for the history panel."""
    history = []
    files = sorted(OUTPUT_DIR.glob("analysis_*.json"), reverse=True)
    for f in files[:days]:
        try:
            data = json.loads(f.read_text())
            summary = {
                "date": data["date"],
                "chains": [
                    {
                        "name": c["name"],
                        "convergence": c["convergence"],
                        "confidence": c.get("confidence", "")
                    }
                    for c in data.get("chains", [])
                ],
                "has_alert": bool(data.get("top_alert"))
            }
            history.append(summary)
        except Exception as e:
            log.warning(f"Could not load history file {f}: {e}")
    return list(reversed(history))


def get_dashboard_url() -> str:
    repo = CONFIG["dashboard"]["github_pages_repo"]
    parts = repo.split("/")
    if len(parts) == 2:
        return f"https://{parts[0]}.github.io/{parts[1]}"
    return "#"


def render_dashboard(analysis: dict, history: list) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template("dashboard.html")
    return template.render(
        analysis=analysis,
        history=history,
        date=TODAY,
        dashboard_url=get_dashboard_url()
    )


def git_push():
    """Commit updated dashboard and data files, then push to GitHub."""
    try:
        subprocess.run(
            ["git", "config", "user.email", "github-actions@github.com"],
            cwd=BASE_DIR, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "config", "user.name", "Signal Watch Bot"],
            cwd=BASE_DIR, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "add",
             str(DOCS_DIR / "index.html"),
             str(OUTPUT_DIR),
             str(BASE_DIR / "data")],
            cwd=BASE_DIR, check=True
        )
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=BASE_DIR
        )
        if result.returncode == 0:
            log.info("No changes to commit")
            return
        subprocess.run(
            ["git", "commit", "-m", f"Daily update {TODAY}"],
            cwd=BASE_DIR, check=True
        )
        subprocess.run(
            ["git", "push"],
            cwd=BASE_DIR, check=True
        )
        log.info("Changes committed and pushed to GitHub")
    except subprocess.CalledProcessError as e:
        log.warning(f"Git operation failed: {e} — dashboard rendered locally but not pushed")


def main():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    analysis = load_analysis()
    history = load_history(CONFIG["dashboard"]["sparkline_days"])
    html = render_dashboard(analysis, history)

    index_path = DOCS_DIR / "index.html"
    index_path.write_text(html)
    log.info(f"Dashboard rendered → {index_path}")

    git_push()


if __name__ == "__main__":
    main()
