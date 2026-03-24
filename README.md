# Signal Watch

AI-powered early signal intelligence. Monitors 28 upstream sources across 7 chains every morning, uses Claude to detect convergence, and delivers a daily email digest + live web dashboard.

**Cost:** ~$0.30/month (Claude Haiku). Everything else is free.

---

## How it works

```
07:00 UTC  fetcher.py   → pulls 28 APIs/feeds → data/history/raw_data_YYYY-MM-DD.json
07:05 UTC  analyzer.py  → Claude Haiku analysis → output/analysis_YYYY-MM-DD.json
07:10 UTC  emailer.py   → SendGrid HTML digest → your inbox
07:15 UTC  publisher.py → Jinja2 render → docs/index.html → GitHub Pages
```

A chain fires when **3+ of its 4 signals** show >20% movement above the 30-day average on the same day (the "convergence principle").

---

## Quick start

### Step 1 — Register free API keys

| Service | URL | Used for |
|---|---|---|
| Anthropic | console.anthropic.com | Claude analysis (~$0.30/mo) |
| FRED | fred.stlouisfed.org/docs/api | Economic data |
| Reddit | reddit.com/prefs/apps | Subreddit monitoring |
| SendGrid | sendgrid.com | Email delivery (100/day free) |
| Congress.gov | api.congress.gov | Hearing data (optional) |

### Step 2 — Create your GitHub repo

```bash
# Fork or create a new repo named signal-watch on GitHub, then clone it
git clone https://github.com/YOUR_USERNAME/signal-watch
cd signal-watch
```

### Step 3 — Add secrets to GitHub

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

```
ANTHROPIC_API_KEY
FRED_API_KEY
REDDIT_CLIENT_ID
REDDIT_SECRET
SENDGRID_API_KEY
CONGRESS_API_KEY     (optional)
```

### Step 4 — Personalise config.yaml

Open `config.yaml` and update:

```yaml
email:
  to: your@email.com          # where to send the digest
  from: signals@yourdomain.com

tech_chain:
  arxiv_keywords:
    - "agentic AI"            # what to track on arXiv
  patent_companies:
    - "OpenAI"               # whose patents to monitor

social_chain:
  subreddits:
    - r/longevity             # subreddits to watch
  google_trends_terms:
    - "biohacking"

corporate_chain:
  company_watchlist:
    - "NVDA"                  # companies to track SEC filings for

dashboard:
  github_pages_repo: "YOUR_USERNAME/signal-watch"  # update this!
```

### Step 5 — Enable GitHub Pages

1. Go to **Settings → Pages**
2. Set Source to **Deploy from a branch**
3. Branch: `main`, folder: `/docs`
4. Save

### Step 6 — Run manually to verify

1. Go to **Actions → Signal Watch — Daily Pipeline**
2. Click **Run workflow**
3. Watch all 4 stages complete
4. Check your inbox and visit `https://YOUR_USERNAME.github.io/signal-watch`

### Step 7 — Test source connections locally

```bash
pip install -r requirements.txt

# Set env vars
export FRED_API_KEY=your_key
export REDDIT_CLIENT_ID=your_id
export REDDIT_SECRET=your_secret
export ANTHROPIC_API_KEY=your_key

# Run connection test
python src/fetcher.py --test
```

After that, the workflow runs automatically every day at **07:00 UTC**.

---

## Signal chains

| Chain | Sources | Fires when | Time horizon |
|---|---|---|---|
| Tech | arXiv, HN, USPTO, Crunchbase | 3+ RISING | ~18 months |
| Economy | FRED yield, FRED box, Marine, Jobs | 3+ RISING | 6–9 months |
| Biotech | NIH, bioRxiv, ClinicalTrials, SEC S-1 | 3+ RISING | 3–5 years |
| Social | Reddit, Google Trends, Kickstarter, Amazon | 3+ RISING | 12–24 months |
| Geopolitics | Commodities, Marine, EU, Congress | 3+ RISING | 3–6 months |
| Corporate | SEC Form 4, Jobs, Patents, SEC 8-K | 3+ RISING | Weeks–months |
| Energy | ARPA-E, arXiv physics, Commodities, Crunchbase | 3+ RISING | 3–7 years |

---

## File structure

```
signal-watch/
├── .github/workflows/daily.yml   # GitHub Actions scheduler
├── src/
│   ├── fetcher.py                # Stage 1: fetches all 28 sources
│   ├── analyzer.py               # Stage 2: Claude API analysis
│   ├── emailer.py                # Stage 3: SendGrid digest
│   └── publisher.py              # Stage 4: GitHub Pages render
├── templates/
│   ├── email.html                # HTML email template (Jinja2)
│   └── dashboard.html            # Dashboard template (Jinja2)
├── data/
│   ├── baselines.json            # 30-day rolling averages (auto-updated)
│   └── history/                  # raw_data_YYYY-MM-DD.json files
├── output/                       # analysis_YYYY-MM-DD.json files
├── docs/
│   └── index.html                # Live dashboard (auto-generated)
├── prompts/
│   ├── system_prompt.txt         # Claude analyst persona + scoring rules
│   └── user_prompt_template.txt  # Daily data injection template
├── config.yaml                   # All user configuration (edit this)
└── requirements.txt
```

---

## Thresholds

All thresholds are in `config.yaml`:

```yaml
thresholds:
  rising_pct: 20       # % above 30d avg = RISING
  dropping_pct: 20     # % below 30d avg = DROPPING
  convergence_alert: 3 # chains with this many RISING signals get flagged
```

---

## Troubleshooting

**Email not arriving:** Check `output/email_preview_YYYY-MM-DD.html` — the email is always saved locally even if SendGrid fails. Verify `SENDGRID_API_KEY` is set and sender domain is verified.

**Source returning SILENT:** Most sources fail gracefully. Check the Actions log for warning messages. Sources that are SILENT are excluded from convergence scoring.

**Dashboard not updating:** Ensure GitHub Pages is set to deploy from `docs/` on `main`. Check the workflow has `contents: write` permission.

**FRED data missing:** Ensure `FRED_API_KEY` is set as a repo secret. Get a free key at fred.stlouisfed.org/docs/api.
