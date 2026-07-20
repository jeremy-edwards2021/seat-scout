# Seat Scout

Autonomous movie seat availability monitor.

- Submit a movie + city + format
- Scrapes Fandango 24/7 via headless Chromium
- Public dashboard with live results

## Run

```bash
pip install -r requirements.txt
python -m playwright install chromium
python3 app.py
```

Open http://localhost:8000

## Deploy

Railway: push this repo and connect it. Nixpacks auto-builds.
