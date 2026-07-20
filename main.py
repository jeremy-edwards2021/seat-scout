"""
Seat Scout — Multi-tenant autonomous seat availability monitor.
Anyone can submit a movie + city + format search; the scraper runs 24/7 and
serves results from a public dashboard.

Deploy:
    railway up
Local:
    python3 app.py
"""

import os, sys, json, asyncio, hashlib, re, logging, subprocess, sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional

# ── deps ────────────────────────────────────────────────────────────────────
try:
    from fastapi import FastAPI, Request, Query, Form, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
    import uvicorn
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn", "-q"])
    from fastapi import FastAPI, Request, Query, Form, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
    import uvicorn

try:
    from playwright.async_api import async_playwright
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "-q"])
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
    from playwright.async_api import async_playwright


LOG = logging.getLogger("seatscout")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = os.environ.get("DB_PATH", "seat_scout.db")
MAX_CONCURRENT_SCRAPES = int(os.environ.get("MAX_CONCURRENT_SCRAPES", "3"))
DEFAULT_INTERVAL = int(os.environ.get("DEFAULT_INTERVAL", "300"))  # 5 min per search
DAYS_AHEAD_DEFAULT = int(os.environ.get("DAYS", "7"))


# ═══════════════════════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie TEXT NOT NULL,
    movie_id TEXT NOT NULL,
    city TEXT NOT NULL,
    zip_code TEXT NOT NULL,
    formats TEXT NOT NULL,
    interval_seconds INTEGER DEFAULT 300,
    days_ahead INTEGER DEFAULT 7,
    active INTEGER DEFAULT 1,
    created_at TEXT,
    last_scraped TEXT,
    next_scrape TEXT,
    last_error TEXT,
    checks_completed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS showtimes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER,
    theater_name TEXT,
    theater_distance TEXT,
    date TEXT,
    time TEXT,
    format TEXT,
    status TEXT,
    first_seen TEXT,
    last_seen TEXT,
    UNIQUE(search_id, theater_name, date, time, format)
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id INTEGER,
    message TEXT,
    created_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_showtimes_search ON showtimes(search_id);
CREATE INDEX IF NOT EXISTS idx_searches_next ON searches(next_scrape);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# Movie + city resolution
# ═══════════════════════════════════════════════════════════════════════════════

KNOWN_MOVIES = {
    "the odyssey": "241283",
    "odyssey": "241283",
    "odyssey 70mm": "241386",
    "odyssey imax": "241386",
    "avengers doomsday": "237176",
    "avengers: doomsday": "237176",
    "moana": "243429",
    "supergirl": "243716",
    "toy story 5": "243393",
}

CITY_ZIPS = {
    "new york": "10023", "nyc": "10023", "manhattan": "10023",
    "los angeles": "90028", "la": "90028",
    "chicago": "60611",
    "san francisco": "94102", "sf": "94102",
    "boston": "02111", "seattle": "98101", "austin": "78701",
    "miami": "33132", "denver": "80202", "portland": "97204",
    "philadelphia": "19103", "philly": "19103", "atlanta": "30303",
    "dallas": "75201", "houston": "77002", "phoenix": "85004",
    "nashville": "37201", "las vegas": "89109", "vegas": "89109",
    "orlando": "32801", "san diego": "92101", "detroit": "48226",
    "minneapolis": "55401", "tampa": "33602", "st louis": "63101",
    "pittsburgh": "15222", "charlotte": "28202", "indianapolis": "46204",
    "rochester": "14614", "washington": "20001", "dc": "20001",
    "sacramento": "95814", "baltimore": "21201",
}


def resolve_movie(name: str) -> str:
    key = name.lower().strip()
    if key in KNOWN_MOVIES:
        return KNOWN_MOVIES[key]
    for k, v in KNOWN_MOVIES.items():
        if k in key or key in k:
            return v
    # Allow direct movie IDs
    if name.isdigit():
        return name
    raise ValueError(f"Unknown movie '{name}'. Add to KNOWN_MOVIES or pass a Fandango movie ID.")


def resolve_zip(city: str) -> str:
    return CITY_ZIPS.get(city.lower().strip(), "10023")


# ═══════════════════════════════════════════════════════════════════════════════
# Scraper
# ═══════════════════════════════════════════════════════════════════════════════

async def scrape_one(movie_id: str, date: str, zip_code: str,
                     formats: list[str], browser) -> list[dict]:
    """Returns list of {name, distance, showtimes:[{time,format,status}]}."""
    url = (f"https://www.fandango.com/the-odyssey-the-imax-70mm-experience-2026"
           f"-{movie_id}/movie-times?date={date}&zip={zip_code}")

    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080}, locale="en-US",
    )
    page = await ctx.new_page()
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = { runtime: {} };
    """)
    try:
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(10000)
        body = await page.inner_text("body")
    except Exception as e:
        LOG.warning(f"scrape error: {e}")
        return []
    finally:
        await ctx.close()

    return parse_theaters(body, formats)


def parse_theaters(body: str, formats: list[str]) -> list[dict]:
    lines = body.split("\n")
    theaters = []
    markers = []
    for i, line in enumerate(lines):
        s = line.strip()
        m = re.match(r"^(\d+\.\d{2})\s*mi$", s)
        if m and i > 0:
            name = lines[i - 1].strip()
            if name and len(name) > 5 and not re.match(r"^\d", name):
                markers.append((i - 1, name, m.group(1)))

    for idx, (start, name, distance) in enumerate(markers):
        end = markers[idx + 1][0] if idx + 1 < len(markers) else len(lines)
        section = "\n".join(lines[start:end])
        sts = parse_showtimes(section, formats)
        if sts:
            theaters.append({"name": name, "distance": distance, "showtimes": sts})
    return theaters


def parse_showtimes(section: str, formats: list[str]) -> list[dict]:
    results = []
    lines = section.split("\n")
    capturing = False
    current_format = ""
    times_buf = []
    seat_label = ""
    stop_terms = ["closed caption", "open caption", "standard", "dolby",
                  "laser", "recliner", "accessibility", "premium format"]

    for line in lines:
        s = line.strip()
        if not s:
            continue
        fmt_hit = any(f.lower() in s.lower() for f in formats)
        is_time = re.match(r"^\d{1,2}:\d{2}[ap]", s, re.IGNORECASE)
        is_seat = s.lower() in ("check seats", "sold out", "sold out!")
        is_stop = any(t in s.lower() for t in stop_terms)

        if fmt_hit and ":" not in s and not is_time:
            if capturing and times_buf:
                status = "sold_out" if "sold" in seat_label.lower() else "available"
                for t in times_buf:
                    results.append({"time": t.upper(), "format": current_format, "status": status})
            current_format = s
            capturing = True
            times_buf = []
            seat_label = ""
            continue

        if capturing and is_stop and not fmt_hit:
            if times_buf:
                status = "sold_out" if "sold" in seat_label.lower() else "available"
                for t in times_buf:
                    results.append({"time": t.upper(), "format": current_format, "status": status})
                times_buf = []
            capturing = False
            continue

        if not capturing:
            continue

        if is_time:
            times_buf.append(is_time.group(0).upper())
            continue

        if is_seat:
            seat_label = s.lower()
            if times_buf:
                status = "sold_out" if "sold" in seat_label.lower() else "available"
                for t in times_buf:
                    results.append({"time": t.upper(), "format": current_format, "status": status})
                times_buf = []

    if capturing and times_buf:
        status = "sold_out" if "sold" in seat_label.lower() else "available"
        for t in times_buf:
            results.append({"time": t.upper(), "format": current_format, "status": status})
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Scheduler — picks due searches, scrapes them, stores results
# ═══════════════════════════════════════════════════════════════════════════════

async def scheduler_loop():
    """Continuously poll the DB for due searches and scrape them."""
    pw = None
    browser = None
    install_attempted = False
    # Retry browser launch forever — the web server keeps serving regardless
    while browser is None:
        try:
            pw = await async_playwright().start()
            browser = await pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            LOG.info("Browser launched for scraping")
        except Exception as e:
            LOG.error(f"Browser launch failed: {e}")
            try:
                if pw:
                    await pw.stop()
            except Exception:
                pass
            pw = None

            # Runtime fallback: browser binary missing — download it now
            if not install_attempted and ("Executable doesn't exist" in str(e)
                                          or "playwright install" in str(e).lower()):
                install_attempted = True
                LOG.warning("Browser binary missing — running 'playwright install chromium' (~90s)...")
                try:
                    proc = await asyncio.create_subprocess_exec(
                        sys.executable, "-m", "playwright", "install", "chromium",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                    )
                    await asyncio.wait_for(proc.wait(), timeout=300)
                    LOG.info(f"playwright install finished (rc={proc.returncode})")
                    continue  # retry launch immediately after install
                except Exception as ie:
                    LOG.error(f"playwright install failed: {ie}")

            LOG.info("Retrying browser launch in 60s")
            await asyncio.sleep(60)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCRAPES)
    failures = 0

    while True:
        try:
            conn = db()
            now = datetime.now().isoformat()
            due = conn.execute(
                "SELECT * FROM searches WHERE active=1 AND (next_scrape IS NULL OR next_scrape <= ?)",
                (now,)
            ).fetchall()
            conn.close()

            if due:
                LOG.info(f"scheduler: {len(due)} searches due")

            tasks = [
                process_search(s, browser, semaphore) for s in due
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            LOG.error(f"scheduler error: {e}")
            failures += 1
            if failures >= 5:
                LOG.warning("restarting browser")
                try:
                    await browser.close()
                    await asyncio.sleep(3)
                    browser = await pw.chromium.launch(
                        headless=True,
                        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                    )
                except Exception:
                    pass
                failures = 0

        await asyncio.sleep(10)  # check for due work every 10s


async def process_search(search: sqlite3.Row, browser, semaphore):
    """Scrape one search across all dates, store results, detect changes."""
    async with semaphore:
        sid = search["id"]
        movie_id = search["movie_id"]
        zip_code = search["zip_code"]
        formats = [f.strip() for f in search["formats"].split(",")]
        days = search["days_ahead"]

        now = datetime.now()
        dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

        total_avail = 0
        new_alerts = []

        for date in dates:
            theaters = await scrape_one(movie_id, date, zip_code, formats, browser)
            for t in theaters:
                for s in t["showtimes"]:
                    if s["status"] == "available":
                        total_avail += 1

                    # Upsert showtime
                    conn = db()
                    existing = conn.execute(
                        "SELECT * FROM showtimes WHERE search_id=? AND theater_name=? "
                        "AND date=? AND time=? AND format=?",
                        (sid, t["name"], date, s["time"], s["format"])
                    ).fetchone()

                    if existing:
                        # Detect status changes
                        if existing["status"] != s["status"]:
                            if s["status"] == "available":
                                new_alerts.append(
                                    f"REOPENED: {t['name']} {date} {s['time']} [{s['format']}]"
                                )
                            elif s["status"] == "sold_out" and existing["status"] == "available":
                                new_alerts.append(
                                    f"SOLD OUT: {t['name']} {date} {s['time']} [{s['format']}]"
                                )
                        conn.execute(
                            "UPDATE showtimes SET status=?, last_seen=? WHERE id=?",
                            (s["status"], now.isoformat(), existing["id"])
                        )
                    else:
                        # New showtime
                        if s["status"] == "available":
                            new_alerts.append(
                                f"NEW: {t['name']} {date} {s['time']} [{s['format']}]"
                            )
                        conn.execute(
                            "INSERT INTO showtimes (search_id, theater_name, theater_distance, "
                            "date, time, format, status, first_seen, last_seen) "
                            "VALUES (?,?,?,?,?,?,?,?,?)",
                            (sid, t["name"], t["distance"], date, s["time"], s["format"],
                             s["status"], now.isoformat(), now.isoformat())
                        )
                    conn.commit()
                    conn.close()
            await asyncio.sleep(0.3)  # polite

        # Update search metadata
        next_scrape = (now + timedelta(seconds=search["interval_seconds"])).isoformat()
        conn = db()
        conn.execute(
            "UPDATE searches SET last_scraped=?, next_scrape=?, checks_completed=checks_completed+1, "
            "last_error=NULL WHERE id=?",
            (now.isoformat(), next_scrape, sid)
        )
        for msg in new_alerts:
            conn.execute(
                "INSERT INTO alerts (search_id, message, created_at) VALUES (?,?,?)",
                (sid, msg, now.isoformat())
            )
        # Keep only last 20 alerts per search
        conn.execute(
            "DELETE FROM alerts WHERE search_id=? AND id NOT IN "
            "(SELECT id FROM alerts WHERE search_id=? ORDER BY id DESC LIMIT 20)",
            (sid, sid)
        )
        conn.commit()
        conn.close()

        LOG.info(f"search #{sid} done: {total_avail} seats, {len(new_alerts)} alerts")


# ═══════════════════════════════════════════════════════════════════════════════
# FastAPI app
# ═══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB
    try:
        conn = db()
        conn.close()
    except Exception as e:
        LOG.error(f"DB init failed: {e}")
    # Start scheduler — even if this fails, the web server should still serve
    try:
        task = asyncio.create_task(scheduler_loop())
        LOG.info("Seat Scout started — scheduler running")
    except Exception as e:
        LOG.error(f"Scheduler failed to start: {e}")
        task = None
    yield
    if task:
        task.cancel()


app = FastAPI(title="Seat Scout", lifespan=lifespan)


# ── Form submit ──────────────────────────────────────────────────────────────

@app.post("/search")
async def create_search(
    movie: str = Form(...),
    city: str = Form(...),
    formats: str = Form("IMAX 70mm,70mm Film"),
    interval: int = Form(DEFAULT_INTERVAL),
    days: int = Form(DAYS_AHEAD_DEFAULT),
):
    try:
        movie_id = resolve_movie(movie)
    except ValueError as e:
        return HTMLResponse(f"<div class='err'>{e}</div>", status_code=400)

    # Allow "City:ZIP" format
    if ":" in city:
        parts = city.split(":", 1)
        city_name = parts[0].strip()
        zip_code = parts[1].strip()
    else:
        city_name = city.strip()
        zip_code = resolve_zip(city_name)

    conn = db()
    # Prevent duplicates
    existing = conn.execute(
        "SELECT id FROM searches WHERE movie=? AND city=? AND formats=? AND active=1",
        (movie, city_name, formats)
    ).fetchone()
    if existing:
        conn.close()
        return RedirectResponse(url=f"/?dup=1", status_code=303)

    conn.execute(
        "INSERT INTO searches (movie, movie_id, city, zip_code, formats, interval_seconds, "
        "days_ahead, active, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (movie, movie_id, city_name, zip_code, formats, interval, days, 1,
         datetime.now().isoformat())
    )
    conn.commit()
    conn.close()
    LOG.info(f"new search: {movie} in {city_name}")
    return RedirectResponse(url="/?created=1", status_code=303)


@app.post("/search/{sid}/delete")
async def delete_search(sid: int):
    conn = db()
    conn.execute("UPDATE searches SET active=0 WHERE id=?", (sid,))
    conn.execute("DELETE FROM showtimes WHERE search_id=?", (sid,))
    conn.execute("DELETE FROM alerts WHERE search_id=?", (sid,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/?deleted=1", status_code=303)


# ── API ──────────────────────────────────────────────────────────────────────

@app.get("/api/searches")
async def api_searches():
    conn = db()
    rows = conn.execute(
        "SELECT * FROM searches WHERE active=1 ORDER BY created_at DESC"
    ).fetchall()
    result = []
    for r in rows:
        avail = conn.execute(
            "SELECT COUNT(*) as c FROM showtimes WHERE search_id=? AND status='available'",
            (r["id"],)
        ).fetchone()["c"]
        result.append({
            "id": r["id"], "movie": r["movie"], "city": r["city"],
            "formats": r["formats"], "interval": r["interval_seconds"],
            "checks": r["checks_completed"],
            "last_scraped": r["last_scraped"],
            "next_scrape": r["next_scrape"],
            "available_seats": avail,
        })
    conn.close()
    return JSONResponse(result)


@app.get("/api/search/{sid}")
async def api_search_detail(sid: int):
    conn = db()
    search = conn.execute("SELECT * FROM searches WHERE id=?", (sid,)).fetchone()
    if not search:
        raise HTTPException(404)
    showtimes = conn.execute(
        "SELECT * FROM showtimes WHERE search_id=? ORDER BY date, time", (sid,)
    ).fetchall()
    alerts = conn.execute(
        "SELECT * FROM alerts WHERE search_id=? ORDER BY id DESC LIMIT 20", (sid,)
    ).fetchall()
    conn.close()
    return JSONResponse({
        "search": dict(search),
        "showtimes": [dict(s) for s in showtimes],
        "alerts": [dict(a) for a in alerts],
    })


@app.get("/api/health")
async def health():
    conn = db()
    n_searches = conn.execute("SELECT COUNT(*) as c FROM searches WHERE active=1").fetchone()["c"]
    n_seats = conn.execute(
        "SELECT COUNT(*) as c FROM showtimes WHERE status='available'"
    ).fetchone()["c"]
    conn.close()
    return {"ok": True, "active_searches": n_searches, "total_available_seats": n_seats}


# ── Dashboard ────────────────────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0d1117; color: #c9d1d9; padding: 16px; max-width: 900px; margin: auto; }
h1 { font-size: 26px; color: #58a6ff; margin-bottom: 4px; }
.sub { color: #8b949e; font-size: 13px; margin-bottom: 20px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
        padding: 16px; margin-bottom: 16px; }
.form-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
input, select, button { padding: 8px 12px; border-radius: 6px; border: 1px solid #30363d;
                        background: #0d1117; color: #c9d1d9; font-size: 14px; }
input[type=text] { flex: 1; min-width: 150px; }
button { background: #238636; border-color: #238636; color: white; cursor: pointer;
         font-weight: 600; padding: 8px 20px; }
button:hover { background: #2ea043; }
button.danger { background: #da3633; border-color: #da3633; padding: 4px 12px; font-size: 12px; }
button.danger:hover { background: #f85149; }
.stat { display: inline-block; margin-right: 24px; }
.stat .num { font-size: 28px; font-weight: 700; color: #58a6ff; }
.stat .label { font-size: 12px; color: #8b949e; }
.search-card { border-left: 3px solid #58a6ff; padding-left: 12px; margin-bottom: 16px; }
.search-header { display: flex; justify-content: space-between; align-items: center; }
.search-title { font-size: 18px; font-weight: 600; color: #f0f6fc; }
.search-meta { color: #8b949e; font-size: 12px; }
.theater { margin: 8px 0; padding: 8px 0; border-bottom: 1px solid #21262d; }
.theater:last-child { border-bottom: none; }
.t-name { font-weight: 600; color: #f0f6fc; }
.t-dist { color: #8b949e; font-size: 12px; margin-left: 8px; }
.st { display: inline-block; padding: 3px 8px; margin: 2px; border-radius: 4px; font-size: 12px; }
.st-avail { background: #1a3a2a; color: #3fb950; }
.st-sold { background: #3a1a1a; color: #f85149; }
.alert { background: #1a3a2a; color: #3fb950; padding: 6px 10px; border-radius: 4px;
         margin: 4px 0; font-size: 13px; }
.alert.sold { background: #3a1a1a; color: #f85149; }
.empty { text-align: center; color: #8b949e; padding: 30px; }
.refresh { color: #8b949e; font-size: 12px; text-align: center; margin-top: 16px; }
.err { color: #f85149; padding: 16px; background: #3a1a1a; border-radius: 6px; margin: 16px 0; }
.flash { color: #3fb950; padding: 8px 12px; background: #1a3a2a; border-radius: 6px; margin-bottom: 12px; }
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard(dup: int = 0, created: int = 0, deleted: int = 0):
    conn = db()
    searches = conn.execute(
        "SELECT * FROM searches WHERE active=1 ORDER BY created_at DESC"
    ).fetchall()

    # Stats
    n_searches = len(searches)
    n_seats = conn.execute(
        "SELECT COUNT(*) as c FROM showtimes WHERE status='available' "
        "AND search_id IN (SELECT id FROM searches WHERE active=1)"
    ).fetchone()["c"]
    n_theaters = conn.execute(
        "SELECT COUNT(DISTINCT theater_name) as c FROM showtimes "
        "WHERE search_id IN (SELECT id FROM searches WHERE active=1)"
    ).fetchone()["c"]

    # Build search cards
    cards = ""
    for s in searches:
        showtimes = conn.execute(
            "SELECT * FROM showtimes WHERE search_id=? ORDER BY date, time", (s["id"],)
        ).fetchall()
        alerts = conn.execute(
            "SELECT * FROM alerts WHERE search_id=? ORDER BY id DESC LIMIT 5", (s["id"],)
        ).fetchall()

        # Group by date
        by_date = {}
        for st in showtimes:
            by_date.setdefault(st["date"], []).append(st)

        theaters_html = ""
        for date in sorted(by_date.keys()):
            sts = by_date[date]
            by_theater = {}
            for st in sts:
                by_theater.setdefault(st["theater_name"], []).append(st)

            for tname, t_sts in by_theater.items():
                st_html = ""
                for st in t_sts:
                    cls = "st-avail" if st["status"] == "available" else "st-sold"
                    st_html += f'<span class="st {cls}">{st["time"]} [{st["format"]}]</span>'
                dist = t_sts[0]["theater_distance"]
                theaters_html += (
                    f'<div class="theater">'
                    f'<div class="t-name">{tname}<span class="t-dist">{dist} mi · {date}</span></div>'
                    f'{st_html}</div>'
                )

        alerts_html = ""
        for a in alerts:
            cls = "alert sold" if "SOLD OUT" in a["message"] else "alert"
            alerts_html += f'<div class="{cls}">{a["message"]}</div>'

        checks = s["checks_completed"]
        last = (s["last_scraped"] or "—")[:19]
        next_s = (s["next_scrape"] or "—")[:19]

        cards += f"""
        <div class="card search-card">
          <div class="search-header">
            <div>
              <div class="search-title">{s["movie"]} · {s["city"]}</div>
              <div class="search-meta">{s["formats"]} · {checks} checks · last: {last} · next: {next_s}</div>
            </div>
            <form method="post" action="/search/{s["id"]}/delete" style="margin:0"
                  onsubmit="return confirm('Remove this search?')">
              <button type="submit" class="danger">Remove</button>
            </form>
          </div>
          {alerts_html}
          {theaters_html if theaters_html else '<div class="empty">Waiting for first scan...</div>'}
        </div>
        """

    conn.close()

    flash = ""
    if created:
        flash = '<div class="flash">Search added! First results will appear in ~30 seconds.</div>'
    elif dup:
        flash = '<div class="flash">You are already monitoring that search.</div>'
    elif deleted:
        flash = '<div class="flash">Search removed.</div>'

    if not searches:
        cards = (
            '<div class="card empty">'
            'No searches yet.<br>'
            'Submit one above to start monitoring.'
            '</div>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Seat Scout</title>
<style>{CSS}</style>
</head>
<body>
<h1>Seat Scout</h1>
<div class="sub">Autonomous seat availability monitor · any movie · any US city · 24/7</div>

<div class="card">
  <form method="post" action="/search">
    <div class="form-row">
      <input type="text" name="movie" placeholder="Movie (e.g. The Odyssey)" required>
      <input type="text" name="city" placeholder="City (e.g. New York) or City:ZIP" required>
    </div>
    <div class="form-row">
      <input type="text" name="formats" value="IMAX 70mm,70mm Film" placeholder="Formats (comma separated)">
      <input type="number" name="interval" value="{DEFAULT_INTERVAL}" min="60" max="3600"
             style="width:90px" title="Seconds between checks">
      <input type="number" name="days" value="{DAYS_AHEAD_DEFAULT}" min="1" max="30"
             style="width:70px" title="Days ahead to scan">
      <button type="submit">Add Search</button>
    </div>
  </form>
</div>

<div class="card">
  <div class="stat"><div class="num">{n_searches}</div><div class="label">active searches</div></div>
  <div class="stat"><div class="num">{n_seats}</div><div class="label">seats available</div></div>
  <div class="stat"><div class="num">{n_theaters}</div><div class="label">theaters</div></div>
</div>

{flash}
{cards}

<div class="refresh">Auto-refreshes every 60s · <span onclick="location.reload()" style="color:#58a6ff;cursor:pointer">refresh now</span></div>
<script>setTimeout(() => location.reload(), 60000);</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# Entry
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
