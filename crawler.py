import json
import time
import hashlib
import logging
from random import uniform
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dateparser

# ----------------------
# Konfiguration
# ----------------------
USER_AGENT = "kidzout-crawler/1.0 (+https://github.com/ElCapitano-builder/kidzout)"
CITY_DEFAULT = {"city": "munich", "region": "BY", "country": "DE"}
OUTPUT_FILE = "data.json"
CONFIG_FILE = "sources.config.json"
REQUEST_TIMEOUT = 20
RATELIMIT_RANGE = (0.7, 1.2)   # Sekunden

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# ----------------------
# Utils
# ----------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha1_16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

def stable_id(title: str, date_iso: str, link: str) -> str:
    return "ev-" + sha1_16(f"{title}|{date_iso}|{link}")

def normalize_date(value) -> str:
    """Beliebige Datumsangaben zu YYYY-MM-DD (UTC) normalisieren."""
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = dateparser.parse(str(value))
        except Exception:
            dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.date().isoformat()

def map_category(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["theater"]):
        return "theater"
    if any(k in t for k in ["museum"]):
        return "museum"
    if any(k in t for k in ["outdoor", "park", "spielplatz", "garten"]):
        return "outdoor"
    if any(k in t for k in ["indoor", "halle"]):
        return "indoor"
    if any(k in t for k in ["workshop", "basteln", "kreativ"]):
        return "kreativ"
    return "event"

def short(text: str, limit: int = 300) -> str:
    text = (text or "").strip().replace("\r", " ").replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"

def http_get(url: str, headers: dict | None = None) -> requests.Response:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    r = requests.get(url, headers=h, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r

def ratelimit_sleep():
    time.sleep(uniform(*RATELIMIT_RANGE))


# ----------------------
# Quellen (Plugins)
# ----------------------
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"rss": [], "html": [], "ical": []}

def harvest_rss(url: str) -> list[dict]:
    logging.info(f"RSS: {url}")
    feed = feedparser.parse(url, request_headers={"User-Agent": USER_AGENT})
    items = []
    for e in feed.entries:
        title = (e.get("title") or "Ohne Titel").strip()
        link = (e.get("link") or "").strip()

        # Datum ermitteln
        dt = e.get("published") or e.get("updated")
        if not dt and getattr(e, "published_parsed", None):
            dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
        date_iso = normalize_date(dt or datetime.now(timezone.utc))

        desc = e.get("summary") or e.get("description") or ""
        cat_src = ""
        try:
            tags = e.get("tags") or []
            if tags and isinstance(tags, list):
                cat_src = tags[0].get("term") or tags[0].get("label") or ""
        except Exception:
            pass

        item = {
            "id": stable_id(title, date_iso, link or url),
            "name": title,
            "date": date_iso,
            "endDate": None,
            "time": None,
            "category": map_category(title + " " + cat_src),
            "description": short(desc, 600),
            **CITY_DEFAULT,
            "bookingRequired": False,
            "bookingUrl": None,
            "price": {"kids": None, "adults": None, "family": None, "note": None},
            "source": url,
            "link": link or url,
            "lastUpdated": now_iso(),
        }
        items.append(item)
    return items

def harvest_html(url: str, selector: str, date_selector: str | None = None) -> list[dict]:
    logging.info(f"HTML: {url}")
    resp = http_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(selector)
    out = []
    for c in cards:
        title_el = c.select_one("h1, h2, h3, .title, .event-title")
        desc_el = c.select_one("p, .description, .text")
        date_el = c.select_one(date_selector) if date_selector else None

        title = (title_el.get_text(strip=True) if title_el else "Ohne Titel")
        desc = (desc_el.get_text(" ", strip=True) if desc_el else "")
        date_iso = normalize_date(date_el.get_text(strip=True)) if date_el else normalize_date(datetime.now(timezone.utc))

        link = ""
        a = c.select_one("a")
        if a and a.get("href"):
            href = a["href"]
            if href.startswith("http"):
                link = href
            else:
                from urllib.parse import urljoin
                link = urljoin(url, href)

        out.append({
            "id": stable_id(title, date_iso, link or url),
            "name": title,
            "date": date_iso,
            "endDate": None,
            "time": None,
            "category": map_category(title + " " + desc),
            "description": short(desc, 600),
            **CITY_DEFAULT,
            "bookingRequired": False,
            "bookingUrl": None,
            "price": {"kids": None, "adults": None, "family": None, "note": None},
            "source": url,
            "link": link or url,
            "lastUpdated": now_iso(),
        })
    return out

def harvest_ical(url: str) -> list[dict]:
    logging.info(f"ICAL: {url}")
    try:
        import icalendar
    except Exception:
        logging.warning("icalendar nicht installiert – überspringe iCal.")
        return []

    resp = http_get(url)
    cal = icalendar.Calendar.from_ical(resp.content)
    out = []
    for comp in cal.subcomponents:
        if comp.name != "VEVENT":
            continue
        title = str(comp.get("summary", "Ohne Titel"))
        dtstart = comp.get("dtstart").dt
        dtend = comp.get("dtend").dt if comp.get("dtend") else None
        link = str(comp.get("url") or "")
        desc = str(comp.get("description") or "")

        date_iso = normalize_date(dtstart)
        end_date = normalize_date(dtend) if dtend else None

        out.append({
            "id": stable_id(title, date_iso, link or url),
            "name": title,
            "date": date_iso,
            "endDate": end_date,
            "time": None,
            "category": map_category(title + " " + desc),
            "description": short(desc, 600),
            **CITY_DEFAULT,
            "bookingRequired": False,
            "bookingUrl": None,
            "price": {"kids": None, "adults": None, "family": None, "note": None},
            "source": url,
            "link": link or url,
            "lastUpdated": now_iso(),
        })
    return out


# ----------------------
# Pipeline
# ----------------------
def get_events_from_all_sources() -> list[dict]:
    cfg = load_config()
    events: list[dict] = []

    # RSS
    for url in cfg.get("rss", []):
        try:
            chunk = harvest_rss(url)
            print(f"RSS OK: {url} → {len(chunk)} Events")     # <- Änderung #4: Logging
            events.extend(chunk)
        except Exception as e:
            logging.error(f"RSS-Fehler {url}: {e}")
        ratelimit_sleep()

    # HTML
    for item in cfg.get("html", []):
        try:
            url = item["url"]
            selector = item["selector"]
            date_selector = item.get("date_selector")
            chunk = harvest_html(url, selector, date_selector)
            print(f"HTML OK: {url} → {len(chunk)} Events")    # <- Änderung #4: Logging
            events.extend(chunk)
        except Exception as e:
            logging.error(f"HTML-Fehler {item}: {e}")
        ratelimit_sleep()

    # iCal
    for url in cfg.get("ical", []):
        try:
            chunk = harvest_ical(url)
            print(f"ICAL OK: {url} → {len(chunk)} Events")    # <- Änderung #4: Logging
            events.extend(chunk)
        except Exception as e:
            logging.error(f"ICAL-Fehler {url}: {e}")
        ratelimit_sleep()

    # Dedupe by id
    dedup = {}
    for ev in events:
        dedup[ev["id"]] = ev
    return list(dedup.values())


def main():
    events = get_events_from_all_sources()

    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"locations": [], "events": []}

    # -------- Änderung #2: Nur überschreiben, wenn Events vorhanden --------
    if events:
        data["events"] = sorted(events, key=lambda e: (e["date"], e["name"]))
        data["totalEvents"] = len(events)
        print(f"✅ {len(events)} Events gespeichert")
    else:
        print("⚠️ 0 Events gefunden – behalte bestehende data.json bei")

    # lastCrawled immer aktualisieren, damit sichtbar ist, dass der Job lief
    data["lastCrawled"] = now_iso()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
