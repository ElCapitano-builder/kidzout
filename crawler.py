import json
import time
import hashlib
import logging
from random import uniform
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import parser as dateparser

# ----------------------
# Konfiguration
# ----------------------
USER_AGENT = "kidzout-crawler/1.0 (+https://github.com/ElCapitano-builder/kidzout)"
CITY_DEFAULT = {"city": "München", "region": "BY", "country": "DE"}
OUTPUT_FILE = "data.json"
CONFIG_FILE = "sources_config.json"
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
    """Kategorien für Kinder-Events"""
    t = (text or "").lower()
    if any(k in t for k in ["theater", "puppentheater", "kasperltheater", "bühne"]):
        return "theater"
    if any(k in t for k in ["museum", "ausstellung", "galerie"]):
        return "museum"
    if any(k in t for k in ["outdoor", "park", "spielplatz", "garten", "wandern", "natur"]):
        return "outdoor"
    if any(k in t for k in ["indoor", "halle", "drinnen"]):
        return "indoor"
    if any(k in t for k in ["workshop", "basteln", "kreativ", "malen", "werken"]):
        return "kreativ"
    if any(k in t for k in ["schwimmen", "baden", "pool", "freibad", "hallenbad"]):
        return "schwimmbad"
    if any(k in t for k in ["sport", "turnen", "fußball", "klettern", "bewegung"]):
        return "sport"
    if any(k in t for k in ["musik", "konzert", "singen"]):
        return "musik"
    if any(k in t for k in ["kino", "film"]):
        return "kino"
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

def enrich_for_kids(item):
    """Macht Events kinderfreundlich und fügt KidzOut-spezifische Felder hinzu"""
    text = (item.get('name', '') + ' ' + item.get('description', '')).lower()
    
    # Altersgruppen-Erkennung
    age_groups = []
    if any(word in text for word in ['baby', 'kleinkind', 'ab 1', 'ab 2', 'krippe', 'krabbelgruppe']):
        age_groups.append("0-3")
    if any(word in text for word in ['kindergarten', 'ab 3', 'ab 4', 'ab 5', 'vorschule', 'kita']):
        age_groups.append("3-6")
    if any(word in text for word in ['grundschule', 'ab 6', 'ab 7', 'ab 8', 'schulkind']):
        age_groups.append("6-9")
    if any(word in text for word in ['ab 9', 'ab 10', 'ab 11', 'ab 12', 'teenager', 'jugend']):
        age_groups.append("9-12")
    
    # Wenn keine spezifische Altersgruppe gefunden, basierend auf Event-Typ
    if not age_groups:
        if any(word in text for word in ['kasperle', 'puppentheater', 'märchen']):
            age_groups = ["3-6", "6-9"]
        elif any(word in text for word in ['workshop', 'basteln']):
            age_groups = ["6-9", "9-12"]
        elif any(word in text for word in ['konzert', 'musik']):
            age_groups = ["3-6", "6-9", "9-12"]
        else:
            age_groups = ["3-6", "6-9"]  # Default
    
    # Kinderfreundlicher Name
    if 'kasperle' in text:
        item['nameKids'] = "Kasperle-Abenteuer! 🎭"
    elif 'workshop' in text:
        item['nameKids'] = "Bastel-Spaß! 🎨"
    elif 'musik' in text or 'konzert' in text:
        item['nameKids'] = "Musik-Erlebnis! 🎵"
    elif 'theater' in text:
        item['nameKids'] = "Theater-Zauber! 🎪"
    else:
        item['nameKids'] = item['name'][:50] + " - Spaß für Kids!"
    
    item['ageGroups'] = age_groups
    
    # Eltern-Tipps (generisch, später durch KI ersetzen)
    item['parentTips'] = [
        "Rechtzeitig da sein - beliebte Events sind schnell voll",
        "Snacks und Getränke mitbringen schadet nie",
        "Mit Öffis anreisen - Parkplätze oft knapp"
    ]
    
    # Wetterabhängigkeit
    if any(word in text for word in ['draußen', 'outdoor', 'garten', 'park', 'spielplatz', 'wandern']):
        item['weatherDependent'] = 'good-weather'
    elif any(word in text for word in ['drinnen', 'indoor', 'halle', 'museum', 'theater']):
        item['weatherDependent'] = 'indoor'
    else:
        item['weatherDependent'] = 'any'
    
    # Kinderfreundliche Beschreibungen nach Alter
    base_desc = item.get('description', '')[:200]
    item['descriptions'] = {
        'age3to6': f"Ein tolles Erlebnis für kleine Entdecker! {base_desc}",
        'age6to9': f"Spannendes Abenteuer für Schulkinder! {base_desc}",
        'age9to12': f"Coole Action für größere Kids! {base_desc}"
    }
    
    # Energie-Level
    if any(word in text for word in ['sport', 'toben', 'klettern', 'rennen', 'action']):
        item['energyLevel'] = 'aktiv'
    elif any(word in text for word in ['basteln', 'malen', 'lesen', 'märchen']):
        item['energyLevel'] = 'ruhig'
    else:
        item['energyLevel'] = 'moderat'
    
    return item


# ----------------------
# Quellen (Plugins)
# ----------------------
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # Fallback auf München-Kinder-Event-Quellen
        return {
            "rss": [],
            "html": [
                {
                    "url": "https://www.muenchen.de/veranstaltungen/event/kinder",
                    "selector": "article, .event-item, .teaser",
                    "date_selector": "time, .date",
                    "title_selector": "h2, h3, .title",
                    "desc_selector": "p, .description"
                }
            ],
            "ical": []
        }

def harvest_rss(url: str) -> list[dict]:
    logging.info(f"RSS: {url}")
    feed = feedparser.parse(url, request_headers={"User-Agent": USER_AGENT})
    items = []
    for e in feed.entries:
        title = (e.get("title") or "Ohne Titel").strip()
        link = (e.get("link") or "").strip()

        dt = e.get("published") or e.get("updated")
        if not dt and getattr(e, "published_parsed", None):
            dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
        date_iso = normalize_date(dt or datetime.now(timezone.utc))

        desc = e.get("summary") or e.get("description") or ""
        
        item = {
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
            "price": {"kids": None, "adults": None, "family": None, "note": "Preis auf Webseite prüfen"},
            "source": url,
            "link": link or url,
            "lastUpdated": now_iso(),
        }
        
        # KidzOut-Anreicherung
        item = enrich_for_kids(item)
        items.append(item)
        
    return items

def harvest_html(url: str, selector: str, date_selector: str | None = None, 
                 title_selector: str | None = None, desc_selector: str | None = None) -> list[dict]:
    logging.info(f"HTML: {url}")
    try:
        resp = http_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select(selector)
        out = []
        
        for c in cards[:30]:  # Max 30 Events pro Seite
            # Titel
            if title_selector:
                title_el = c.select_one(title_selector)
            else:
                title_el = c.select_one("h1, h2, h3, .title, .event-title, .headline")
            
            # Beschreibung
            if desc_selector:
                desc_el = c.select_one(desc_selector)
            else:
                desc_el = c.select_one("p, .description, .text, .teaser, .summary")
            
            # Datum
            if date_selector:
                date_el = c.select_one(date_selector)
            else:
                date_el = c.select_one("time, .date, .datum, .when")

            title = (title_el.get_text(strip=True) if title_el else "")
            if not title:
                continue  # Skip wenn kein Titel
                
            desc = (desc_el.get_text(" ", strip=True) if desc_el else "")
            
            # Datum parsen
            if date_el:
                if date_el.get("datetime"):
                    date_str = date_el["datetime"]
                else:
                    date_str = date_el.get_text(strip=True)
                date_iso = normalize_date(date_str)
            else:
                date_iso = normalize_date(datetime.now(timezone.utc))

            # Link extrahieren
            link = ""
            a = c.select_one("a[href]")
            if a:
                href = a["href"]
                if href.startswith("http"):
                    link = href
                else:
                    link = urljoin(url, href)

            item = {
                "id": stable_id(title, date_iso, link or url),
                "name": title,
                "date": date_iso,
                "endDate": None,
                "time": None,
                "category": map_category(title + " " + desc),
                "description": short(desc, 600),
                **CITY_DEFAULT,
                "bookingRequired": False,
                "bookingUrl": link if "ticket" in link.lower() else None,
                "price": {"kids": None, "adults": None, "family": None, "note": "Preis auf Webseite prüfen"},
                "source": url,
                "link": link or url,
                "lastUpdated": now_iso(),
            }
            
            # KidzOut-Anreicherung
            item = enrich_for_kids(item)
            out.append(item)
            
    except Exception as e:
        logging.error(f"HTML-Fehler bei {url}: {e}")
        
    return out

def harvest_ical(url: str) -> list[dict]:
    logging.info(f"ICAL: {url}")
    try:
        import icalendar
    except Exception:
        logging.warning("icalendar nicht installiert – überspringe iCal.")
        return []

    try:
        resp = http_get(url)
        cal = icalendar.Calendar.from_ical(resp.content)
        out = []
        
        for comp in cal.subcomponents:
            if comp.name != "VEVENT":
                continue
                
            title = str(comp.get("summary", "Ohne Titel"))
            dtstart = comp.get("dtstart").dt if comp.get("dtstart") else datetime.now(timezone.utc)
            dtend = comp.get("dtend").dt if comp.get("dtend") else None
            link = str(comp.get("url") or "")
            desc = str(comp.get("description") or "")
            location = str(comp.get("location") or "")

            date_iso = normalize_date(dtstart)
            end_date = normalize_date(dtend) if dtend else None

            item = {
                "id": stable_id(title, date_iso, link or url),
                "name": title,
                "date": date_iso,
                "endDate": end_date,
                "time": None,
                "location": location,
                "category": map_category(title + " " + desc),
                "description": short(desc, 600),
                **CITY_DEFAULT,
                "bookingRequired": False,
                "bookingUrl": None,
                "price": {"kids": None, "adults": None, "family": None, "note": "Preis auf Webseite prüfen"},
                "source": url,
                "link": link or url,
                "lastUpdated": now_iso(),
            }
            
            # KidzOut-Anreicherung
            item = enrich_for_kids(item)
            out.append(item)
            
    except Exception as e:
        logging.error(f"ICAL-Fehler bei {url}: {e}")
        
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
            print(f"RSS OK: {url} → {len(chunk)} Events")
            events.extend(chunk)
        except Exception as e:
            logging.error(f"RSS-Fehler {url}: {e}")
        ratelimit_sleep()

    # HTML
    for item in cfg.get("html", []):
        try:
            if isinstance(item, dict):
                url = item["url"]
                selector = item["selector"]
                date_selector = item.get("date_selector")
                title_selector = item.get("title_selector")
                desc_selector = item.get("desc_selector")
                chunk = harvest_html(url, selector, date_selector, title_selector, desc_selector)
            else:
                # Falls nur URL als String
                chunk = harvest_html(item, "article, .event", None, None, None)
            print(f"HTML OK: {url if isinstance(item, dict) else item} → {len(chunk)} Events")
            events.extend(chunk)
        except Exception as e:
            logging.error(f"HTML-Fehler {item}: {e}")
        ratelimit_sleep()

    # iCal
    for url in cfg.get("ical", []):
        try:
            chunk = harvest_ical(url)
            print(f"ICAL OK: {url} → {len(chunk)} Events")
            events.extend(chunk)
        except Exception as e:
            logging.error(f"ICAL-Fehler {url}: {e}")
        ratelimit_sleep()

    # Deduplizierung nach ID
    dedup = {}
    for ev in events:
        dedup[ev["id"]] = ev
    
    return list(dedup.values())


def main():
    print("🚀 KidzOut Crawler startet...")
    events = get_events_from_all_sources()

    # Lade bestehende Daten
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"locations": [], "events": []}

    # Nur überschreiben wenn Events gefunden
    if events:
        data["events"] = sorted(events, key=lambda e: (e["date"], e["name"]))
        data["totalEvents"] = len(events)
        print(f"✅ {len(events)} Events gespeichert")
    else:
        print("⚠️ Keine Events gefunden – behalte bestehende data.json")

    # Metadata aktualisieren
    data["lastCrawled"] = now_iso()
    data["metadata"] = {
        "version": "1.0",
        "lastCrawled": now_iso(),
        "totalLocations": len(data.get("locations", [])),
        "totalEvents": len(events),
        "sources": load_config()
    }

    # Speichern
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"📅 Nächstes Update: siehe GitHub Actions Schedule")

if __name__ == "__main__":
    main()
