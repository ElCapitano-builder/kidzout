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
RATELIMIT_RANGE = (0.5, 1.0)   # Schneller

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
    """Beliebige Datumsangaben zu YYYY-MM-DD normalisieren."""
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            # Deutsche Datumsformate verstehen
            date_str = str(value)
            # Ersetze deutsche Monatsnamen
            replacements = {
                'Januar': 'January', 'Februar': 'February', 'März': 'March',
                'April': 'April', 'Mai': 'May', 'Juni': 'June',
                'Juli': 'July', 'August': 'August', 'September': 'September',
                'Oktober': 'October', 'November': 'November', 'Dezember': 'December'
            }
            for de, en in replacements.items():
                date_str = date_str.replace(de, en)
            
            dt = dateparser.parse(date_str)
        except Exception:
            dt = datetime.now(timezone.utc)
    
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.date().isoformat()

def map_category(text: str) -> str:
    """Kategorien für Kinder-Events"""
    t = (text or "").lower()
    if any(k in t for k in ["theater", "puppentheater", "kasperl", "bühne", "musical"]):
        return "theater"
    if any(k in t for k in ["museum", "ausstellung", "galerie", "kunst"]):
        return "museum"
    if any(k in t for k in ["spielplatz", "outdoor", "park", "garten", "wandern", "natur", "draußen"]):
        return "outdoor"
    if any(k in t for k in ["indoor", "halle", "drinnen", "spielplatz indoor"]):
        return "indoor"
    if any(k in t for k in ["workshop", "basteln", "kreativ", "malen", "werken", "kurs"]):
        return "kreativ"
    if any(k in t for k in ["schwimmen", "baden", "pool", "freibad", "hallenbad", "wasser"]):
        return "schwimmbad"
    if any(k in t for k in ["sport", "turnen", "fußball", "klettern", "bewegung", "tanz"]):
        return "sport"
    if any(k in t for k in ["musik", "konzert", "singen", "instrument"]):
        return "musik"
    if any(k in t for k in ["kino", "film", "vorführung"]):
        return "kino"
    if any(k in t for k in ["fest", "festival", "markt", "feier"]):
        return "festival"
    return "event"

def short(text: str, limit: int = 300) -> str:
    text = (text or "").strip().replace("\r", " ").replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"

def http_get(url: str, headers: dict | None = None) -> requests.Response:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    try:
        r = requests.get(url, headers=h, timeout=REQUEST_TIMEOUT, verify=False)
        r.raise_for_status()
        return r
    except Exception as e:
        logging.error(f"HTTP Fehler bei {url}: {e}")
        raise

def ratelimit_sleep():
    time.sleep(uniform(*RATELIMIT_RANGE))

def enrich_for_kids(item):
    """Macht Events kinderfreundlich und fügt KidzOut-spezifische Felder hinzu"""
    text = (item.get('name', '') + ' ' + item.get('description', '')).lower()
    
    # Altersgruppen-Erkennung
    age_groups = []
    if any(word in text for word in ['baby', 'kleinkind', 'ab 1', 'ab 2', 'krippe', 'krabbelgruppe', '0-3', 'u3']):
        age_groups.append("0-3")
    if any(word in text for word in ['kindergarten', 'ab 3', 'ab 4', 'ab 5', 'vorschule', 'kita', '3-6']):
        age_groups.append("3-6")
    if any(word in text for word in ['grundschule', 'ab 6', 'ab 7', 'ab 8', 'schulkind', '6-9', 'erstklässler']):
        age_groups.append("6-9")
    if any(word in text for word in ['ab 9', 'ab 10', 'ab 11', 'ab 12', 'teenager', 'jugend', '9-12']):
        age_groups.append("9-12")
    
    if not age_groups:
        # Standard basierend auf Kategorie
        if item['category'] in ['theater', 'museum']:
            age_groups = ["3-6", "6-9"]
        elif item['category'] in ['sport', 'kreativ']:
            age_groups = ["6-9", "9-12"]
        else:
            age_groups = ["3-6", "6-9", "9-12"]
    
    # Kinderfreundlicher Name
    name_short = item['name'][:50]
    if 'theater' in text or 'kasperl' in text:
        item['nameKids'] = f"🎭 {name_short}"
    elif 'workshop' in text or 'basteln' in text:
        item['nameKids'] = f"🎨 {name_short}"
    elif 'musik' in text or 'konzert' in text:
        item['nameKids'] = f"🎵 {name_short}"
    elif 'sport' in text or 'bewegung' in text:
        item['nameKids'] = f"⚽ {name_short}"
    elif 'museum' in text:
        item['nameKids'] = f"🏛️ {name_short}"
    else:
        item['nameKids'] = f"🎉 {name_short}"
    
    item['ageGroups'] = age_groups
    
    # Eltern-Tipps
    item['parentTips'] = [
        "Rechtzeitig da sein - beliebte Events sind schnell voll",
        "Snacks und Getränke mitbringen",
        "Mit Öffis anreisen wenn möglich"
    ]
    
    # Wetterabhängigkeit
    if any(word in text for word in ['draußen', 'outdoor', 'garten', 'park', 'spielplatz', 'wandern']):
        item['weatherDependent'] = 'good-weather'
    elif any(word in text for word in ['drinnen', 'indoor', 'halle', 'museum', 'theater']):
        item['weatherDependent'] = 'indoor'
    else:
        item['weatherDependent'] = 'any'
    
    # Energie-Level
    if any(word in text for word in ['sport', 'toben', 'klettern', 'rennen', 'action', 'trampolin']):
        item['energyLevel'] = 'aktiv'
    elif any(word in text for word in ['basteln', 'malen', 'lesen', 'märchen', 'ruhig']):
        item['energyLevel'] = 'ruhig'
    else:
        item['energyLevel'] = 'moderat'
    
    return item


# ----------------------
# Verbesserte HTML-Harvest Funktion
# ----------------------
def harvest_html(url: str, selector: str, date_selector: str | None = None, 
                 title_selector: str | None = None, desc_selector: str | None = None) -> list[dict]:
    """Flexibler HTML-Crawler der verschiedene Strukturen versteht"""
    print(f"\n🔍 Crawle: {url}")
    
    try:
        resp = http_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Debug: Zeige Seitentitel
        page_title = soup.find('title')
        if page_title:
            print(f"   Seite: {page_title.text[:50]}")
        
        # Erweiterte Selektoren für bessere Trefferquote
        selectors_to_try = [
            # Original
            selector,
            # Allgemeine Event-Selektoren
            "div[class*='event']",
            "div[class*='veranstaltung']",
            "article[class*='event']",
            "article[class*='teaser']",
            "div[class*='teaser']",
            "div[class*='item']",
            "div[class*='entry']",
            "div[class*='card']",
            "li[class*='event']",
            "section[class*='event']",
            # München.de spezifisch
            "div.m-teaser",
            "article.m-teaser",
            ".event-list-item",
            # Kindaling spezifisch  
            ".event-card",
            ".activity-card",
            # Allgemeine Container
            ".row .col",
            ".list-item",
            ".post",
            # Links zu Events
            "a[href*='/event']",
            "a[href*='/veranstaltung']"
        ]
        
        found_elements = []
        for sel in selectors_to_try:
            try:
                elements = soup.select(sel)
                if elements:
                    print(f"   ✓ Gefunden mit '{sel}': {len(elements)} Elemente")
                    found_elements.extend(elements[:10])  # Max 10 pro Selektor
                    if len(found_elements) > 30:
                        break
            except Exception:
                continue
        
        # Deduplizieren
        seen = set()
        unique_elements = []
        for elem in found_elements:
            elem_text = elem.get_text(strip=True)[:100]
            if elem_text not in seen and len(elem_text) > 20:
                seen.add(elem_text)
                unique_elements.append(elem)
        
        print(f"   📊 {len(unique_elements)} unique Elemente nach Deduplizierung")
        
        # Fallback: Wenn nichts gefunden, suche alle Links mit relevanten Keywords
        if not unique_elements:
            print("   ⚠️ Keine Elemente mit Selektoren gefunden, verwende Fallback...")
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                text = link.get_text(strip=True)
                href = link.get('href', '')
                if any(keyword in text.lower() for keyword in ['kind', 'familie', 'event', 'workshop', 'kurs', 'theater', 'museum']):
                    if len(text) > 20 and len(text) < 200:
                        unique_elements.append(link)
                        if len(unique_elements) >= 20:
                            break
            print(f"   📊 {len(unique_elements)} Links mit Keywords gefunden")
        
        # Extrahiere Events
        events = []
        for elem in unique_elements[:30]:
            try:
                # Titel finden - verschiedene Strategien
                title = ""
                
                # Strategie 1: Spezifische Title-Selektoren
                if title_selector:
                    title_elem = elem.select_one(title_selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                
                # Strategie 2: Überschriften suchen
                if not title:
                    for tag in ['h1', 'h2', 'h3', 'h4', 'h5']:
                        heading = elem.find(tag)
                        if heading:
                            title = heading.get_text(strip=True)
                            break
                
                # Strategie 3: Links mit Text
                if not title:
                    link = elem.find('a')
                    if link:
                        title = link.get_text(strip=True)
                
                # Strategie 4: Klassen-basiert
                if not title:
                    for class_name in ['title', 'headline', 'name', 'event-title']:
                        title_elem = elem.find(class_=lambda x: x and class_name in x)
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            break
                
                # Strategie 5: Erster sinnvoller Text
                if not title:
                    texts = elem.stripped_strings
                    for text in texts:
                        if len(text) > 10 and len(text) < 100:
                            title = text
                            break
                
                if not title or len(title) < 5:
                    continue
                
                # Beschreibung finden
                desc = ""
                if desc_selector:
                    desc_elem = elem.select_one(desc_selector)
                    if desc_elem:
                        desc = desc_elem.get_text(strip=True)
                
                if not desc:
                    all_text = elem.get_text(' ', strip=True)
                    desc = all_text[:500]
                
                # Datum finden
                date_iso = None
                if date_selector:
                    date_elem = elem.select_one(date_selector)
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        date_iso = normalize_date(date_text)
                
                if not date_iso:
                    # Suche nach Datums-Patterns im Text
                    import re
                    date_patterns = [
                        r'\d{1,2}\.\d{1,2}\.\d{2,4}',  # DD.MM.YYYY
                        r'\d{1,2}\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)',
                        r'\d{4}-\d{2}-\d{2}'  # YYYY-MM-DD
                    ]
                    
                    all_text = elem.get_text()
                    for pattern in date_patterns:
                        match = re.search(pattern, all_text)
                        if match:
                            date_iso = normalize_date(match.group())
                            break
                
                if not date_iso:
                    date_iso = normalize_date(datetime.now())
                
                # Link finden
                link_elem = elem.find('a', href=True)
                if link_elem:
                    link = urljoin(url, link_elem['href'])
                else:
                    link = url
                
                # Event erstellen
                event = {
                    "id": stable_id(title, date_iso, link),
                    "name": title[:200],
                    "date": date_iso,
                    "endDate": None,
                    "time": None,
                    "category": map_category(title + " " + desc),
                    "description": short(desc, 500),
                    **CITY_DEFAULT,
                    "bookingRequired": False,
                    "bookingUrl": None,
                    "price": {"kids": None, "adults": None, "family": None, "note": "Preis auf Webseite prüfen"},
                    "source": url,
                    "link": link,
                    "lastUpdated": now_iso(),
                }
                
                # KidzOut-Anreicherung
                event = enrich_for_kids(event)
                events.append(event)
                
            except Exception as e:
                logging.debug(f"Fehler beim Parsen eines Elements: {e}")
                continue
        
        print(f"   ✅ {len(events)} Events extrahiert")
        return events
        
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return []


# ----------------------
# RSS Handler (funktioniert besser!)
# ----------------------
def harvest_rss(url: str) -> list[dict]:
    print(f"\n📡 RSS Feed: {url}")
    try:
        feed = feedparser.parse(url, request_headers={"User-Agent": USER_AGENT})
        items = []
        
        for entry in feed.entries[:50]:  # Max 50 Items
            title = entry.get("title", "Ohne Titel").strip()
            link = entry.get("link", "").strip()
            
            # Datum
            dt = entry.get("published") or entry.get("updated")
            if not dt and hasattr(entry, "published_parsed"):
                dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            date_iso = normalize_date(dt or datetime.now())
            
            desc = entry.get("summary") or entry.get("description") or ""
            
            item = {
                "id": stable_id(title, date_iso, link or url),
                "name": title,
                "date": date_iso,
                "category": map_category(title + " " + desc),
                "description": short(desc, 600),
                **CITY_DEFAULT,
                "source": url,
                "link": link or url,
                "lastUpdated": now_iso(),
            }
            
            item = enrich_for_kids(item)
            items.append(item)
        
        print(f"   ✅ {len(items)} Events aus RSS")
        return items
    except Exception as e:
        print(f"   ❌ RSS Fehler: {e}")
        return []


# ----------------------
# Config laden
# ----------------------
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # Fallback Config mit funktionierenden Quellen
        return {
            "rss": [
                "https://www.muenchen.de/system/feeds/veranstaltungen/kinder.rss"
            ],
            "html": [
                {
                    "url": "https://www.muenchen.de/veranstaltungen/kinder",
                    "selector": "article, .m-teaser, .event",
                    "date_selector": "time, .date",
                    "title_selector": "h2, h3, .m-teaser__headline",
                    "desc_selector": "p, .m-teaser__text"
                }
            ],
            "ical": []
        }


# ----------------------
# Pipeline
# ----------------------
def get_events_from_all_sources() -> list[dict]:
    cfg = load_config()
    events = []
    
    print("\n" + "="*50)
    print("🚀 KidzOut Crawler gestartet!")
    print(f"📋 Config: {len(cfg.get('rss', []))} RSS, {len(cfg.get('html', []))} HTML, {len(cfg.get('ical', []))} iCal")
    print("="*50)

    # RSS (funktioniert am besten)
    for url in cfg.get("rss", []):
        try:
            chunk = harvest_rss(url)
            events.extend(chunk)
        except Exception as e:
            logging.error(f"RSS-Fehler {url}: {e}")
        ratelimit_sleep()

    # HTML
    for item in cfg.get("html", []):
        try:
            if isinstance(item, dict):
                url = item["url"]
                selector = item.get("selector", "article, .event, .teaser")
                date_selector = item.get("date_selector")
                title_selector = item.get("title_selector")
                desc_selector = item.get("desc_selector")
                chunk = harvest_html(url, selector, date_selector, title_selector, desc_selector)
            else:
                chunk = harvest_html(item, "article, .event", None, None, None)
            
            events.extend(chunk)
        except Exception as e:
            logging.error(f"HTML-Fehler {item}: {e}")
        ratelimit_sleep()

    # Deduplizierung
    dedup = {}
    for ev in events:
        # Verbesserte Deduplizierung - basierend auf Titel + Datum
        dedup_key = f"{ev['name'][:30]}_{ev['date']}"
        dedup[dedup_key] = ev
    
    unique_events = list(dedup.values())
    
    print("\n" + "="*50)
    print(f"📊 ERGEBNIS: {len(unique_events)} unique Events gefunden")
    print("="*50 + "\n")
    
    return unique_events


def main():
    print("\n🎯 KidzOut Crawler v2.0")
    events = get_events_from_all_sources()

    # Lade bestehende Daten
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"locations": [], "events": []}

    # Update
    if events:
        data["events"] = sorted(events, key=lambda e: (e["date"], e["name"]))
        data["totalEvents"] = len(events)
        print(f"\n✅ {len(events)} Events gespeichert in data.json")
    else:
        print("\n⚠️ Keine Events gefunden")

    # Metadata
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
    
    # Zeige erste 3 Events als Beispiel
    if events and len(events) > 0:
        print("\n📅 Beispiel-Events:")
        for ev in events[:3]:
            print(f"  - {ev['name'][:50]} ({ev['date']})")

if __name__ == "__main__":
    main()
