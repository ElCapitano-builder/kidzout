# KidzOut Crawler Status

## Current Situation (November 2025)

### The Problem: Cloudflare Bot Protection

Fast alle München-Event-Websites nutzen **Cloudflare** oder ähnliche Bot-Protection-Services. Diese blockieren automatisierte Zugriffe (HTTP 403 Forbidden).

**Betroffene Sources:** ~95% aller konfigurierten Quellen in `sources.config.json`

### Was wurde versucht:

✅ **Realistische Browser Headers**
- Sec-Fetch-* Headers
- Referer Headers
- Accept-Encoding: br
- Cache-Control

✅ **User-Agent Rotation**
- 5 verschiedene moderne Browser User-Agents
- Random selection bei jedem Request

✅ **Intelligent Rate Limiting**
- 4 Sekunden Delay pro Domain
- ±20% Random Jitter für menschliches Verhalten
- Dynamisches Backoff bei Fehlern

✅ **RSS Parser ohne feedparser**
- Custom XML Parser mit BeautifulSoup + lxml
- Unterstützt RSS 2.0 und Atom Feeds

**Ergebnis:** Cloudflare erkennt trotzdem den Bot und blockiert mit 403

---

## Lösungen für Production

### Option 1: Headless Browser (Selenium/Playwright) ⭐ Empfohlen für Start

**Vorteile:**
- Umgeht die meisten Bot-Detections
- Kann JavaScript-basierte Seiten crawlen
- Cookies und Sessions werden automatisch gehandled

**Nachteile:**
- Langsamer (5-10x)
- Höherer Ressourcen-Verbrauch (RAM, CPU)
- Komplexere Deployment (braucht Browser-Installation)

**Implementation:**
```python
# Mit Playwright:
pip install playwright
playwright install chromium

# Im Crawler:
from playwright.async_api import async_playwright

async def crawl_with_browser(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        content = await page.content()
        await browser.close()
        return content
```

**Kosten:** Kostenlos, aber höhere Server-Kosten durch Ressourcen

---

### Option 2: Scraping Service (ScrapingBee, Bright Data, etc.)

**Vorteile:**
- Professionell, hohe Success-Rate
- Automatisches Proxy-Rotation
- Managed Service (kein Maintenance)
- API-basiert (einfach zu integrieren)

**Nachteile:**
- 💰 **Kostet Geld**
- ScrapingBee: ~49€/Monat für 100k requests
- Bright Data: Pay-as-you-go, ~$0.001 pro Request

**Implementation:**
```python
import requests

def scrape_with_scrapingbee(url):
    api_key = "YOUR_API_KEY"
    resp = requests.get(
        'https://app.scrapingbee.com/api/v1/',
        params={
            'api_key': api_key,
            'url': url,
            'render_js': 'false'
        }
    )
    return resp.text
```

---

### Option 3: Fokus auf API-basierte Quellen

**Strategie:** Nur Quellen nutzen die APIs oder offene Daten anbieten

**Beispiele:**
- München Open Data Portal
- Eventbrite API (neu, nicht die alte deprecated)
- Google Places API (kostet nach Freibetrag)
- Facebook Events API
- Meetup.com API

**Vorteile:**
- Zuverlässig, strukturiert
- Keine Bot-Detection
- Meist schneller

**Nachteile:**
- Weniger Quellen verfügbar
- Oft kostenpflichtig nach Freibetrag
- API-Limits

---

### Option 4: Hybrid-Ansatz (MVP Empfehlung 🎯)

**Phase 1 - Jetzt:**
- Frontend mit Sample-Daten entwickeln (`create_sample_data.py`)
- Crawler-Code ist bereit, wartet nur auf Bot-Protection-Lösung

**Phase 2 - Beta:**
- Selenium/Playwright für wichtigste 10-15 Quellen
- 1x täglich crawlen (nicht 4x)
- Caching nutzen

**Phase 3 - Production:**
- Bei Erfolg: Scraping Service (ScrapingBee o.ä.)
- Oder: Partnerships mit Venues (direkter Datenzugang)

---

## Current MVP Status

### ✅ Was funktioniert:

1. **Crawler Infrastructure**
   - Smart Rate Limiting ✅
   - User-Agent Rotation ✅
   - Session Management ✅
   - Retry Logic ✅
   - Parallel Processing (5 Workers) ✅
   - Source Quality Tracking ✅

2. **Data Processing**
   - Event Enrichment (kinderfreundliche Namen, Altersgruppen) ✅
   - Location Geocoding (OpenStreetMap) ✅
   - Opening Hours Parsing ✅
   - Category Mapping ✅

3. **Sample Data**
   - 5 realistische Events ✅
   - 3 Locations ✅
   - Alle Felder korrekt ausgefüllt ✅
   - Frontend kann sofort entwickelt werden ✅

### 🔧 Was noch fehlt:

1. **Bot-Protection umgehen** - Siehe Optionen oben
2. **Produktiv-Daten** - Aktuell nur Sample-Daten

---

## Nächste Schritte

### Sofort:
```bash
# Sample Data nutzen für Frontend-Development
python3 create_sample_data.py

# data.json ist jetzt gefüllt mit 5 Events + 3 Locations
# Frontend kann entwickelt werden!
```

### Für Production:
1. **Entscheidung treffen:** Selenium vs. Scraping Service
2. **Budget klären:** Kostenlos (Selenium) vs. ~50€/Monat (Service)
3. **Implementation:** Je nach Wahl aus Optionen 1-4

---

## Technische Details

### Crawler Configuration

**File:** `sources.config.json`
- 73 HTML Sources
- 4 RSS Feeds
- 1 iCal Feed
- 5 Location Sources
- **Total: 78 Sources**

### Crawler Features (v4.1)

```python
# Key Classes:
- SmartRateLimiter: Per-Domain Rate Limiting
- SessionManager: HTTP Sessions + Retry Logic
- StructuredDataExtractor: JSON-LD Parsing
- SourceQualityTracker: Success/Failure Tracking
- Geocoder: OpenStreetMap Geocoding + Caching
- OpeningHoursParser: Regex-based Hour Parsing

# Processing Pipeline:
1. Load sources from config
2. Parallel crawling (5 workers)
3. HTML/RSS/iCal parsing
4. Data enrichment (kids-friendly)
5. Geocoding (locations)
6. Save to data.json
```

### GitHub Actions

**File:** `.github/workflows/crawler.yml`
- Runs: 4x daily (06:00, 10:00, 14:00, 18:00 UTC)
- Auto-commits: `data.json` + `geocode_cache.json`
- Dependencies: requests, beautifulsoup4, python-dateutil, lxml

---

## Kontakt / Fragen

Bei Fragen zur Crawler-Implementation oder Bot-Protection-Lösungen:
- Check GitHub Issues
- Siehe Code-Kommentare in `crawler.py`
- Review this document

**Stand:** November 2025
**Version:** 4.1
**Status:** Sample Data Ready, Production Needs Bot-Protection Solution
