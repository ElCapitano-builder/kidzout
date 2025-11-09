"""
KidzOut Event & Location Crawler v4.1 - Enterprise Edition
===========================================================
Features:
- Smart Rate Limiting mit per-domain tracking
- User-Agent Rotation
- Strukturierte Daten-Extraktion (JSON-LD, Schema.org)
- Paralleles Multi-Threading
- Self-Healing mit Retry-Strategien
- Source Quality Tracking
- Advanced HTML Parsing
- Session Management
- 🆕 Location Harvesting (Spielplätze, Museen, etc.)
- 🆕 Geocoding mit OpenStreetMap Nominatim
- 🆕 Opening Hours Parser
"""

import json
import time
import hashlib
import logging
import os
import random
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import requests
from bs4 import BeautifulSoup
try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
from dateutil import parser as dateparser

# ----------------------
# Konfiguration
# ----------------------
CITY_DEFAULT = {"city": "München", "region": "BY", "country": "DE"}
OUTPUT_FILE = "data.json"
CONFIG_FILE = "sources.config.json"
STATS_FILE = "crawler_stats.json"
GEOCODE_CACHE_FILE = "geocode_cache.json"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 10]  # Sekunden
MAX_WORKERS = 5  # Parallel threads
RATE_LIMIT_DEFAULT = 4.0  # Sekunden zwischen Requests pro Domain (erhöht gegen Bot-Detection)

# Geocoding
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "KidzOut-Crawler/4.1 (+https://github.com/ElCapitano-builder/kidzout)"

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ----------------------
# User-Agent Rotation
# ----------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


# ----------------------
# Rate Limiter
# ----------------------
class SmartRateLimiter:
    """Per-Domain Rate Limiting mit intelligenten Backoffs"""

    def __init__(self):
        self.last_request: Dict[str, float] = {}
        self.failure_count: Dict[str, int] = defaultdict(int)
        self.rate_limits: Dict[str, float] = {}

    def get_domain(self, url: str) -> str:
        return urlparse(url).netloc

    def wait(self, url: str):
        """Warte basierend auf Domain und Fehler-Historie"""
        domain = self.get_domain(url)

        # Dynamisches Rate-Limit basierend auf Fehlern
        base_limit = self.rate_limits.get(domain, RATE_LIMIT_DEFAULT)
        failure_multiplier = 1 + (self.failure_count[domain] * 0.5)
        limit = min(base_limit * failure_multiplier, 10.0)

        # Add random jitter (±20%) to look more human
        jitter = random.uniform(0.8, 1.2)
        limit = limit * jitter

        if domain in self.last_request:
            elapsed = time.time() - self.last_request[domain]
            if elapsed < limit:
                wait_time = limit - elapsed
                time.sleep(wait_time)

        self.last_request[domain] = time.time()

    def record_success(self, url: str):
        domain = self.get_domain(url)
        if self.failure_count[domain] > 0:
            self.failure_count[domain] = max(0, self.failure_count[domain] - 1)

    def record_failure(self, url: str):
        domain = self.get_domain(url)
        self.failure_count[domain] += 1


# ----------------------
# Session Manager
# ----------------------
class SessionManager:
    """Verwaltet HTTP Sessions mit Retry-Logik"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        })
        self.rate_limiter = SmartRateLimiter()

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """GET mit Retry-Logik"""
        self.rate_limiter.wait(url)

        # Random User-Agent + Referer
        headers = kwargs.get('headers', {})
        headers['User-Agent'] = random.choice(USER_AGENTS)
        # Add referer from same domain to look more realistic
        from urllib.parse import urlparse
        parsed = urlparse(url)
        headers['Referer'] = f"{parsed.scheme}://{parsed.netloc}/"
        kwargs['headers'] = headers
        kwargs['timeout'] = kwargs.get('timeout', REQUEST_TIMEOUT)
        kwargs['verify'] = kwargs.get('verify', False)

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, **kwargs)

                if response.status_code == 200:
                    self.rate_limiter.record_success(url)
                    return response
                elif response.status_code == 429:  # Too Many Requests
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                elif response.status_code in [403, 404]:
                    logger.warning(f"[{response.status_code}] {url}")
                    return None
                else:
                    logger.warning(f"[{response.status_code}] {url}")

            except requests.RequestException as e:
                self.rate_limiter.record_failure(url)
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    logger.debug(f"Retry {attempt + 1}/{MAX_RETRIES} for {url} in {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed after {MAX_RETRIES} attempts: {url} - {e}")

        return None


# ----------------------
# Structured Data Extractor
# ----------------------
class StructuredDataExtractor:
    """Extrahiert strukturierte Daten (JSON-LD, Microdata, Schema.org)"""

    @staticmethod
    def extract_json_ld(soup: BeautifulSoup) -> List[dict]:
        """Extrahiert JSON-LD Events"""
        events = []
        scripts = soup.find_all('script', type='application/ld+json')

        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if data.get('@type') == 'Event':
                        events.append(data)
                    elif data.get('@graph'):
                        events.extend([item for item in data['@graph'] if item.get('@type') == 'Event'])
                elif isinstance(data, list):
                    events.extend([item for item in data if isinstance(item, dict) and item.get('@type') == 'Event'])
            except (json.JSONDecodeError, AttributeError):
                continue

        return events

    @staticmethod
    def parse_event(event_data: dict, source_url: str) -> Optional[dict]:
        """Konvertiert JSON-LD Event zu unserem Format"""
        try:
            name = event_data.get('name', '')
            if not name:
                return None

            # Datum
            start_date = event_data.get('startDate', '')
            date_iso = normalize_date(start_date) if start_date else normalize_date(datetime.now())

            # Location
            location_data = event_data.get('location', {})
            if isinstance(location_data, dict):
                location = location_data.get('name', 'München')
            else:
                location = 'München'

            # Description
            desc = event_data.get('description', '')

            # Link
            link = event_data.get('url', source_url)

            return {
                "id": stable_id(name, date_iso, link),
                "name": name[:200],
                "date": date_iso,
                "category": map_category(name + " " + desc),
                "description": short(desc, 500),
                **CITY_DEFAULT,
                "location": location,
                "price": {"kids": None, "adults": None, "note": "Siehe Webseite"},
                "source": source_url,
                "link": link,
                "lastUpdated": now_iso(),
            }
        except Exception as e:
            logger.debug(f"JSON-LD parse error: {e}")
            return None


# ----------------------
# Source Quality Tracker
# ----------------------
class SourceQualityTracker:
    """Trackt Erfolgsrate von Quellen"""

    def __init__(self):
        self.stats = self.load_stats()

    def load_stats(self) -> dict:
        try:
            with open(STATS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}

    def save_stats(self):
        with open(STATS_FILE, 'w') as f:
            json.dump(self.stats, f, indent=2)

    def record(self, url: str, success: bool, events_found: int):
        if url not in self.stats:
            self.stats[url] = {"attempts": 0, "successes": 0, "total_events": 0, "last_success": None}

        self.stats[url]["attempts"] += 1
        if success:
            self.stats[url]["successes"] += 1
            self.stats[url]["total_events"] += events_found
            self.stats[url]["last_success"] = now_iso()

    def get_quality_score(self, url: str) -> float:
        """Returns 0.0 - 1.0"""
        stats = self.stats.get(url)
        if not stats or stats["attempts"] == 0:
            return 0.5  # Neutral
        return stats["successes"] / stats["attempts"]

    def should_skip(self, url: str) -> bool:
        """Skip wenn zu viele Fehler"""
        stats = self.stats.get(url)
        if not stats:
            return False
        if stats["attempts"] >= 10 and self.get_quality_score(url) < 0.2:
            return True
        return False


# ----------------------
# Geocoder (OpenStreetMap Nominatim)
# ----------------------
class Geocoder:
    """Geocoding mit OpenStreetMap Nominatim + Caching"""

    def __init__(self):
        self.cache = self.load_cache()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': NOMINATIM_USER_AGENT})
        self.last_request_time = 0

    def load_cache(self) -> dict:
        try:
            with open(GEOCODE_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}

    def save_cache(self):
        with open(GEOCODE_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, indent=2, ensure_ascii=False)

    def geocode(self, address: str, city: str = "München") -> Optional[Tuple[float, float]]:
        """Gibt (lat, lon) zurück oder None"""
        full_address = f"{address}, {city}, Germany"
        cache_key = full_address.lower().strip()

        # Cache lookup
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if cached:  # nicht None
                return tuple(cached)
            return None

        # Rate limiting (1 req/sec für Nominatim)
        elapsed = time.time() - self.last_request_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        try:
            params = {
                'q': full_address,
                'format': 'json',
                'limit': 1,
                'addressdetails': 1
            }

            response = self.session.get(
                NOMINATIM_BASE_URL,
                params=params,
                timeout=10
            )
            self.last_request_time = time.time()

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    lat = float(data[0]['lat'])
                    lon = float(data[0]['lon'])
                    self.cache[cache_key] = [lat, lon]
                    self.save_cache()
                    logger.debug(f"   📍 Geocoded: {address} → ({lat}, {lon})")
                    return (lat, lon)

            # Not found
            self.cache[cache_key] = None
            self.save_cache()
            return None

        except Exception as e:
            logger.debug(f"Geocoding error for {address}: {e}")
            return None


# ----------------------
# Opening Hours Parser
# ----------------------
class OpeningHoursParser:
    """Parst Öffnungszeiten aus Text"""

    WEEKDAYS_DE = ['montag', 'dienstag', 'mittwoch', 'donnerstag', 'freitag', 'samstag', 'sonntag']
    WEEKDAYS_SHORT = ['mo', 'di', 'mi', 'do', 'fr', 'sa', 'so']

    @staticmethod
    def parse(text: str) -> Dict[str, str]:
        """
        Versucht Öffnungszeiten zu parsen.
        Returns: {"monday": "10:00-18:00", "tuesday": "10:00-18:00", ...}
        """
        if not text:
            return {}

        text = text.lower()
        hours = {}

        # Pattern: "Mo-Fr 10:00-18:00"
        pattern = r'(mo|di|mi|do|fr|sa|so)(?:\s*-\s*(mo|di|mi|do|fr|sa|so))?\s*:?\s*(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})'
        matches = re.finditer(pattern, text)

        weekday_map = {
            'mo': 'monday', 'di': 'tuesday', 'mi': 'wednesday',
            'do': 'thursday', 'fr': 'friday', 'sa': 'saturday', 'so': 'sunday'
        }

        for match in matches:
            start_day = match.group(1)
            end_day = match.group(2) or start_day
            open_time = match.group(3)
            close_time = match.group(4)

            time_str = f"{open_time}-{close_time}"

            # Range (Mo-Fr)
            start_idx = OpeningHoursParser.WEEKDAYS_SHORT.index(start_day)
            end_idx = OpeningHoursParser.WEEKDAYS_SHORT.index(end_day)

            for i in range(start_idx, end_idx + 1):
                day_key = weekday_map[OpeningHoursParser.WEEKDAYS_SHORT[i]]
                hours[day_key] = time_str

        return hours


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
            date_str = str(value)
            # Deutsche Monatsnamen ersetzen
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

def calculate_event_quality_score(item):
    """
    Bewertet Event-Qualität und Relevanz für Kinder
    Returns: score (0-100), higher is better
    """
    score = 50  # Base score
    text = (item.get('name', '') + ' ' + item.get('description', '')).lower()

    # NEGATIVE indicators (irrelevant events)
    bad_patterns = [
        'pressemitteilung', 'pressetermin', 'akkreditierung', 'anmeldung erforderlich nur für presse',
        'nur für mitglieder', 'geschlossene veranstaltung', 'interne veranstaltung',
        'fortbildung für', 'lehrerfortbildung', 'pädagogische fachkräfte',
        'konferenz', 'tagung', 'symposium', 'fachvortrag',
        'ausstellungseröffnung nur für geladene gäste', 'vernissage',
        'board meeting', 'meeting', 'sitzung'
    ]

    for pattern in bad_patterns:
        if pattern in text:
            score -= 30

    # Check for very short/empty descriptions (low quality)
    desc = item.get('description', '')
    if len(desc) < 30:
        score -= 20
    elif len(desc) < 50:
        score -= 10

    # POSITIVE indicators (kid-friendly events)
    good_patterns = [
        'kinder', 'familien', 'kids', 'family', 'spielen', 'spaß',
        'workshop', 'basteln', 'mitmachen', 'entdecken', 'abenteuer',
        'märchen', 'theater', 'kindertheater', 'puppentheater',
        'museum', 'ausstellung', 'führung für kinder',
        'sport', 'bewegung', 'turnen', 'tanzen',
        'kreativ', 'malen', 'zeichnen', 'gestalten',
        'natur', 'tiere', 'zoo', 'tierpark',
        'musik', 'konzert für kinder', 'mitmachkonzert',
        'vorlesen', 'lesestunde', 'bilderbuch'
    ]

    for pattern in good_patterns:
        if pattern in text:
            score += 5

    # Check for age information (good sign)
    age_patterns = ['ab ', 'jahre', 'alter', 'für kinder', 'kindergarten', 'grundschule']
    if any(pattern in text for pattern in age_patterns):
        score += 10

    # Check for practical info (time, price)
    if item.get('time') or 'uhr' in text:
        score += 5
    if item.get('price') or 'euro' in text or 'kostenlos' in text or 'frei' in text:
        score += 5

    return max(0, min(100, score))  # Clamp between 0-100


def generate_kids_description(item):
    """
    Generiert eine kinderfreundliche Beschreibung aus dem Event
    Dies ist eine regelbasierte "AI"-Approximation für das MVP
    """
    name = item.get('name', '')
    desc = item.get('description', '')
    text = (name + ' ' + desc).lower()
    category = item.get('category', '')

    # Extract key exciting words/phrases
    exciting_words = []

    # Activity-based excitement
    activities = {
        'basteln': 'Bastle und gestalte',
        'malen': 'Male bunte Bilder',
        'bauen': 'Baue tolle Sachen',
        'spielen': 'Spiele mit',
        'entdecken': 'Entdecke',
        'erforschen': 'Erforsche',
        'lernen': 'Lerne',
        'ausprobieren': 'Probiere aus',
        'mitmachen': 'Mach mit',
        'theater': 'Erlebe ein tolles Theater-Stück',
        'musik': 'Höre tolle Musik',
        'tanzen': 'Tanze mit',
        'singen': 'Singe mit',
        'sport': 'Bewege dich',
        'klettern': 'Klettere',
        'springen': 'Spring und hüpf',
        'rennen': 'Lauf und renn',
        'schwimmen': 'Schwimme',
        'tiere': 'Besuche die Tiere',
        'zoo': 'Schau dir Tiere an',
        'museum': 'Entdecke im Museum',
        'ausstellung': 'Schau dir tolle Sachen an',
        'führung': 'Geh auf Entdeckungstour',
        'workshop': 'Lerne was Neues',
        'kurs': 'Probiere was Neues',
        'lesen': 'Höre spannende Geschichten',
        'märchen': 'Erlebe ein Märchen',
        'abenteuer': 'Erlebe ein Abenteuer',
        'schatzsuche': 'Geh auf Schatzsuche',
        'rätsel': 'Löse spannende Rätsel'
    }

    # Find matching activities
    intro = None
    for keyword, action in activities.items():
        if keyword in text:
            intro = action
            break

    # Extract topic/subject if possible
    topics = {
        'dinosaurier': 'Dinosaurier',
        'weltraum': 'Weltraum und Sterne',
        'planeten': 'Planeten',
        'roboter': 'Roboter',
        'piraten': 'Piraten',
        'prinzessin': 'Prinzessinnen',
        'ritter': 'Ritter und Burgen',
        'natur': 'die Natur',
        'pflanzen': 'Pflanzen',
        'experimente': 'spannende Experimente',
        'tiere': 'Tiere',
        'wald': 'den Wald',
        'wasser': 'das Wasser',
        'kunst': 'Kunst',
        'farben': 'bunte Farben',
        'essen': 'leckeres Essen',
        'kochen': 'Kochen und Backen',
        'pizza': 'Pizza',
        'kuchen': 'Kuchen',
        'winter': 'den Winter',
        'sommer': 'den Sommer',
        'weihnachten': 'Weihnachten',
        'ostern': 'Ostern',
        'halloween': 'Halloween',
        'fasching': 'Fasching',
        'geburtstag': 'Geburtstage'
    }

    topic = None
    for keyword, topic_name in topics.items():
        if keyword in text:
            topic = topic_name
            break

    # Generate description based on category and found elements
    if intro and topic:
        kids_desc = f"{intro} {topic}!"
    elif intro:
        kids_desc = f"{intro}!"
    else:
        # Fallback based on category
        category_templates = {
            'theater': 'Schau dir ein spannendes Theater-Stück an!',
            'museum': 'Entdecke tolle Sachen im Museum!',
            'sport': 'Beweg dich und hab Spaß!',
            'kreativ': 'Sei kreativ und gestalte was Tolles!',
            'outdoor': 'Erlebe ein Abenteuer draußen!',
            'musik': 'Höre tolle Musik und mach mit!',
            'workshop': 'Lerne was Neues und probiere es selbst aus!',
            'natur': 'Entdecke die Natur!'
        }
        kids_desc = category_templates.get(category, 'Komm vorbei und hab Spaß!')

    # Try to extract one interesting detail from description
    interesting_details = []

    # Look for specific named things (proper nouns, animals, characters)
    detail_patterns = {
        'pumuckl': 'Triff Pumuckl!',
        'grüffelo': 'Triff den Grüffelo!',
        'räuber': 'Triff echte Räuber!',
        'elefant': 'Schau dir Elefanten an!',
        'pinguin': 'Besuch die Pinguine!',
        'löwe': 'Sieh die Löwen!',
        'affen': 'Beobachte lustige Affen!',
        'schmetterlinge': 'Entdecke bunte Schmetterlinge!',
        'fische': 'Schau dir Fische an!',
        'sterne': 'Betrachte die Sterne!',
        'lagerfeuer': 'Sitz am Lagerfeuer!',
        'schatzsuche': 'Such einen Schatz!',
        'fütterung': 'Schau bei der Fütterung zu!',
        'führung': 'Geh auf Entdeckungstour!',
        'kostüm': 'Verkleid dich!',
        'basteln': 'Bastle was Schönes!',
        'malen': 'Male bunte Bilder!',
        'spielen': 'Spiel mit anderen Kindern!'
    }

    for pattern, detail in detail_patterns.items():
        if pattern in text and detail not in kids_desc:
            interesting_details.append(detail)
            break

    # Combine
    if interesting_details:
        kids_desc = f"{kids_desc} {interesting_details[0]}"

    # Make sure it's not too long
    if len(kids_desc) > 200:
        kids_desc = kids_desc[:197] + "..."

    return kids_desc


def enrich_for_kids(item):
    """Macht Events kinderfreundlich und fügt KidzOut-spezifische Felder hinzu"""
    text = (item.get('name', '') + ' ' + item.get('description', '')).lower()

    # QUALITY CHECK FIRST - skip low-quality events
    quality_score = calculate_event_quality_score(item)
    item['_qualityScore'] = quality_score  # Internal field for debugging

    if quality_score < 30:
        # Event is likely irrelevant (press release, admin, etc.)
        item['_skipReason'] = 'Low quality score'
        return None  # Signal to skip this event

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

    # *** NEW: Generate kid-friendly description ***
    item['descriptionKids'] = generate_kids_description(item)

    # Eltern-Tipps (make them more contextual)
    tips = []

    # Always useful
    tips.append("Rechtzeitig da sein - beliebte Events sind schnell voll")

    # Context-specific tips
    if 'museum' in text or 'ausstellung' in text:
        tips.append("Online-Tickets vorab buchen spart Wartezeit")

    if 'outdoor' in text or 'draußen' in text or 'park' in text:
        tips.append("Wetterfeste Kleidung und Sonnenschutz nicht vergessen")
        tips.append("Wasser und Snacks mitbringen")
    elif 'indoor' in text or 'drinnen' in text:
        tips.append("Wechselkleidung kann hilfreich sein")

    if 'basteln' in text or 'workshop' in text or 'malen' in text:
        tips.append("Kleidung anziehen die dreckig werden darf")

    if 'sport' in text or 'bewegung' in text or 'klettern' in text:
        tips.append("Bequeme Sportkleidung und feste Schuhe mitbringen")

    # Add generic tip if we have less than 3
    if len(tips) < 3:
        tips.append("Mit Öffis anreisen wenn möglich - entspannter für alle")

    item['parentTips'] = tips[:4]  # Max 4 tips

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


def enrich_location(item, geocoder: Geocoder = None):
    """Macht Locations kinderfreundlich und fügt KidzOut-spezifische Felder hinzu"""
    text = (item.get('name', '') + ' ' + item.get('description', '')).lower()

    # Altersgruppen
    age_groups = []
    if any(word in text for word in ['baby', 'kleinkind', '0-3', 'u3', 'krabbelgruppe']):
        age_groups.append("0-3")
    if any(word in text for word in ['kindergarten', '3-6', 'vorschule', 'kita']):
        age_groups.append("3-6")
    if any(word in text for word in ['grundschule', '6-9', 'schulkind']):
        age_groups.append("6-9")
    if any(word in text for word in ['ab 9', 'ab 10', '9-12', 'teenager']):
        age_groups.append("9-12")

    if not age_groups:
        category = item.get('category', '')
        if category in ['spielplatz', 'outdoor']:
            age_groups = ["3-6", "6-9"]
        elif category in ['museum', 'indoor']:
            age_groups = ["6-9", "9-12"]
        else:
            age_groups = ["3-6", "6-9", "9-12"]

    # Name Kids
    name = item.get('name', '')
    category = item.get('category', '')

    if 'spielplatz' in text or category == 'spielplatz':
        item['nameKids'] = f"🏞️ {name[:50]}"
    elif 'museum' in text or category == 'museum':
        item['nameKids'] = f"🏛️ {name[:50]}"
    elif 'indoor' in text or 'halle' in text:
        item['nameKids'] = f"🏠 {name[:50]}"
    elif 'schwimm' in text or 'bad' in text:
        item['nameKids'] = f"🏊 {name[:50]}"
    elif 'tier' in text or 'zoo' in text:
        item['nameKids'] = f"🦁 {name[:50]}"
    else:
        item['nameKids'] = f"🎯 {name[:50]}"

    item['ageGroups'] = age_groups

    # Descriptions (altersgerecht)
    base_desc = item.get('description', '')
    item['content'] = {
        "3-6": f"Ein toller Ort für kleine Entdecker! {base_desc[:150]}",
        "6-9": f"Spannend für Schulkinder! {base_desc[:150]}",
        "9-12": f"Perfekt für ältere Kinder! {base_desc[:150]}"
    }

    # Wetterabhängigkeit
    if any(word in text for word in ['outdoor', 'draußen', 'park', 'spielplatz', 'garten']):
        item['weatherSuitable'] = 'good-weather'
    elif any(word in text for word in ['indoor', 'drinnen', 'halle', 'museum']):
        item['weatherSuitable'] = 'indoor'
    else:
        item['weatherSuitable'] = 'any'

    # Energie-Level
    if any(word in text for word in ['sport', 'klettern', 'toben', 'action', 'spielplatz']):
        item['energyLevel'] = 'high'
    elif any(word in text for word in ['basteln', 'malen', 'lesen', 'museum']):
        item['energyLevel'] = 'low'
    else:
        item['energyLevel'] = 'medium'

    # Duration (geschätzt)
    if 'museum' in text:
        item['duration'] = '2-3 Stunden'
    elif 'spielplatz' in text:
        item['duration'] = '1-2 Stunden'
    else:
        item['duration'] = '2-4 Stunden'

    # Parent Tips
    tips = ["Wasser und Snacks nicht vergessen"]
    if item['weatherSuitable'] == 'good-weather':
        tips.append("Sonnenschutz und wetterfeste Kleidung einpacken")
    if item['weatherSuitable'] == 'indoor':
        tips.append("Wechselkleidung kann hilfreich sein")
    if 'spielplatz' in text:
        tips.append("Erste-Hilfe-Set griffbereit haben")

    item['parentTips'] = tips

    # Highlights (optional)
    highlights = []
    if 'kostenlos' in text or 'frei' in text:
        highlights.append("Kostenloser Eintritt")
    if 'parkplatz' in text or 'parken' in text:
        highlights.append("Parkplätze vorhanden")
    if 'öpnv' in text or 'u-bahn' in text or 'bus' in text:
        highlights.append("Gut mit Öffis erreichbar")

    if highlights:
        item['highlights'] = highlights

    # Amenities
    amenities = []
    if 'wickel' in text:
        amenities.append("Wickelraum")
    if 'wc' in text or 'toilette' in text:
        amenities.append("WC")
    if 'parkplatz' in text:
        amenities.append("Parkplatz")
    if 'rollstuhl' in text or 'barrierefrei' in text:
        amenities.append("Rollstuhlgerecht")

    if amenities:
        item['amenities'] = amenities

    # Geocoding
    if geocoder and 'address' in item:
        coords = geocoder.geocode(item['address'], item.get('city', 'München'))
        if coords:
            item['lat'], item['lon'] = coords

    return item


# ----------------------
# Location Harvesting
# ----------------------
def harvest_locations(url: str, selector: str,
                     name_selector: str | None = None,
                     address_selector: str | None = None,
                     desc_selector: str | None = None,
                     session: SessionManager = None,
                     quality_tracker: SourceQualityTracker = None,
                     geocoder: Geocoder = None) -> list[dict]:
    """Crawlt dauerhafte Locations (Spielplätze, Museen, etc.)"""

    if quality_tracker and quality_tracker.should_skip(url):
        logger.info(f"⏭️  Skipping low-quality source: {url}")
        return []

    logger.info(f"🗺️  Crawling Locations: {url}")

    try:
        resp = session.get(url) if session else requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        if not resp:
            if quality_tracker:
                quality_tracker.record(url, False, 0)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try structured data first (JSON-LD Place/LocalBusiness)
        locations = []
        scripts = soup.find_all('script', type='application/ld+json')

        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if data.get('@type') in ['Place', 'LocalBusiness', 'TouristAttraction']:
                        loc = parse_json_ld_location(data, url)
                        if loc:
                            locations.append(loc)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') in ['Place', 'LocalBusiness']:
                            loc = parse_json_ld_location(item, url)
                            if loc:
                                locations.append(loc)
            except:
                continue

        if locations:
            logger.info(f"   ✨ Found {len(locations)} locations via JSON-LD")
            for loc in locations:
                enrich_location(loc, geocoder)
            if quality_tracker:
                quality_tracker.record(url, True, len(locations))
            return locations

        # Fallback: Traditional HTML parsing
        selectors_to_try = [
            selector,
            "div[class*='location']",
            "div[class*='place']",
            "article[class*='location']",
            "div[class*='item']",
            ".location-card",
            ".place-item"
        ]

        found_elements = []
        for sel in selectors_to_try:
            try:
                elements = soup.select(sel)
                if elements:
                    logger.debug(f"   ✓ Found with '{sel}': {len(elements)} elements")
                    found_elements.extend(elements[:20])
                    if len(found_elements) >= 30:
                        break
            except:
                continue

        # Deduplicate
        seen = set()
        unique_elements = []
        for elem in found_elements:
            elem_text = elem.get_text(strip=True)[:100]
            if elem_text not in seen and len(elem_text) > 15:
                seen.add(elem_text)
                unique_elements.append(elem)

        logger.debug(f"   📊 {len(unique_elements)} unique elements")

        # Extract locations
        for elem in unique_elements[:30]:
            try:
                # Name
                name = ""
                if name_selector:
                    name_elem = elem.select_one(name_selector)
                    if name_elem:
                        name = name_elem.get_text(strip=True)

                if not name:
                    for tag in ['h1', 'h2', 'h3', 'h4']:
                        heading = elem.find(tag)
                        if heading:
                            name = heading.get_text(strip=True)
                            break

                if not name:
                    link = elem.find('a')
                    if link:
                        name = link.get_text(strip=True)

                if not name or len(name) < 3:
                    continue

                # Address
                address = ""
                if address_selector:
                    addr_elem = elem.select_one(address_selector)
                    if addr_elem:
                        address = addr_elem.get_text(strip=True)

                # Description
                desc = ""
                if desc_selector:
                    desc_elem = elem.select_one(desc_selector)
                    if desc_elem:
                        desc = desc_elem.get_text(strip=True)

                if not desc:
                    desc = elem.get_text(' ', strip=True)[:500]

                # Link
                link_elem = elem.find('a', href=True)
                if link_elem:
                    link = urljoin(url, link_elem['href'])
                else:
                    link = url

                # Category (geschätzt)
                text_lower = (name + " " + desc).lower()
                category = 'location'
                if 'spielplatz' in text_lower:
                    category = 'spielplatz'
                elif 'museum' in text_lower:
                    category = 'museum'
                elif 'indoor' in text_lower or 'halle' in text_lower:
                    category = 'indoor'
                elif 'schwimm' in text_lower or 'bad' in text_lower:
                    category = 'schwimmbad'
                elif 'tier' in text_lower or 'zoo' in text_lower:
                    category = 'tierpark'

                location = {
                    "id": "loc-" + sha1_16(f"{name}|{address}|{link}"),
                    "name": name[:200],
                    "address": address,
                    "category": category,
                    "description": desc[:500],
                    **CITY_DEFAULT,
                    "source": url,
                    "link": link,
                    "lastUpdated": now_iso(),
                }

                location = enrich_location(location, geocoder)
                locations.append(location)

            except Exception as e:
                logger.debug(f"Parse error: {e}")
                continue

        logger.info(f"   ✅ {len(locations)} locations extracted")

        if quality_tracker:
            quality_tracker.record(url, len(locations) > 0, len(locations))

        return locations

    except Exception as e:
        logger.error(f"   ❌ Error: {e}")
        if quality_tracker:
            quality_tracker.record(url, False, 0)
        return []


def parse_json_ld_location(data: dict, source_url: str) -> Optional[dict]:
    """Parst JSON-LD Place/LocalBusiness"""
    try:
        name = data.get('name', '')
        if not name:
            return None

        address_data = data.get('address', {})
        if isinstance(address_data, dict):
            street = address_data.get('streetAddress', '')
            postal = address_data.get('postalCode', '')
            city = address_data.get('addressLocality', 'München')
            address = f"{street}, {postal} {city}" if street else city
        else:
            address = str(address_data) if address_data else ""

        desc = data.get('description', '')
        link = data.get('url', source_url)

        # Geo coordinates (if available)
        lat, lon = None, None
        geo = data.get('geo', {})
        if isinstance(geo, dict):
            lat = geo.get('latitude')
            lon = geo.get('longitude')

        location = {
            "id": "loc-" + sha1_16(f"{name}|{address}|{link}"),
            "name": name[:200],
            "address": address,
            "category": "location",
            "description": desc[:500],
            **CITY_DEFAULT,
            "source": source_url,
            "link": link,
            "lastUpdated": now_iso(),
        }

        if lat and lon:
            location['lat'] = float(lat)
            location['lon'] = float(lon)

        # Opening hours
        opening_hours = data.get('openingHours')
        if opening_hours:
            parser = OpeningHoursParser()
            hours = parser.parse(str(opening_hours))
            if hours:
                location['openingHours'] = hours

        return location

    except Exception as e:
        logger.debug(f"JSON-LD location parse error: {e}")
        return None


# ----------------------
# HTML Harvesting mit Super-Powers
# ----------------------
def harvest_html(url: str, selector: str, date_selector: str | None = None,
                 title_selector: str | None = None, desc_selector: str | None = None,
                 session: SessionManager = None, quality_tracker: SourceQualityTracker = None) -> list[dict]:
    """Ultra-intelligenter HTML-Crawler"""

    if quality_tracker and quality_tracker.should_skip(url):
        logger.info(f"⏭️  Skipping low-quality source: {url}")
        return []

    logger.info(f"🔍 Crawling: {url}")

    try:
        resp = session.get(url) if session else requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        if not resp:
            if quality_tracker:
                quality_tracker.record(url, False, 0)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. Try structured data first (JSON-LD)
        extractor = StructuredDataExtractor()
        json_ld_events = extractor.extract_json_ld(soup)
        events = []

        for event_data in json_ld_events:
            event = extractor.parse_event(event_data, url)
            if event:
                event = enrich_for_kids(event)
                if event:  # Check if event passed quality filter
                    events.append(event)

        if events:
            logger.info(f"   ✨ Found {len(events)} events via JSON-LD")
            if quality_tracker:
                quality_tracker.record(url, True, len(events))
            return events

        # 2. Fallback: Traditional HTML parsing
        selectors_to_try = [
            selector,
            "div[class*='event']",
            "div[class*='veranstaltung']",
            "article[class*='event']",
            "article[class*='teaser']",
            "div[class*='teaser']",
            "div[class*='item']",
            "div[class*='card']",
            "li[class*='event']",
            ".m-teaser",
            ".event-card",
            ".list-item",
            "a[href*='/event']"
        ]

        found_elements = []
        for sel in selectors_to_try:
            try:
                elements = soup.select(sel)
                if elements:
                    logger.debug(f"   ✓ Found with '{sel}': {len(elements)} elements")
                    found_elements.extend(elements[:20])
                    if len(found_elements) >= 30:
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

        logger.debug(f"   📊 {len(unique_elements)} unique elements")

        # Events extrahieren
        for elem in unique_elements[:30]:
            try:
                # Titel
                title = ""
                if title_selector:
                    title_elem = elem.select_one(title_selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)

                if not title:
                    for tag in ['h1', 'h2', 'h3', 'h4']:
                        heading = elem.find(tag)
                        if heading:
                            title = heading.get_text(strip=True)
                            break

                if not title:
                    link = elem.find('a')
                    if link:
                        title = link.get_text(strip=True)

                if not title or len(title) < 5:
                    continue

                # Beschreibung
                desc = ""
                if desc_selector:
                    desc_elem = elem.select_one(desc_selector)
                    if desc_elem:
                        desc = desc_elem.get_text(strip=True)

                if not desc:
                    desc = elem.get_text(' ', strip=True)[:500]

                # Datum
                date_iso = None
                if date_selector:
                    date_elem = elem.select_one(date_selector)
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        date_iso = normalize_date(date_text)

                if not date_iso:
                    date_patterns = [
                        r'\d{1,2}\.\d{1,2}\.\d{2,4}',
                        r'\d{1,2}\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)',
                        r'\d{4}-\d{2}-\d{2}'
                    ]

                    all_text = elem.get_text()
                    for pattern in date_patterns:
                        match = re.search(pattern, all_text)
                        if match:
                            date_iso = normalize_date(match.group())
                            break

                if not date_iso:
                    date_iso = normalize_date(datetime.now())

                # Link
                link_elem = elem.find('a', href=True)
                if link_elem:
                    link = urljoin(url, link_elem['href'])
                else:
                    link = url

                event = {
                    "id": stable_id(title, date_iso, link),
                    "name": title[:200],
                    "date": date_iso,
                    "category": map_category(title + " " + desc),
                    "description": short(desc, 500),
                    **CITY_DEFAULT,
                    "price": {"kids": None, "adults": None, "note": "Siehe Webseite"},
                    "source": url,
                    "link": link,
                    "lastUpdated": now_iso(),
                }

                event = enrich_for_kids(event)
                if event:  # Check if event passed quality filter
                    events.append(event)

            except Exception as e:
                logger.debug(f"Parse error: {e}")
                continue

        logger.info(f"   ✅ {len(events)} events extracted")

        if quality_tracker:
            quality_tracker.record(url, len(events) > 0, len(events))

        return events

    except Exception as e:
        logger.error(f"   ❌ Error: {e}")
        if quality_tracker:
            quality_tracker.record(url, False, 0)
        return []


# ----------------------
# RSS Handler
# ----------------------
def harvest_rss(url: str, session: SessionManager = None) -> list[dict]:
    """Parse RSS/Atom feeds with BeautifulSoup (no feedparser needed)"""
    logger.info(f"📡 RSS Feed: {url}")
    try:
        # Fetch RSS content
        if session:
            resp = session.get(url)
        else:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False,
                              headers={"User-Agent": random.choice(USER_AGENTS)})

        if not resp or resp.status_code != 200:
            logger.warning(f"   ⚠️ RSS fetch failed: {url}")
            return []

        # Parse XML with BeautifulSoup
        soup = BeautifulSoup(resp.content, 'xml')
        items = []

        # Try RSS 2.0 format first
        entries = soup.find_all('item')
        if not entries:
            # Try Atom format
            entries = soup.find_all('entry')

        for entry in entries[:50]:  # Limit to 50 entries
            # Extract title
            title_tag = entry.find('title')
            title = title_tag.text.strip() if title_tag else "Ohne Titel"

            # Extract link
            link_tag = entry.find('link')
            if link_tag:
                # Atom format: <link href="..."/>
                link = link_tag.get('href') or link_tag.text.strip()
            else:
                link = ""

            # Extract date (pubDate for RSS, published/updated for Atom)
            date_tag = entry.find('pubDate') or entry.find('published') or entry.find('updated')
            if date_tag:
                date_iso = normalize_date(date_tag.text.strip())
            else:
                date_iso = normalize_date(datetime.now())

            # Extract description
            desc_tag = entry.find('description') or entry.find('summary') or entry.find('content')
            desc = desc_tag.text.strip() if desc_tag else ""

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
            if item:  # Check if event passed quality filter
                items.append(item)

        logger.info(f"   ✅ {len(items)} events from RSS")
        return items
    except Exception as e:
        logger.error(f"   ❌ RSS Error: {e}")
        return []


# ----------------------
# iCal Handler
# ----------------------
def harvest_ical(url: str, session: SessionManager = None) -> list[dict]:
    logger.info(f"📅 iCal: {url}")
    try:
        import icalendar
    except:
        logger.warning("   ⚠️ icalendar not installed")
        return []

    try:
        resp = session.get(url) if session else requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        if not resp:
            return []

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
                "location": location,
                "category": map_category(title + " " + desc),
                "description": short(desc, 600),
                **CITY_DEFAULT,
                "source": url,
                "link": link or url,
                "lastUpdated": now_iso(),
            }

            item = enrich_for_kids(item)
            if item:  # Check if event passed quality filter
                out.append(item)

        logger.info(f"   ✅ {len(out)} events from iCal")
        return out

    except Exception as e:
        logger.error(f"   ❌ iCal Error: {e}")
        return []


# ----------------------
# Config laden
# ----------------------
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"rss": [], "html": [], "ical": []}


# ----------------------
# Parallel Crawler Worker
# ----------------------
def crawl_source(source_config: tuple, session: SessionManager, quality_tracker: SourceQualityTracker) -> Tuple[str, list]:
    """Worker function für paralleles Crawling"""
    source_type, source_data = source_config

    try:
        if source_type == "html":
            if isinstance(source_data, dict):
                url = source_data["url"]
                events = harvest_html(
                    url,
                    source_data.get("selector", "article"),
                    source_data.get("date_selector"),
                    source_data.get("title_selector"),
                    source_data.get("desc_selector"),
                    session,
                    quality_tracker
                )
            else:
                events = harvest_html(source_data, "article", None, None, None, session, quality_tracker)
            return (source_data if isinstance(source_data, str) else source_data["url"], events)

        elif source_type == "rss":
            events = harvest_rss(source_data, session)
            return (source_data, events)

        elif source_type == "ical":
            events = harvest_ical(source_data, session)
            return (source_data, events)

    except Exception as e:
        logger.error(f"Worker error: {e}")
        return ("", [])

    return ("", [])


# ----------------------
# Pipeline - PARALLELISIERT!
# ----------------------
def get_events_from_all_sources() -> list[dict]:
    cfg = load_config()

    logger.info("\n" + "="*70)
    logger.info("🚀 KidzOut SUPER-CRAWLER v4.1 - Event Harvesting")
    logger.info(f"📋 Sources: {len(cfg.get('rss', []))} RSS | {len(cfg.get('html', []))} HTML | {len(cfg.get('ical', []))} iCal")
    logger.info(f"⚡ Parallel Workers: {MAX_WORKERS}")
    logger.info("="*70)

    session = SessionManager()
    quality_tracker = SourceQualityTracker()

    # Prepare all sources
    sources = []
    for url in cfg.get("rss", []):
        sources.append(("rss", url))
    for item in cfg.get("html", []):
        sources.append(("html", item))
    for url in cfg.get("ical", []):
        sources.append(("ical", url))

    logger.info(f"🎯 Total sources to crawl: {len(sources)}")

    # Parallel crawling!
    all_events = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(crawl_source, src, session, quality_tracker): src for src in sources}

        for future in as_completed(futures):
            completed += 1
            try:
                source_url, events = future.result()
                all_events.extend(events)
                logger.info(f"   [{completed}/{len(sources)}] Progress: {len(all_events)} events total")
            except Exception as e:
                logger.error(f"Future failed: {e}")

    # Deduplizierung
    dedup = {}
    for ev in all_events:
        dedup_key = f"{ev['name'][:30]}_{ev['date']}"
        if dedup_key not in dedup or len(ev.get('description', '')) > len(dedup[dedup_key].get('description', '')):
            dedup[dedup_key] = ev

    unique_events = list(dedup.values())

    # Save quality stats
    quality_tracker.save_stats()

    logger.info("\n" + "="*70)
    logger.info(f"📊 RESULT: {len(unique_events)} unique events | {len(all_events)} total scraped")
    logger.info("="*70 + "\n")

    return unique_events


def get_locations_from_all_sources(geocoder: Geocoder = None) -> list[dict]:
    """Crawlt alle Locations (Spielplätze, Museen, etc.)"""
    cfg = load_config()

    logger.info("\n" + "="*70)
    logger.info("🗺️  KidzOut SUPER-CRAWLER v4.1 - Location Harvesting")
    logger.info(f"📋 Location Sources: {len(cfg.get('locations', []))}")
    logger.info(f"⚡ Parallel Workers: {MAX_WORKERS}")
    logger.info("="*70)

    session = SessionManager()
    quality_tracker = SourceQualityTracker()

    # Prepare location sources
    sources = cfg.get("locations", [])
    logger.info(f"🎯 Total location sources: {len(sources)}")

    all_locations = []

    for item in sources:
        try:
            if isinstance(item, dict):
                url = item["url"]
                locations = harvest_locations(
                    url,
                    item.get("selector", "div"),
                    item.get("name_selector"),
                    item.get("address_selector"),
                    item.get("desc_selector"),
                    session,
                    quality_tracker,
                    geocoder
                )
                all_locations.extend(locations)
            time.sleep(2.0)  # Rate limiting
        except Exception as e:
            logger.error(f"Location harvest error: {e}")

    # Deduplizierung
    dedup = {}
    for loc in all_locations:
        dedup_key = f"{loc['name'][:30]}_{loc.get('address', '')[:20]}"
        if dedup_key not in dedup or len(loc.get('description', '')) > len(dedup[dedup_key].get('description', '')):
            dedup[dedup_key] = loc

    unique_locations = list(dedup.values())

    quality_tracker.save_stats()

    logger.info("\n" + "="*70)
    logger.info(f"📊 RESULT: {len(unique_locations)} unique locations | {len(all_locations)} total scraped")
    logger.info("="*70 + "\n")

    return unique_locations


def main():
    logger.info("\n🎯 KidzOut SUPER-CRAWLER v4.1 starting...")

    start_time = time.time()

    # Load manual events
    try:
        with open('manual_events.json', 'r', encoding='utf-8') as f:
            manual = json.load(f)
            manual_events = manual.get('events', [])
            logger.info(f"📋 {len(manual_events)} manual events loaded")
    except:
        manual_events = []

    # Crawl new events
    crawled_events = get_events_from_all_sources()

    # Combine events
    all_events = manual_events + crawled_events

    # Crawl locations (mit Geocoder!)
    logger.info("\n" + "="*70)
    logger.info("🗺️  Starting Location Harvesting...")
    logger.info("="*70)
    geocoder = Geocoder()
    crawled_locations = get_locations_from_all_sources(geocoder)

    # Load existing data
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {"locations": [], "events": []}

    # Update events
    if all_events:
        data["events"] = sorted(all_events, key=lambda e: (e["date"], e["name"]))
        data["totalEvents"] = len(all_events)
        logger.info(f"\n✅ {len(all_events)} events saved")
    else:
        logger.warning("\n⚠️ No events found")

    # Update locations
    if crawled_locations:
        data["locations"] = sorted(crawled_locations, key=lambda l: l["name"])
        data["totalLocations"] = len(crawled_locations)
        logger.info(f"✅ {len(crawled_locations)} locations saved")
    else:
        logger.warning("⚠️ No locations found")

    # Metadata
    elapsed = time.time() - start_time
    data["lastCrawled"] = now_iso()
    data["metadata"] = {
        "version": "4.1",
        "lastCrawled": now_iso(),
        "totalLocations": len(data.get("locations", [])),
        "totalEvents": len(all_events),
        "crawlDurationSeconds": round(elapsed, 2),
        "sources": load_config()
    }

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Show examples
    if all_events and len(all_events) > 0:
        logger.info("\n📅 Sample events (first 5):")
        for ev in all_events[:5]:
            logger.info(f"  - {ev['nameKids'][:60]} ({ev['date']}) [{ev['category']}]")

    logger.info(f"\n✨ Finished in {elapsed:.1f}s!")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    main()
