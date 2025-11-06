#!/usr/bin/env python3
"""
Creates sample events and locations for MVP testing
Until we solve the Cloudflare/bot-protection issues, this provides realistic test data
"""
import json
from datetime import datetime, timedelta

def create_sample_events():
    """Generate realistic sample events for kids in Munich"""
    base_date = datetime.now()

    events = [
        {
            "id": "sample-tierpark-1",
            "name": "Winterfest im Tierpark Hellabrunn",
            "nameKids": "Winter-Party bei den Tieren",
            "date": (base_date + timedelta(days=2)).strftime("%Y-%m-%d"),
            "time": "10:00-16:00",
            "category": "Natur & Tiere",
            "description": "Ein besonderer Tag im Tierpark mit Winteraktivitäten, Tierfütterungen und warmen Getränken für die ganze Familie.",
            "descriptionKids": "Schau zu wie die Pinguine gefüttert werden und bastle deine eigene Winter-Laterne!",
            "ageGroups": ["0-3", "3-6", "6-12"],
            "price": "15 Euro Erwachsene, 8 Euro Kinder",
            "address": "Tierparkstraße 30, 81543 München",
            "gps": {"lat": 48.098611, "lon": 11.558333},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.tierpark-hellabrunn.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["sunny", "cloudy", "rainy"],
            "energyLevel": "active",
            "duration": "3-4 Stunden",
            "parentTips": "Warme Kleidung mitbringen! Snacks gibt's vor Ort.",
            "highlights": ["Tierfütterungen", "Bastelstation", "Kindergerecht"],
            "amenities": ["parking", "food", "restrooms", "stroller-friendly"]
        },
        {
            "id": "sample-museum-1",
            "name": "Pumuckl-Ausstellung im Kindermuseum",
            "nameKids": "Triff Pumuckl!",
            "date": (base_date + timedelta(days=5)).strftime("%Y-%m-%d"),
            "time": "14:00-17:00",
            "category": "Museen & Ausstellungen",
            "description": "Interaktive Familienführung durch die beliebte Pumuckl-Ausstellung mit Rätseln und Mitmach-Stationen.",
            "descriptionKids": "Geh auf Entdeckungsreise in Meister Eders Werkstatt und löse knifflige Pumuckl-Rätsel!",
            "ageGroups": ["3-6", "6-12"],
            "price": "8 Euro pro Person",
            "address": "Arnulfstraße 3, 80335 München",
            "gps": {"lat": 48.145833, "lon": 11.558056},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.kindermuseum-muenchen.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["rainy", "cloudy", "sunny"],
            "energyLevel": "moderate",
            "duration": "2-3 Stunden",
            "parentTips": "Reservierung online empfohlen! Café im Museum vorhanden.",
            "highlights": ["Interaktiv", "Pädagogisch wertvoll", "Drinnen"],
            "amenities": ["food", "restrooms", "stroller-friendly"]
        },
        {
            "id": "sample-outdoor-1",
            "name": "Waldabenteuer im Englischen Garten",
            "nameKids": "Schatzsuche im Wald",
            "date": (base_date + timedelta(days=7)).strftime("%Y-%m-%d"),
            "time": "10:00-13:00",
            "category": "Outdoor & Sport",
            "description": "Geführte Naturerkundung für Familien mit Schatzsuche, Naturquiz und Lagerfeuer am Eisbach.",
            "descriptionKids": "Werde zum Naturforscher! Suche versteckte Schätze und lerne coole Tiere und Pflanzen kennen.",
            "ageGroups": ["3-6", "6-12"],
            "price": "Kostenlos (Spende willkommen)",
            "address": "Englischer Garten, Treffpunkt Chinesischer Turm",
            "gps": {"lat": 48.164167, "lon": 11.600556},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.muenchen.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["sunny", "cloudy"],
            "energyLevel": "very-active",
            "duration": "3 Stunden",
            "parentTips": "Feste Schuhe und wetterfeste Kleidung! Verpflegung selbst mitbringen.",
            "highlights": ["Outdoor", "Kostenlos", "Abenteuer"],
            "amenities": ["parking", "restrooms"]
        },
        {
            "id": "sample-theater-1",
            "name": "Der Froschkönig - Kindertheater",
            "nameKids": "Märchen auf der Bühne",
            "date": (base_date + timedelta(days=10)).strftime("%Y-%m-%d"),
            "time": "15:00-16:30",
            "category": "Theater & Shows",
            "description": "Klassisches Märchen kindgerecht inszeniert von der Schauburg München. Für Kinder ab 4 Jahren.",
            "descriptionKids": "Erlebe das Märchen vom Froschkönig live auf der Bühne - mit tollen Kostümen und Musik!",
            "ageGroups": ["3-6", "6-12"],
            "price": "12 Euro Kinder, 18 Euro Erwachsene",
            "address": "Franz-Joseph-Straße 47, 80801 München",
            "gps": {"lat": 48.163889, "lon": 11.583611},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.schauburg.net",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["rainy", "cloudy", "sunny"],
            "energyLevel": "calm",
            "duration": "1,5 Stunden",
            "parentTips": "Tickets online kaufen! Kommt 15 Min früher für gute Plätze.",
            "highlights": ["Drinnen", "Kulturell", "Ab 4 Jahren"],
            "amenities": ["restrooms", "stroller-friendly"]
        },
        {
            "id": "sample-workshop-1",
            "name": "Pizza-Back-Workshop für Kids",
            "nameKids": "Werde Pizza-Meister!",
            "date": (base_date + timedelta(days=14)).strftime("%Y-%m-%d"),
            "time": "14:00-16:30",
            "category": "Workshops & Kurse",
            "description": "Kinder lernen unter Anleitung wie man echte italienische Pizza macht - vom Teig bis zum Belag.",
            "descriptionKids": "Knete deinen eigenen Teig, wirf ihn in die Luft und belege deine Traum-Pizza!",
            "ageGroups": ["6-12"],
            "price": "25 Euro inkl. Material",
            "address": "Feierwerk Funkstation, Margarethe-Danzi-Straße 13",
            "gps": {"lat": 48.190556, "lon": 11.600278},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.feierwerk.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["rainy", "cloudy", "sunny"],
            "energyLevel": "moderate",
            "duration": "2,5 Stunden",
            "parentTips": "Begrenzte Plätze - schnell anmelden! Schürzen werden gestellt.",
            "highlights": ["Kreativ", "Lecker", "Lehrreich"],
            "amenities": ["food", "restrooms", "parking"]
        }
    ]

    return events

def create_sample_locations():
    """Generate realistic sample locations in Munich"""
    locations = [
        {
            "id": "loc-tierpark",
            "name": "Tierpark Hellabrunn",
            "nameKids": "Der große Zoo",
            "category": "Natur & Tiere",
            "description": "Münchens beliebter Tierpark mit über 750 Tierarten in naturnah gestalteten Lebensräumen.",
            "descriptionKids": "Besuche Elefanten, Pinguine, Affen und viele andere Tiere! Es gibt auch einen Streichelzoo.",
            "ageGroups": ["0-3", "3-6", "6-12", "12+"],
            "price": "15 Euro Erwachsene, 8 Euro Kinder (Jahreskarten verfügbar)",
            "address": "Tierparkstraße 30, 81543 München",
            "gps": {"lat": 48.098611, "lon": 11.558333},
            "openingHours": {"Mon-Sun": "09:00-18:00"},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.tierpark-hellabrunn.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["sunny", "cloudy"],
            "energyLevel": "active",
            "duration": "3-5 Stunden",
            "parentTips": "Bollerwagen können gemietet werden. Mehrere Spielplätze vorhanden!",
            "highlights": ["750+ Tierarten", "Streichelzoo", "Spielplätze", "Gastronomie"],
            "amenities": ["parking", "food", "restrooms", "stroller-friendly", "playground"]
        },
        {
            "id": "loc-museum",
            "name": "Kindermuseum München",
            "nameKids": "Mitmach-Museum",
            "category": "Museen & Ausstellungen",
            "description": "Interaktives Museum speziell für Kinder mit wechselnden Ausstellungen zum Anfassen und Mitmachen.",
            "descriptionKids": "Hier darfst du ALLES anfassen! Baue, entdecke und probiere aus!",
            "ageGroups": ["3-6", "6-12"],
            "price": "6 Euro Kinder, 8 Euro Erwachsene",
            "address": "Arnulfstraße 3, 80335 München",
            "gps": {"lat": 48.145833, "lon": 11.558056},
            "openingHours": {"Tue-Sun": "14:00-17:00"},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.kindermuseum-muenchen.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["rainy", "cloudy", "sunny"],
            "energyLevel": "moderate",
            "duration": "2-3 Stunden",
            "parentTips": "Am Wochenende oft voll - Tickets online reservieren!",
            "highlights": ["Interaktiv", "Pädagogisch", "Wechselnde Ausstellungen"],
            "amenities": ["food", "restrooms", "stroller-friendly"]
        },
        {
            "id": "loc-spielplatz",
            "name": "Westpark Spielplatz",
            "nameKids": "Riesen-Spielplatz",
            "category": "Spielplätze",
            "description": "Einer der größten und schönsten Spielplätze Münchens mit Klettergerüsten, Schaukeln und Wasserspiel.",
            "descriptionKids": "Riesige Rutsche, Seilbahn, Sandkiste und im Sommer ein cooler Wasserspielplatz!",
            "ageGroups": ["0-3", "3-6", "6-12"],
            "price": "Kostenlos",
            "address": "Westpark, 81373 München",
            "gps": {"lat": 48.117222, "lon": 11.530556},
            "openingHours": {"Mon-Sun": "Immer geöffnet"},
            "city": "München",
            "region": "BY",
            "country": "DE",
            "source": "sample_data",
            "link": "https://www.muenchen.de",
            "lastUpdated": datetime.now().isoformat(),
            "weatherSuitable": ["sunny", "cloudy"],
            "energyLevel": "very-active",
            "duration": "1-3 Stunden",
            "parentTips": "Wechselkleidung für Wasserspiel mitbringen! Schatten unter Bäumen.",
            "highlights": ["Kostenlos", "Wasserspiel", "Große Anlage"],
            "amenities": ["restrooms", "playground", "parking"]
        }
    ]

    return locations

def main():
    """Create sample data.json file"""
    events = create_sample_events()
    locations = create_sample_locations()

    data = {
        "events": events,
        "locations": locations,
        "totalEvents": len(events),
        "totalLocations": len(locations),
        "metadata": {
            "version": "4.1-sample",
            "lastCrawl": datetime.now().isoformat(),
            "crawlDurationSeconds": 0,
            "notice": "Sample data for MVP testing - Crawler blocked by Cloudflare on real sources"
        }
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ Created sample data.json with {len(events)} events and {len(locations)} locations")
    print(f"📊 Total items: {len(events) + len(locations)}")
    print("\n🔍 Events:")
    for event in events:
        print(f"   - {event['nameKids']} ({event['date']})")
    print("\n📍 Locations:")
    for loc in locations:
        print(f"   - {loc['nameKids']}")

if __name__ == "__main__":
    main()
