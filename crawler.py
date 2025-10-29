import json
import requests
from datetime import datetime

# Super simpler Crawler - nur RSS Feeds lesen
def get_events():
    events = []
    
    # Beispiel: Feste vordefinierte Events
    # Später kannst du hier echte Webseiten crawlen
    
    events.append({
        "id": "auto-" + datetime.now().strftime("%Y%m%d"),
        "name": "Kindertheater im Gasteig",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "category": "theater",
        "description": "Automatisch gefunden!",
        "source": "crawler",
        "lastUpdated": datetime.now().isoformat()
    })
    
    return events

# Hauptprogramm
def main():
    # Events sammeln
    new_events = get_events()
    
    # Mit alten Daten mergen
    try:
        with open('data.json', 'r') as f:
            data = json.load(f)
    except:
        data = {"locations": [], "events": []}
    
    # Update
    data["events"] = new_events
    data["lastCrawled"] = datetime.now().isoformat()
    data["totalEvents"] = len(new_events)
    
    # Speichern
    with open('data.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ {len(new_events)} Events gespeichert")

if __name__ == "__main__":
    main()
