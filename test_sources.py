#!/usr/bin/env python3
"""
Test which sources are accessible without browser automation
"""
import json
import requests
from urllib.parse import urlparse

def test_source(url, timeout=10):
    """Test if a source is accessible"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        }
        r = requests.get(url, headers=headers, timeout=timeout, verify=False, allow_redirects=True)
        return r.status_code, len(r.text) if r.text else 0
    except requests.exceptions.Timeout:
        return 'TIMEOUT', 0
    except Exception as e:
        return f'ERROR: {type(e).__name__}', 0

def main():
    # Load sources
    with open('sources.config.json', 'r') as f:
        config = json.load(f)

    print("🧪 Testing all sources...\n")

    results = {
        'working': [],
        'blocked': [],
        'error': []
    }

    # Test HTML sources
    print("📄 Testing HTML sources:")
    for i, source in enumerate(config.get('html', [])[:20], 1):  # Test first 20
        url = source['url']
        domain = urlparse(url).netloc
        status, size = test_source(url)

        if status == 200:
            print(f"  ✅ [{i}] {domain} - {size} bytes")
            results['working'].append({'url': url, 'domain': domain, 'size': size})
        elif status == 403:
            print(f"  🚫 [{i}] {domain} - BLOCKED (Cloudflare)")
            results['blocked'].append({'url': url, 'domain': domain})
        else:
            print(f"  ❌ [{i}] {domain} - {status}")
            results['error'].append({'url': url, 'domain': domain, 'error': str(status)})

    # Test RSS sources
    print("\n📡 Testing RSS sources:")
    for i, url in enumerate(config.get('rss', []), 1):
        domain = urlparse(url).netloc
        status, size = test_source(url)

        if status == 200:
            print(f"  ✅ [{i}] {domain} - {size} bytes")
            results['working'].append({'url': url, 'domain': domain, 'size': size, 'type': 'rss'})
        elif status == 403:
            print(f"  🚫 [{i}] {domain} - BLOCKED")
            results['blocked'].append({'url': url, 'domain': domain, 'type': 'rss'})
        else:
            print(f"  ❌ [{i}] {domain} - {status}")
            results['error'].append({'url': url, 'domain': domain, 'error': str(status), 'type': 'rss'})

    # Test iCal sources
    print("\n📅 Testing iCal sources:")
    for i, url in enumerate(config.get('ical', []), 1):
        domain = urlparse(url).netloc
        status, size = test_source(url)

        if status == 200:
            print(f"  ✅ [{i}] {domain} - {size} bytes")
            results['working'].append({'url': url, 'domain': domain, 'size': size, 'type': 'ical'})
        elif status == 403:
            print(f"  🚫 [{i}] {domain} - BLOCKED")
            results['blocked'].append({'url': url, 'domain': domain, 'type': 'ical'})
        else:
            print(f"  ❌ [{i}] {domain} - {status}")
            results['error'].append({'url': url, 'domain': domain, 'error': str(status), 'type': 'ical'})

    # Summary
    print("\n" + "="*60)
    print("📊 SUMMARY")
    print("="*60)
    print(f"✅ Working sources: {len(results['working'])}")
    print(f"🚫 Blocked sources: {len(results['blocked'])}")
    print(f"❌ Error sources: {len(results['error'])}")

    if results['working']:
        print("\n🎯 Working sources (can be crawled now):")
        for s in results['working']:
            print(f"  - {s['domain']}")

    # Save results
    with open('source_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print("\n💾 Saved results to source_test_results.json")

if __name__ == '__main__':
    main()
