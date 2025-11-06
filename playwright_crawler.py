#!/usr/bin/env python3
"""
Playwright-based crawler for bypassing Cloudflare
Designed to run in GitHub Actions with Playwright installed
"""
import json
import asyncio
from datetime import datetime
from typing import Optional
import sys

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("⚠️  Playwright not available. Install with: pip install playwright && playwright install chromium")

async def fetch_with_browser(url: str, timeout: int = 30000) -> Optional[str]:
    """Fetch URL content using Playwright browser"""
    if not HAS_PLAYWRIGHT:
        return None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled'
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='de-DE',
                timezone_id='Europe/Berlin'
            )

            page = await context.new_page()

            # Navigate and wait for content
            await page.goto(url, wait_until='networkidle', timeout=timeout)

            # Wait a bit for JavaScript to load
            await page.wait_for_timeout(2000)

            # Get HTML content
            content = await page.content()

            await browser.close()

            return content

    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

async def crawl_sources_with_playwright(config_file: str = 'sources.config.json', max_sources: int = 10):
    """Crawl sources using Playwright"""
    print("🎭 Playwright Crawler Starting...")

    if not HAS_PLAYWRIGHT:
        print("❌ Playwright not installed!")
        sys.exit(1)

    # Load config
    with open(config_file, 'r') as f:
        config = json.load(f)

    results = []
    sources_to_crawl = []

    # Collect sources (HTML + RSS)
    html_sources = config.get('html', [])[:max_sources]  # Limit for testing
    rss_sources = [{'url': url, 'type': 'rss'} for url in config.get('rss', [])]

    sources_to_crawl.extend([{'url': s['url'], 'type': 'html', **s} for s in html_sources])
    sources_to_crawl.extend(rss_sources)

    print(f"📋 Crawling {len(sources_to_crawl)} sources...")

    for i, source in enumerate(sources_to_crawl, 1):
        url = source['url']
        print(f"\n[{i}/{len(sources_to_crawl)}] 🔍 {url}")

        content = await fetch_with_browser(url)

        if content:
            print(f"  ✅ Success - {len(content)} bytes")
            results.append({
                'url': url,
                'success': True,
                'size': len(content),
                'timestamp': datetime.now().isoformat()
            })

            # TODO: Parse content with BeautifulSoup here
            # For now just confirm we CAN get past Cloudflare

        else:
            print(f"  ❌ Failed")
            results.append({
                'url': url,
                'success': False,
                'timestamp': datetime.now().isoformat()
            })

        # Rate limit: wait between requests
        await asyncio.sleep(3)

    # Save results
    print(f"\n📊 Results: {sum(1 for r in results if r['success'])}/{len(results)} successful")

    with open('playwright_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print("💾 Saved to playwright_test_results.json")

    return results

async def main():
    """Main entry point"""
    results = await crawl_sources_with_playwright(max_sources=5)  # Test with 5 sources
    success_count = sum(1 for r in results if r['success'])

    if success_count > 0:
        print(f"\n✅ SUCCESS! Playwright bypassed Cloudflare on {success_count} sources!")
        sys.exit(0)
    else:
        print("\n❌ All sources failed")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
