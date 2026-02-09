"""
Hilton website scraper using Playwright with stealth
"""
import asyncio
import random
import logging
import json  # <-- Add this
from typing import Dict, Any
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import stealth  # <-- Correct import

logger = logging.getLogger(__name__)

class HiltonPlaywrightScraper:
    """Hilton scraper using Playwright with comprehensive anti-detection."""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        # Common desktop viewports for realism
        self._viewports = [
            {"width": 1920, "height": 1080},
            {"width": 1366, "height": 768},
            {"width": 1536, "height": 864}
        ]
    
    async def _create_stealth_context(self, browser) -> BrowserContext:
        """Creates a browser context with anti-detection configurations."""
        selected_viewport = random.choice(self._viewports)
        
        # Create context with realistic settings
        context = await browser.new_context(
            viewport=selected_viewport,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
            # Extra arguments to disable automation tells
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        # Inject JavaScript to disable the navigator.webdriver flag [citation:2]
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override permissions if needed
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
        return context
    
    async def _human_delay(self, min_ms: float = 100, max_ms: float = 700):
        """Introduces random delays to mimic human reading/response times."""
        delay = random.uniform(min_ms, max_ms) / 1000.0
        await asyncio.sleep(delay)
    
    async def _human_click(self, page: Page, selector: str):
        """Simulates a human-like click with slight movement and delay."""
        element = page.locator(selector)
        await element.wait_for(state="visible")
        
        # Get element position
        box = await element.bounding_box()
        if box:
            # Move to a random point within the element
            x = box['x'] + random.uniform(10, box['width'] - 10)
            y = box['y'] + random.uniform(10, box['height'] - 10)
            
            # Move mouse with intermediate steps
            await page.mouse.move(x, y, steps=random.randint(2, 5))
            await self._human_delay(50, 200)
            await page.mouse.down()
            await self._human_delay(20, 100)
            await page.mouse.up()
    
    async def _click_tab(self, page: Page, tab_id: str, panel_id: str):
        """Clicks a policy tab and waits for its panel."""
        try:
            # Wait for and click the tab
            tab_locator = page.locator(f"#{tab_id}")
            await tab_locator.wait_for(state="visible")
            await self._human_click(page, f"#{tab_id}")
            
            # Wait for panel with retry logic
            panel_locator = page.locator(f"#{panel_id}")
            await panel_locator.wait_for(state="visible", timeout=10000)
            await self._human_delay(500, 1000)  # Wait for content
            
            return panel_locator
        except Exception as e:
            logger.warning(f"Could not click tab {tab_id}: {e}")
            return None
    
    async def extract_data(self, url: str) -> Dict[str, Any]:
        """Main async method to extract hotel data."""
        async with async_playwright() as p:
            # Launch browser with additional anti-detection args [citation:5]
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials',
                    '--disable-features=BlockInsecurePrivateNetworkRequests'
                ]
            )
            
            try:
                # Create stealth-configured context
                context = await self._create_stealth_context(browser)
                page = await context.new_page()
                
                # Apply the stealth plugin [citation:1][citation:9]
                await stealth(page)
                
                logger.info(f"Navigating to {url}")
                await page.goto(url, wait_until="networkidle")
                await self._human_delay(2000, 4000)
                
                # Use Playwright's auto-waiting for stability [citation:4]
                await page.wait_for_load_state("domcontentloaded")
                
                # --- Extract Hotel Name ---
                hotel_name = ""
                try:
                    # Try multiple selectors
                    name_selectors = [
                        "h1.heading--base.heading--md",
                        "h1[data-testid='hotel-name']",
                        ".property-name",
                        "h1"
                    ]
                    
                    for selector in name_selectors:
                        try:
                            name_locator = page.locator(selector).first
                            if await name_locator.count() > 0:
                                hotel_name = (await name_locator.text_content()).strip()
                                if hotel_name:
                                    break
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Hotel name error: {e}")
                
                # --- Extract Description ---
                description = ""
                try:
                    desc_selectors = [
                        "p.text--base.text--md",
                        "[class*='description']",
                        ".property-description"
                    ]
                    
                    for selector in desc_selectors:
                        try:
                            desc_locator = page.locator(selector).first
                            if await desc_locator.count() > 0:
                                description = (await desc_locator.text_content()).strip()
                                if len(description) > 10:
                                    break
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Description error: {e}")
                
                # --- Extract Policies (with humanized interaction) ---
                policies = {
                    "parking": {},
                    "pets": {},
                    "smoking": "",
                    "wifi": ""
                }
                
                # Check if policy tabs exist
                tablist_exists = await page.locator("[role='tablist'], .policies-section").count() > 0
                if tablist_exists:
                    # Parking
                    try:
                        parking_panel = await self._click_tab(page, "policies-tab-0", "tab-panel-policies-tab-0")
                        if parking_panel:
                            # Extract parking data
                            items = await parking_panel.locator("li").all()
                            for item in items:
                                ps = await item.locator("p").all()
                                if len(ps) >= 2:
                                    label = (await ps[0].text_content()).strip()
                                    val = (await ps[1].text_content()).strip()
                                    if label:
                                        policies["parking"][label] = val
                    except Exception as e:
                        logger.warning(f"Parking policy error: {e}")
                    
                    # Pets
                    try:
                        pets_panel = await self._click_tab(page, "policies-tab-1", "tab-panel-policies-tab-1")
                        if pets_panel:
                            items = await pets_panel.locator("li").all()
                            for item in items:
                                ps = await item.locator("p").all()
                                if len(ps) >= 2:
                                    label = (await ps[0].text_content()).strip()
                                    val = (await ps[1].text_content()).strip()
                                    if label:
                                        policies["pets"][label] = val
                    except Exception as e:
                        logger.warning(f"Pets policy error: {e}")
                    
                    # Reset to first tab to avoid detection
                    await self._human_click(page, "#policies-tab-0")
                
                # --- Extract Amenities ---
                amenities = []
                try:
                    # Scroll to amenities section
                    await page.evaluate("window.scrollBy(0, 800)")
                    await self._human_delay(1000, 2000)
                    
                    amenity_selectors = [
                        "[data-testid^='grid-item-label-']",
                        ".amenity-item",
                        ".facility-item"
                    ]
                    
                    for selector in amenity_selectors:
                        amenity_elements = await page.locator(selector).all()
                        for elem in amenity_elements:
                            text = (await elem.text_content()).strip()
                            if text and text not in amenities:
                                amenities.append(text)
                        if amenities:
                            break
                except Exception as e:
                    logger.warning(f"Amenities error: {e}")
                
                # --- Final Data Assembly ---
                result = {
                    "hotel_name": hotel_name,
                    "description": description,
                    "policies": policies,
                    "amenities": amenities,
                    "url": url,
                    "success": bool(hotel_name)  # Basic success indicator
                }
                
                logger.info(f"Successfully extracted data for: {hotel_name}")
                return result
                
            except Exception as e:
                logger.error(f"Extraction failed: {e}")
                return {
                    "hotel_name": "",
                    "description": "",
                    "policies": {"parking": {}, "pets": {}, "smoking": "", "wifi": ""},
                    "amenities": [],
                    "url": url,
                    "success": False,
                    "error": str(e)
                }
            finally:
                await browser.close()

# Synchronous wrapper for convenience
class HiltonScraper:
    def __init__(self, headless=False):
        self.async_scraper = HiltonPlaywrightScraper(headless=headless)
    
    def extract_all_data(self, url: str) -> Dict[str, Any]:
        """Synchronous wrapper."""
        return asyncio.run(self.async_scraper.extract_data(url))
    



async def main():
    scraper = HiltonPlaywrightScraper(headless=True)
    
    # Test URL
    url = "https://www.hilton.com/en/hotels/anchwhw-homewood-suites-anchorage/"
    
    print("Starting stealth scrape...")
    result = await scraper.extract_data(url)
    
    print("\n" + "="*50)
    print(f"Hotel: {result['hotel_name']}")
    print(f"Success: {result['success']}")
    print(f"Amenities found: {len(result['amenities'])}")
    print("="*50)
    
    # Save to file
    with open("hilton_data.json", "w") as f:
        json.dump(result, f, indent=2)
    
    return result

if __name__ == "__main__":
    asyncio.run(main())