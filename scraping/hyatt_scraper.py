"""
Hyatt website scraper - USING YOUR WORKING DRIVER SETUP
"""
import logging
import time
import json
import re
from typing import Dict, Any, List
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, SessionNotCreatedException

import undetected_chromedriver as uc

logger = logging.getLogger(__name__)

class HyattScraper:
    """Standalone Hyatt scraper with robust data extraction"""
    
    def __init__(self, headless=False):
        self.headless = headless
        self.driver = None
    
    def _make_uc_options(self):
        """Create undetected Chrome options"""
        opts = uc.ChromeOptions()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        return opts
    
    def _get_driver(self):
        """Get Chrome driver with version handling"""
        try:
            driver = uc.Chrome(
                options=self._make_uc_options(),
                version_main=None
            )
            return driver
        except SessionNotCreatedException as e:
            logger.warning(f"Auto-detect failed: {e}")
            try:
                driver = uc.Chrome(
                    options=self._make_uc_options(),
                    version_main=144
                )
                return driver
            except Exception:
                logger.info("Trying with browser_executable_path...")
                driver = uc.Chrome(
                    options=self._make_uc_options(),
                    browser_executable_path=None
                )
                return driver
    
    def _wait_for_page_load(self, driver, timeout=30):
        """Wait for page to fully load"""
        try:
            # Wait for document ready state
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Additional wait for body to be present
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            return True
        except Exception as e:
            logger.warning(f"Page load wait failed: {e}")
            return False
    
    def _safe_find_element(self, driver, by, value, timeout=10):
        """Safely find element with timeout"""
        try:
            return WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
        except:
            return None
    
    def _safe_find_elements(self, driver, by, value, timeout=10):
        """Safely find elements with timeout"""
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return driver.find_elements(by, value)
        except:
            return []
    
    def _safe_text(self, element):
        """Safely extract text from element"""
        if element is None:
            return ""
        try:
            text = element.text.strip()
            return text if text else ""
        except:
            return ""
    
    def _click_if_exists(self, driver, selector):
        """Click element if it exists and is clickable"""
        try:
            # Try CSS selector
            element = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            driver.execute_script("arguments[0].click();", element)
            time.sleep(1)
            return True
        except:
            try:
                # Try text contains
                element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{selector}')]"))
                )
                driver.execute_script("arguments[0].click();", element)
                time.sleep(1)
                return True
            except:
                return False
    
    def _extract_hotel_name(self, driver):
        """Extract hotel name with multiple strategies"""
        hotel_name = ""
        
        # Try common Hyatt selectors
        selectors = [
            "h1.be-headline-standard-1",
            "h1[class*='be-headline']",
            "h1.property-name",
            "h1.hotel-name",
            "h1[data-testid='property-name']",
            ".property-title h1",
            ".hotel-header h1",
            "header h1",
            "h1",
            "h1.sc-ab4365b0-2"  # Common Hyatt class
        ]
        
        for selector in selectors:
            try:
                name_element = driver.find_element(By.CSS_SELECTOR, selector)
                hotel_name = self._safe_text(name_element)
                if hotel_name and len(hotel_name) > 2:
                    logger.info(f"Found hotel name via CSS: {hotel_name}")
                    return hotel_name
            except:
                continue
        
        # Try JavaScript fallback
        try:
            script = """
            // Try to find hotel name
            const selectors = [
                'h1.be-headline-standard-1',
                'h1[class*="be-headline"]',
                'h1.property-name',
                'h1.hotel-name',
                'h1[data-testid="property-name"]',
                '.property-title h1',
                'h1'
            ];
            
            for (const selector of selectors) {
                const element = document.querySelector(selector);
                if (element) {
                    const text = element.textContent.trim();
                    if (text && text.length > 3) {
                        return text;
                    }
                }
            }
            
            return '';
            """
            hotel_name = driver.execute_script(script)
            if hotel_name:
                logger.info(f"Found hotel name via JS: {hotel_name}")
                return hotel_name
        except Exception as e:
            logger.warning(f"JS hotel name extraction failed: {e}")
        
        return hotel_name
    
    def _extract_description(self, driver):
        """Extract hotel description"""
        description = ""
        
        # Hyatt specific selectors
        selectors = [
            "p.be-text-body-2",
            "div[class*='description'] p",
            ".property-description p",
            ".overview-section p",
            "[data-testid='property-description']",
            "p.Body-2",
            "section[class*='overview'] p",
            "div.sc-382996da-0 p"  # Common Hyatt container
        ]
        
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = self._safe_text(element)
                    if text and len(text) > 30:
                        description = text
                        logger.info(f"Found description via CSS ({len(description)} chars)")
                        return description
            except:
                continue
        
        return description
    
    def _extract_address(self, driver):
        """Extract address information"""
        address_text = ""
        
        # Common Hyatt address selectors
        selectors = [
            "[data-testid='property-address']",
            ".property-address",
            ".hotel-address",
            "address",
            "[itemprop='address']",
            "div[class*='address']",
            "span.be-text-body-2",
            ".sc-382996da-0 span"  # Common Hyatt container
        ]
        
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = self._safe_text(element)
                    if text and (',' in text or len(text) > 10):
                        address_text = text
                        logger.info(f"Found address via selector {selector}: {address_text}")
                        break
                if address_text:
                    break
            except:
                continue
        
        # Parse address
        address = ""
        city = ""
        state = ""
        country = ""
        postal_code = ""
        
        if address_text:
            # Clean up
            address_text = re.sub(r'\s+', ' ', address_text).strip()
            parts = [p.strip() for p in address_text.split(',')]
            
            if len(parts) == 1:
                address = parts[0]
            elif len(parts) >= 2:
                address = parts[0]
                city = parts[1]
                
                if len(parts) >= 3:
                    state_zip = parts[2].strip()
                    # Try to split state and ZIP
                    zip_match = re.search(r'(\d{5}(?:-\d{4})?)', state_zip)
                    if zip_match:
                        postal_code = zip_match.group(1)
                        state = state_zip.replace(postal_code, '').strip()
                    else:
                        state = state_zip
                
                if len(parts) >= 4:
                    country = parts[3]
            
            # Extract postal code if not found
            if not postal_code:
                zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address_text)
                if zip_match:
                    postal_code = zip_match.group()
        
        return {
            "address": address,
            "city": city,
            "state": state,
            "country": country if country else "USA",
            "postal_code": postal_code,
            "full_address": address_text
        }
    
    def _extract_phone(self, driver):
        """Extract phone number"""
        phone = ""
        
        selectors = [
            'a[href^="tel:"]',
            '[data-testid="phone-number"]',
            '.contact-phone',
            '.phone-number',
            '[itemprop="telephone"]'
        ]
        
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    # Try href
                    href = element.get_attribute("href")
                    if href and "tel:" in href:
                        phone = re.sub(r'[^\d\+]', '', href.replace("tel:", ""))
                        if phone:
                            return phone
                    
                    # Try text
                    text = self._safe_text(element)
                    if text:
                        digits = re.sub(r'[^\d]', '', text)
                        if 7 <= len(digits) <= 15:
                            phone = text
                            return phone
            except:
                continue
        
        return phone
    
    def _extract_amenities(self, driver):
        """Extract amenities"""
        amenities = []
        
        # Hyatt amenity selectors
        selectors = [
            "div[class*='Amenities'] li",
            "div[class*='amenities'] li",
            "[data-testid='amenities-list'] li",
            ".amenities-list li",
            "ul[class*='amenities'] li",
            ".amenity-item",
            ".facility-item",
            "li[aria-label]",
            "div.sc-382996da-0 li"  # Common Hyatt container
        ]
        
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = self._safe_text(element)
                    if text and 2 < len(text) < 100:
                        amenities.append(text)
            except:
                continue
        
        # Try JavaScript extraction
        if len(amenities) < 5:
            try:
                script = """
                const amenities = [];
                
                // Look for amenities sections
                const sections = document.querySelectorAll('section, div');
                for (const section of sections) {
                    const text = section.textContent.toLowerCase();
                    if (text.includes('amenit') || text.includes('featur') || text.includes('facilit')) {
                        // Get text content
                        const items = section.textContent.split(/\\n/);
                        for (let item of items) {
                            item = item.trim();
                            if (item && item.length > 2 && item.length < 100) {
                                amenities.push(item);
                            }
                        }
                    }
                }
                
                return amenities.slice(0, 50);
                """
                
                js_amenities = driver.execute_script(script)
                if js_amenities:
                    # Add unique amenities
                    existing_set = set(amenities)
                    for amenity in js_amenities:
                        if amenity and amenity not in existing_set:
                            amenities.append(amenity)
            except:
                pass
        
        # Deduplicate
        seen = set()
        unique_amenities = []
        for amenity in amenities:
            if amenity:
                clean_amenity = re.sub(r'\s+', ' ', amenity).strip()
                if clean_amenity and clean_amenity.lower() not in seen:
                    seen.add(clean_amenity.lower())
                    unique_amenities.append(clean_amenity)
        
        logger.info(f"Found {len(unique_amenities)} amenities")
        return unique_amenities[:50]
    
    def _extract_pet_policy(self, driver):
        """Extract pet policy information"""
        pet_info = {
            "policy": "",
            "fees": [],
            "weight_limits": [],
            "restrictions": []
        }
        
        try:
            # Get page text
            all_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Check for pet mentions
            if any(keyword in all_text.lower() for keyword in ["pet", "dog", "animal"]):
                # Look for pet section
                pet_selectors = [
                    "div[data-locator*='pets']",
                    "div[class*='Pet']",
                    "div[class*='pet']",
                    "section[aria-label*='pet']"
                ]
                
                pet_section = None
                for selector in pet_selectors:
                    try:
                        element = driver.find_element(By.CSS_SELECTOR, selector)
                        if element.is_displayed():
                            pet_section = element
                            break
                    except:
                        continue
                
                # Extract text
                if pet_section:
                    pet_text = self._safe_text(pet_section)
                else:
                    pet_text = all_text
                
                # Parse lines
                lines = pet_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    lower_line = line.lower()
                    if any(keyword in lower_line for keyword in ["pet", "dog", "animal"]):
                        if not pet_info["policy"] and len(line) > 20:
                            pet_info["policy"] = line
                        
                        if '$' in line or 'fee' in lower_line:
                            pet_info["fees"].append(line)
                        
                        if 'pound' in lower_line or 'lb' in lower_line:
                            pet_info["weight_limits"].append(line)
                        
                        if 'maximum' in lower_line or 'limit' in lower_line:
                            pet_info["restrictions"].append(line)
        except Exception as e:
            logger.warning(f"Error extracting pet policy: {e}")
        
        return pet_info
    
    def _extract_rating(self, driver):
        """Extract hotel rating"""
        rating = ""
        
        selectors = [
            "[data-testid='review-rating']",
            ".rating-score",
            ".review-rating",
            "[class*='rating'] strong",
            "span[class*='rating']",
            "div[class*='rating']"
        ]
        
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    rating_text = self._safe_text(element)
                    if rating_text:
                        # Extract numbers
                        rating_match = re.search(r'(\d+(?:\.\d+)?)/?\d*', rating_text)
                        if rating_match:
                            rating = rating_match.group(1)
                            logger.info(f"Found rating: {rating}")
                            return rating
            except:
                continue
        
        return rating
    
    def extract_all_data(self, url: str, wait_timeout: int = 40) -> Dict[str, Any]:
        """Main method to extract all data from Hyatt website"""
        driver = None
        try:
            # Create driver using your working method
            driver = self._get_driver()
            
            logger.info(f"Opening URL: {url}")
            
            # Load page with retry
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    driver.get(url)
                    
                    # Wait for page
                    if self._wait_for_page_load(driver, wait_timeout):
                        logger.info("Page loaded successfully")
                        break
                    else:
                        logger.warning(f"Page load attempt {attempt + 1} failed")
                        if attempt < max_retries - 1:
                            time.sleep(3)
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(3)
                    else:
                        raise
            
            # Wait for dynamic content
            time.sleep(3)
            
            # Try to accept cookies
            try:
                accept_buttons = [
                    "//button[contains(text(), 'Accept')]",
                    "//button[contains(text(), 'Got it')]",
                    "//button[contains(text(), 'OK')]",
                    "#onetrust-accept-btn-handler"
                ]
                
                for xpath in accept_buttons:
                    try:
                        cookie_btn = WebDriverWait(driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, xpath))
                        )
                        driver.execute_script("arguments[0].click();", cookie_btn)
                        logger.info("Clicked cookie accept button")
                        time.sleep(2)
                        break
                    except:
                        continue
            except:
                pass
            
            # Scroll a bit
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(2)
            
            # Extract data
            hotel_name = self._extract_hotel_name(driver)
            description = self._extract_description(driver)
            address_info = self._extract_address(driver)
            phone = self._extract_phone(driver)
            amenities = self._extract_amenities(driver)
            pet_policy = self._extract_pet_policy(driver)
            rating = self._extract_rating(driver)
            
            # Prepare result
            result = {
                "hotel_name": hotel_name,
                "description": description,
                "contact_info": {
                    "address": address_info["full_address"],
                    "city": address_info["city"],
                    "state": address_info["state"],
                    "country": address_info["country"],
                    "postal_code": address_info["postal_code"],
                    "phone": phone
                },
                "amenities": amenities,
                "pets_policy": pet_policy,
                "parking_policy": "",
                "smoking_policy": "",
                "wifi_policy": "",
                "rating": rating,
                "url": url,
                "status": "success",
                "timestamp": time.time()
            }
            
            logger.info(f"Successfully extracted data for: {hotel_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}", exc_info=True)
            return {
                "hotel_name": "",
                "description": "",
                "contact_info": {
                    "address": "",
                    "city": "",
                    "state": "",
                    "country": "",
                    "postal_code": "",
                    "phone": ""
                },
                "amenities": [],
                "pets_policy": {
                    "policy": "",
                    "fees": [],
                    "weight_limits": [],
                    "restrictions": []
                },
                "parking_policy": "",
                "smoking_policy": "",
                "wifi_policy": "",
                "rating": "",
                "url": url,
                "status": f"error: {str(e)}",
                "timestamp": time.time()
            }
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("Driver closed")
                except:
                    pass


# =====================================================
# TEST FUNCTION
# =====================================================

def test_hyatt_scraper():
    """Test the scraper"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("HYATT SCRAPER TEST")
    print("=" * 60)
    
    # Start with headless=False to debug
    scraper = HyattScraper(headless=False)
    
    # Test URL from your error
    test_url = "https://www.hyatt.com/hyatt-place/en-US/yqmzm-hyatt-place-moncton-downtown"
    
    print(f"\nTesting URL: {test_url}")
    
    try:
        start_time = time.time()
        data = scraper.extract_all_data(test_url)
        elapsed = time.time() - start_time
        
        print(f"\n✅ Extraction completed in {elapsed:.2f}s")
        print(f"Status: {data.get('status', 'unknown')}")
        
        if data.get('hotel_name'):
            print(f"\n📊 RESULTS:")
            print(f"  Hotel: {data['hotel_name']}")
            print(f"  Address: {data['contact_info']['address']}")
            print(f"  Phone: {data['contact_info']['phone']}")
            print(f"  Description: {data['description'][:100]}..." if data['description'] else "  Description: Not found")
            print(f"  Amenities: {len(data['amenities'])} items")
            if data['amenities']:
                print(f"    Sample: {', '.join(data['amenities'][:3])}")
            if data['pets_policy']['policy']:
                print(f"  Pet Policy: {data['pets_policy']['policy'][:100]}...")
            if data['pets_policy']['fees']:
                print(f"  Pet Fees: {len(data['pets_policy']['fees'])} items")
            print(f"  Rating: {data['rating']}")
        else:
            print("\n❌ Failed to extract hotel name")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_hyatt_scraper()