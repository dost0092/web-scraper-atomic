"""
Hilton website scraper
"""
import logging
import time
from typing import Dict, Any, List
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, SessionNotCreatedException

import undetected_chromedriver as uc

logger = logging.getLogger(__name__)

class HiltonScraper:
    """Standalone Hilton scraper with Chrome version handling"""
    
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
    
    def _click_tab(self, driver, wait, tab_id, panel_id):
        """Clicks a tab button and waits until its panel becomes visible."""
        try:
            btn = wait.until(EC.element_to_be_clickable((By.ID, tab_id)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            time.sleep(0.5)
            panel = wait.until(EC.visibility_of_element_located((By.ID, panel_id)))
            return panel
        except Exception as e:
            logger.warning(f"Error clicking tab {tab_id}: {e}")
            raise
    
    def _parse_parking_html(self, container_el):
        """Parse parking tab content"""
        items = {}
        try:
            lis = container_el.find_elements(By.CSS_SELECTOR, "li")
            for li in lis:
                ps = li.find_elements(By.TAG_NAME, "p")
                if len(ps) >= 2:
                    label = ps[0].text.strip()
                    val = ps[1].text.strip()
                    if label:
                        items[label] = val
        except Exception as e:
            logger.warning(f"Error parsing parking: {e}")
        return items
    
    def _parse_pets_html(self, container_el):
        """Parse pets tab content"""
        items = {}
        try:
            lis = container_el.find_elements(By.CSS_SELECTOR, "li")
            for li in lis:
                ps = li.find_elements(By.TAG_NAME, "p")
                if len(ps) >= 2:
                    label = ps[0].text.strip()
                    val = ps[1].text.strip()
                    if label:
                        items[label] = val
        except Exception as e:
            logger.warning(f"Error parsing pets: {e}")
        return items
    
    def _parse_smoking_html(self, container_el):
        """Parse smoking tab content"""
        try:
            text_el = container_el.find_element(By.CSS_SELECTOR, "[data-testid='policy-smoking']")
            return text_el.text.strip()
        except NoSuchElementException:
            try:
                ps = container_el.find_elements(By.TAG_NAME, "p")
                if ps:
                    return ps[0].text.strip()
            except:
                pass
        return ""
    
    def _parse_wifi_html(self, container_el):
        """Parse WiFi tab content"""
        try:
            text_el = container_el.find_element(By.CSS_SELECTOR, "[data-testid='policy-wifi']")
            return text_el.text.strip()
        except NoSuchElementException:
            try:
                ps = container_el.find_elements(By.TAG_NAME, "p")
                if ps:
                    return ps[0].text.strip()
            except:
                pass
        return ""
    
    def _parse_amenities(self, driver):
        """Parse amenities grid"""
        try:
            selectors = [
                "[data-testid='icon-grid-header']",
                ".amenities-section",
                "[class*='amenities']",
                "h2:contains('Amenities')"
            ]
            
            for selector in selectors:
                try:
                    driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            labels = []
            amenity_selectors = [
                "[data-testid^='grid-item-label-']",
                ".amenity-item",
                "[class*='amenity'] p",
                ".facility-item",
                "li[aria-label]"
            ]
            
            for selector in amenity_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for el in elements:
                        text = el.text.strip()
                        if text and len(text) > 1:
                            labels.append(text)
                    if labels:
                        break
                except:
                    continue
            
            seen = set()
            out = []
            for x in labels:
                if x and x not in seen:
                    seen.add(x)
                    out.append(x)
            return out
            
        except Exception as e:
            logger.warning(f"Could not parse amenities: {e}")
            return []
    
    def extract_all_data(self, url: str, wait_timeout: int = 30) -> Dict[str, Any]:
        """Main method to extract all data from Hilton website."""
        driver = None
        try:
            driver = self._get_driver()
            wait = WebDriverWait(driver, wait_timeout)
            
            logger.info(f"Opening URL: {url}")
            driver.get(url)
            time.sleep(2)
            
            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(1)
            
            # Extract hotel name
            hotel_name = ""
            try:
                # Try XPath first since you have the exact structure
                xpaths = [
                    "//h1[contains(@class, 'heading--base') and contains(@class, 'heading--md')]",
                    "//div[contains(@class, 'text-balance')]/h1",
                    "//*[@id='__next']/div[2]/div/div[1]/div[1]//h1",
                    "/html/body/div[1]/div/div[2]/div/div[1]/div[1]//h1"
                ]
                
                for xpath in xpaths:
                    try:
                        name_element = driver.find_element(By.XPATH, xpath)
                        hotel_name = name_element.text.strip()
                        if hotel_name:
                            break
                    except:
                        continue
                
                # If XPath fails, fall back to CSS selectors
                if not hotel_name:
                    name_selectors = [
                        "h1.heading--base.heading--md",
                        ".text-balance h1",
                        "h1[data-testid='hotel-name']",
                        ".hotel-name",
                        "h1.property-name"
                    ]
                    for selector in name_selectors:
                        try:
                            name_element = driver.find_element(By.CSS_SELECTOR, selector)
                            hotel_name = name_element.text.strip()
                            if hotel_name:
                                break
                        except:
                            continue
                            
            except Exception as e:
                logger.warning(f"Could not find hotel name: {e}")
            
            # Extract description
            description = ""
            try:
                # Try XPath approach first since you have the exact XPath
                xpaths = [
                    "//p[contains(@class, 'text--base') and contains(@class, 'text--md')]",
                    "//div[contains(@class, 'container') and contains(@class, 'border-b')]//p",
                    "//*[@id='__next']/div[6]/div/div[1]//p",
                    "/html/body/div[1]/div/div[6]/div/div[1]//p"
                ]
                
                for xpath in xpaths:
                    try:
                        desc_element = driver.find_element(By.XPATH, xpath)
                        description = desc_element.text.strip()
                        if description:
                            break
                    except:
                        continue
                
                # If XPath fails, fall back to CSS selectors
                if not description:
                    desc_selectors = [
                        "p.text--base.text--md",
                        ".container.border-b p",
                        "[class*='description'] p",
                        ".property-description",
                        ".hotel-description"
                    ]
                    for selector in desc_selectors:
                        try:
                            desc_element = driver.find_element(By.CSS_SELECTOR, selector)
                            description = desc_element.text.strip()
                            if description:
                                break
                        except:
                            continue
                            
            except Exception as e:
                logger.warning(f"Could not find property description: {e}")
            
            # Extract address
            contact_info = {"address": "", "phone": ""}
            address_selectors = [
                "span.underline-offset-2.underline.inline-block", 
                "[data-testid='property-address']",
                ".property-address",
                "[itemprop='address']"
            ]

            for selector in address_selectors:
                try:
                    address_element = driver.find_element(By.CSS_SELECTOR, selector)
                    contact_info['address'] = address_element.text.strip()
                    if contact_info['address']: 
                        break
                except:
                    continue

            if not contact_info['address']:
                try:
                    xpath_selector = '//*[@id="__next"]/div[2]/div/div[1]/div[2]/a/span[1]'
                    address_element = driver.find_element(By.XPATH, xpath_selector)
                    contact_info['address'] = address_element.text.strip()
                except Exception as e:
                    logger.warning(f"Could not find address via CSS or XPath: {e}")
            
            # Extract phone
            try:
                phone_selectors = [
                    "[data-testid='property-phone']",
                    ".property-phone",
                    ".hotel-phone",
                    "[href^='tel:']"
                ]
                for selector in phone_selectors:
                    try:
                        phone_element = driver.find_element(By.CSS_SELECTOR, selector)
                        contact_info['phone'] = phone_element.text.strip()
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Could not find phone: {e}")
            
            # Try to click policy tabs if they exist
            parking_policy = {}
            pets_policy = {}
            smoking_policy = ""
            wifi_policy = ""
            
            try:
                policies_section = driver.find_elements(By.CSS_SELECTOR, "[role='tablist'], .policies-section, #policies-tab-0")
                if policies_section:
                    # Parking
                    try:
                        parking_panel = self._click_tab(driver, wait, "policies-tab-0", "tab-panel-policies-tab-0")
                        parking_policy = self._parse_parking_html(parking_panel)
                    except Exception as e:
                        logger.warning(f"Could not scrape parking policy: {e}")
                    
                    # Pets
                    try:
                        pets_panel = self._click_tab(driver, wait, "policies-tab-1", "tab-panel-policies-tab-1")
                        pets_policy = self._parse_pets_html(pets_panel)
                    except Exception as e:
                        logger.warning(f"Could not scrape pets policy: {e}")
                    
                    # Smoking
                    try:
                        smoking_panel = self._click_tab(driver, wait, "policies-tab-2", "tab-panel-policies-tab-2")
                        smoking_policy = self._parse_smoking_html(smoking_panel)
                    except Exception as e:
                        logger.warning(f"Could not scrape smoking policy: {e}")
                    
                    # WiFi
                    try:
                        wifi_panel = self._click_tab(driver, wait, "policies-tab-3", "tab-panel-policies-tab-3")
                        wifi_policy = self._parse_wifi_html(wifi_panel)
                    except Exception as e:
                        logger.warning(f"Could not scrape WiFi policy: {e}")
            except Exception as e:
                logger.warning(f"Could not access policy tabs: {e}")
            
            # Extract amenities
            amenities = self._parse_amenities(driver)
            
            # Extract rating
            rating = ""
            try:
                rating_selectors = [
                    "[data-testid='review-rating']",
                    ".rating-score",
                    ".review-rating",
                    "[class*='rating'] strong"
                ]
                for selector in rating_selectors:
                    try:
                        rating_element = driver.find_element(By.CSS_SELECTOR, selector)
                        rating = rating_element.text.strip()
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Could not find rating: {e}")
            print("Extraction complete.")
            print(hotel_name, description, contact_info, amenities, parking_policy, pets_policy, smoking_policy, wifi_policy, rating)
            return {
                "hotel_name": hotel_name,
                "description": description,
                "contact_info": contact_info,
                "amenities": amenities,
                "parking_policy": parking_policy,
                "pets_policy": pets_policy,
                "smoking_policy": smoking_policy,
                "wifi_policy": wifi_policy,
                "rating": rating,
                "url": url
            }
            
        except Exception as e:
            logger.error(f"Error in extract_all_data: {e}")
            return {
                "hotel_name": "",
                "description": "",
                "contact_info": {"address": "", "phone": ""},
                "amenities": [],
                "parking_policy": {},
                "pets_policy": {},
                "smoking_policy": "",
                "wifi_policy": "",
                "rating": "",
                "url": url
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = HiltonScraper(headless=False)
    url = "https://www.hilton.com/en/hotels/ancakhx-hampton-anchorage/"
    data = scraper.extract_all_data(url)
    print(data)