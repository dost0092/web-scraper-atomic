"""
hotel_extraction/scraping/base_scraper.py
"""
import logging
import time
from typing import Dict, Any
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
logger = logging.getLogger(__name__)


class BaseHotelScraper:
    """Base class for all hotel scrapers"""

    def __init__(self, headless: bool = False, timeout: int = 30):
        self.headless = headless
        self.timeout = timeout
        self.driver = None
        self.chain_name = "generic"

    # ---------------- DRIVER SETUP ---------------- #

    def _make_uc_options(self) -> uc.ChromeOptions:
        opts = uc.ChromeOptions()
        if self.headless:
            opts.add_argument("--headless=new")

        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--window-size=1920,1080")

        return opts

    def _get_driver(self):
        try:
            options = self._make_uc_options()
            driver = uc.Chrome(
                options=options,
                version_main=144,
                headless=self.headless,
                use_subprocess=True
            )
            driver.set_page_load_timeout(self.timeout)
            return driver
        except Exception as e:
            logger.exception("Failed to initialize Chrome driver")
            raise e

    # ---------------- MAIN ENTRY ---------------- #

    def extract_all_data(self, url: str) -> Dict[str, Any]:
        driver = None
        try:
            driver = self._get_driver()
            wait = WebDriverWait(driver, self.timeout)

            logger.info(f"[{self.chain_name.upper()}] Opening {url}")
            driver.get(url)

            self._wait_for_page_ready(driver)

            return self._extract_hotel_data(driver, wait)

        except Exception as e:
            logger.exception("Fatal scrape error")
            return self._get_empty_data(url)

        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    # ---------------- OVERRIDABLE ---------------- #

    def _extract_hotel_data(self, driver, wait) -> Dict[str, Any]:
        raise NotImplementedError

    # ---------------- HELPERS ---------------- #

    def _wait_for_page_ready(self, driver):
        """
        Waits for Hyatt React page + pet section hydration.
        Compatible with BaseHotelScraper.
        """
        wait = WebDriverWait(driver, 40)

        # 1️⃣ DOM ready
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

        # 2️⃣ Hyatt React mount (pets OR body fallback)
        wait.until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "[data-locator='pets-overview-text'], body"
                )
            )
        )

        # 3️⃣ Extra hydration buffer (VERY IMPORTANT for Hyatt)
        time.sleep(1.5)

    def _safe_find_elements(self, driver, by, selector):
        try:
            return driver.find_elements(by, selector)
        except Exception:
            return []

    def _safe_get_text(self, element, default: str = "") -> str:
        try:
            return element.text.strip() if element else default
        except Exception:
            return default

    # ---------------- FALLBACK DATA ---------------- #

    def _get_empty_data(self, url: str) -> Dict[str, Any]:
        return {
            "hotel_name": "",
            "description": "",
            "contact_info": {
                "address": "",
                "phone": ""
            },
            "amenities": [],
            "parking_policy": {},
            "pets_policy": {},
            "smoking_policy": "",
            "wifi_policy": "",
            "rating": "",
            "url": url,
            "chain": self.chain_name
        }
