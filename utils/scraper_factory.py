"""
hotel_extraction/scraping/scraper_factory.py
"""
import logging
from typing import Optional
from scraping.hilton_scraper import HiltonScraper
from scraping.hyatt_scraper import HyattScraper
from scraping.base_scraper import BaseHotelScraper
from utils.chain_detector import HotelChainDetector

logger = logging.getLogger(__name__)

class HotelScraperFactory:
    """Factory to create appropriate scraper based on chain detection"""
    
    @staticmethod
    def create_scraper(chain: str, headless: bool = True) -> BaseHotelScraper:
        """
        Create scraper based on chain name
        
        Args:
            chain: Chain name (hilton, hyatt, etc.)
            headless: Whether to run in headless mode
            
        Returns:
            Appropriate scraper instance
        """
        chain_lower = chain.lower()
        
        if chain_lower == "hilton":
            return HiltonScraper(headless=headless)
        elif chain_lower == "hyatt":
            return HyattScraper(headless=headless)
        else:
            logger.warning(f"No specific scraper for chain: {chain}. Using base scraper.")
            scraper = BaseHotelScraper(headless=headless)
            scraper.chain_name = chain_lower
            return scraper
    
    @staticmethod
    def create_scraper_from_url(url: str, expected_chain: Optional[str] = None, headless: bool = True) -> BaseHotelScraper:
        """
        Create scraper from URL with chain verification
        
        Args:
            url: Hotel URL
            expected_chain: Expected chain (optional)
            headless: Whether to run in headless mode
            
        Returns:
            Appropriate scraper instance
        """
        # Verify chain
        verification = HotelChainDetector.verify_chain(url, expected_chain)
        
        if not verification["is_verified"]:
            logger.error(f"Chain verification failed: {verification['message']}")
            raise ValueError(verification["message"])
        
        # Use detected chain
        chain_to_use = verification["detected_chain"] or expected_chain or "unknown"
        
        logger.info(f"Creating {chain_to_use} scraper for URL: {url}")
        return HotelScraperFactory.create_scraper(chain_to_use, headless)