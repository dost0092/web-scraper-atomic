"""
Hotel Extraction Pipeline - Main orchestration
"""
import logging
from typing import Dict, Any

from utils.scraper_factory import HotelScraperFactory
from db.operations import HotelDatabaseOperations
from llm.web_context_generator import WebContextGenerator
from llm.pet_attribute_extractor import PetAttributeExtractor
from utils.slug_generator import generate_combined_slug
from utils.context_hashing import generate_raw_content_hash
from utils.address_parser import parse_address
from utils.chain_detector import HotelChainDetector
from utils.scraper_factory import HotelScraperFactory

logger = logging.getLogger(__name__)

class HotelExtractionPipeline:
    """Complete hotel extraction pipeline with chain detection"""
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.db_ops = HotelDatabaseOperations()
        self.web_context_gen = WebContextGenerator()
        self.pet_attr_extractor = PetAttributeExtractor()
    
    def extract_hotel(self, url: str, expected_chain: str = None) -> Dict[str, Any]:
        """
        Complete hotel extraction pipeline
        
        Steps:
        1. Detect and verify chain from URL
        2. Check if URL already exists in DB with correct chain
        3. Create appropriate scraper
        4. Scrape raw data
        5. Generate hash and save to DB
        6. Generate web context
        7. Extract pet attributes
        8. Generate web slug
        9. Return all data
        """
        try:
            logger.info(f"Starting extraction pipeline for URL: {url}")
            
            # Step 1: Detect and verify chain
            chain_info = HotelChainDetector.verify_chain(url, expected_chain)
            if not chain_info["is_verified"]:
                return {
                    "status": "error",
                    "message": chain_info["message"],
                    "url": url
                }
            
            detected_chain = chain_info["detected_chain"]
            logger.info(f"Verified chain: {detected_chain}")
            
            # Step 2: Check if URL already exists with correct chain
            existing_record = self.db_ops.check_url_exists_with_chain(url, detected_chain)
            if existing_record:
                logger.info(f"URL already exists in database with chain {detected_chain}: {url}")
                return {
                    "status": "exists",
                    "message": f"URL already extracted for {detected_chain} chain",
                    "record_id": existing_record[0],
                    "hash": existing_record[1],
                    "chain": detected_chain
                }
            
            # Step 3: Create appropriate scraper
            logger.info(f"Step 1: Creating {detected_chain} scraper...")
            scraper = HotelScraperFactory.create_scraper(detected_chain, self.headless)
            
            # Step 4: Scrape raw data
            logger.info("Step 2: Scraping hotel page...")
            hotel_data = scraper.extract_all_data(url)
            
            # Step 5: Generate hash
            logger.info("Step 3: Generating content hash...")
            hash_value = generate_raw_content_hash(hotel_data)
            
            # Step 6: Parse address
            address_info = parse_address(hotel_data.get('contact_info', {}).get('address', ''))
            
            # Step 7: Save raw extraction to database
            logger.info("Step 4: Saving raw extraction to database...")
            record_id = self.db_ops.save_raw_extraction(
                url=url,
                raw_content=' '.join([
                    hotel_data.get('hotel_name', ''),
                    hotel_data.get('description', ''),
                    hotel_data.get('contact_info', {}).get('address', ''),
                ]),
                hash_value=hash_value,
                hotel_name=hotel_data.get('hotel_name', ''),
                address=hotel_data.get('contact_info', {}).get('address', ''),
                chain=detected_chain
            )
            
            # Step 8: Generate web context
            logger.info("Step 5: Generating web context...")
            web_context = self.web_context_gen.generate(hotel_data)
            
            # Step 9: Save web context to database
            logger.info("Step 6: Saving web context to database...")
            self.db_ops.save_web_context(record_id, web_context)
            
            # Step 10: Extract pet attributes using Gemini
            logger.info("Step 7: Extracting pet attributes...")
            pet_attributes = self.pet_attr_extractor.extract(web_context)
            
            # Step 11: Save pet attributes to database
            logger.info("Step 8: Saving pet attributes to database...")
            if pet_attributes:  # Only save if we have attributes
                self.db_ops.save_pet_attributes(record_id, pet_attributes)
            
            # Step 12: Generate web slug
            logger.info("Step 9: Generating web slug...")
            web_slug = generate_combined_slug(
                country_code=address_info.get('country_code', 'US'),
                state_code=address_info.get('state_code', ''),
                city=address_info.get('city', ''),
                hotel_name=hotel_data.get('hotel_name', ''),
                address_line_1=address_info.get('address_line_1', ''),
                chain=detected_chain
            )
            
            # Step 13: Update database with slug
            logger.info("Step 10: Updating database with slug...")
            if web_slug:
                self.db_ops.update_web_slug(record_id, web_slug)
            
            # Return complete result
            result = {
                "status": "success",
                "record_id": record_id,
                "hash": hash_value,
                "hotel_data": hotel_data,
                "address_info": address_info,
                "web_context": web_context,
                "pet_attributes": pet_attributes,
                "web_slug": web_slug,
                "url": url,
                "chain": detected_chain
            }
            
            logger.info(f"Extraction completed successfully for {url} ({detected_chain})")
            return result
            
        except Exception as e:
            logger.error(f"Error in extraction pipeline: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "url": url,
                "chain": HotelChainDetector.detect_chain_from_url(url) if 'url' in locals() else "unknown"
            }