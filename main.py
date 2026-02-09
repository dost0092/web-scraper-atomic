"""
FastAPI main application - Hotel Scraper Endpoint
"""

import os
import logging
import json
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv
from utils.chain_detector import HotelChainDetector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Hotel Scraper API",
    description="API for scraping hotel locations and extracting hotel data",
    version="2.0.0"
)

# Active scraping sessions
active_scrapes = {}

# ------------------------------------------------------------------
# Import scrapers and extractors
# ------------------------------------------------------------------
try:
    # Import Hilton Locations Scraper
    from url.hilton_location_scraper import HiltonLocationsScraper
    
    # Import Hotel Extraction Pipeline
    # Note: You'll need to adjust the import path based on your actual file structure
    # If the extraction code is in the same file, you might need to reorganize
    # For now, I'll create a simplified version that can be imported separately
    
    logger.info("Successfully imported Hilton scrapers")
except ImportError as e:
    logger.warning(f"Import warning: {e}")
    logger.warning("Some features may not be available")

# ------------------------------------------------------------------
# Request/Response Models
# ------------------------------------------------------------------

class ScrapeRequest(BaseModel):
    hotel_chain: str = "hilton"
    country_code: Optional[str] = None

class ScrapeResponse(BaseModel):
    status: str
    message: str
    session_id: Optional[str] = None

class HotelExtractionRequest(BaseModel):
    url: str
    save_to_db: bool = True
    extract_attributes: bool = True
    chain: Optional[str] = None  # Optional: expected chain for verification

class HotelExtractionResponse(BaseModel):
    status: str
    message: str
    session_id: str
    chain: Optional[str] = None
    data: Optional[dict] = None

# ------------------------------------------------------------------
# Helper Functions for Background Tasks
# ------------------------------------------------------------------

def run_scraper_task(hotel_chain: str, country_code: Optional[str], session_id: str):
    """Background task to run the location scraper"""
    try:
        logger.info(f"Starting scrape: chain={hotel_chain}, country={country_code}, session={session_id}")
        
        active_scrapes[session_id] = {
            'status': 'running',
            'started_at': datetime.now().isoformat(),
            'hotel_chain': hotel_chain,
            'country_code': country_code,
            'type': 'location_scrape'
        }
        
        if hotel_chain == "hilton":
            scraper = HiltonLocationsScraper()
            stats = scraper.scrape_all_locations(country_code_filter=country_code)
            
            active_scrapes[session_id].update({
                'status': 'completed',
                'completed_at': datetime.now().isoformat(),
                'stats': stats
            })
            
            logger.info(f"Scrape completed successfully: {stats}")
        else:
            raise ValueError(f"Unsupported hotel chain: {hotel_chain}")
            
    except Exception as e:
        logger.error(f"Error in scraper: {e}", exc_info=True)
        active_scrapes[session_id].update({
            'status': 'failed',
            'error': str(e),
            'completed_at': datetime.now().isoformat()
        })

def run_hotel_extraction_task(url: str, save_to_db: bool, extract_attributes: bool, chain: str, session_id: str):
    """Background task to run hotel extraction using modular components"""
    try:
        logger.info(f"Starting hotel extraction: {url}, session={session_id}")
        
        active_scrapes[session_id] = {
            'status': 'running',
            'started_at': datetime.now().isoformat(),
            'url': url,
            'save_to_db': save_to_db,
            'extract_attributes': extract_attributes,
            'type': 'hotel_extraction'
        }
        
        # Import modular components
        from context_extraction.hotel_extraction import HotelExtractionPipeline
        from scraping.hilton_scraper import HiltonScraper
        from llm.web_context_generator import WebContextGenerator
        from llm.pet_attribute_extractor import PetAttributeExtractor
        from utils.slug_generator import generate_combined_slug
        from utils.address_parser import parse_address
        from utils.context_hashing import generate_raw_content_hash
        
        if save_to_db:
            # Use the full pipeline with database saving
            pipeline = HotelExtractionPipeline()
            result = pipeline.extract_hotel(url, chain)
            
            active_scrapes[session_id].update({
                'status': 'completed',
                'completed_at': datetime.now().isoformat(),
                'result': result
            })
            
            logger.info(f"Hotel extraction completed and saved to database: {url}")
            
        else:
            # Run extraction without saving to DB
            # Use individual components
            scraper = HiltonScraper(headless=False)
            web_context_gen = WebContextGenerator()
            pet_attr_extractor = PetAttributeExtractor()
            
            # Step 1: Scrape data
            hotel_data = scraper.extract_all_data(url, chain)
            
            # Step 2: Generate web context
            web_context = web_context_gen.generate(hotel_data)
            
            # Step 3: Parse address
            address_info = parse_address(hotel_data.get('contact_info', {}).get('address', ''))
            
            # Step 4: Generate web slug
            web_slug = generate_combined_slug(
                country_code=address_info.get('country_code', 'US'),
                state_code=address_info.get('state', ''),
                city=address_info.get('city', ''),
                hotel_name=hotel_data.get('hotel_name', ''),
                address_line_1=address_info.get('address_line_1', '')
            )
            
            # Step 5: Extract pet attributes (if requested)
            pet_attributes = {}
            if extract_attributes:
                pet_attributes = pet_attr_extractor.extract(web_context)
            
            # Step 6: Generate hash
            hash_value = generate_raw_content_hash(hotel_data)
            
            # Prepare result
            result = {
                "status": "success",
                "hash": hash_value,
                "hotel_data": hotel_data,
                "address_info": address_info,
                "web_context": web_context,
                "pet_attributes": pet_attributes,
                "web_slug": web_slug,
                "url": url
            }
            
            active_scrapes[session_id].update({
                'status': 'completed',
                'completed_at': datetime.now().isoformat(),
                'result': result
            })
            
            logger.info(f"Hotel extraction completed (not saved to database): {url}")
            
    except Exception as e:
        logger.error(f"Error in hotel extraction: {e}", exc_info=True)
        active_scrapes[session_id].update({
            'status': 'failed',
            'error': str(e),
            'completed_at': datetime.now().isoformat()
        })

# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Hotel Scraper API",
        "version": "2.0.0",
        "endpoints": {
            "scrape_url": "POST /scrape_url - Start location scraping",
            "scrape_hotel": "POST /scrape_hotel - Extract hotel data from URL",
            "status": "GET /scrape/status/{session_id} - Check job status",
            "active": "GET /scrape/active - List active jobs",
            "health": "GET /health - Health check"
        }
    }

@app.post("/scrape_url", response_model=ScrapeResponse)
async def scrape_hotels(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Start hotel location scraping job
    
    Parameters:
    - hotel_chain: Hotel chain to scrape (default: hilton)
    - country_code: Optional country code filter (e.g., US, CA, MX, GB, etc.)
    
    Examples:
    - Scrape all Hilton: {"hotel_chain": "hilton"}
    - Scrape US only: {"hotel_chain": "hilton", "country_code": "US"}
    - Scrape Canada only: {"hotel_chain": "hilton", "country_code": "CA"}
    """
    try:
        # Generate session ID
        session_id = f"{request.hotel_chain}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Add scraping task to background
        background_tasks.add_task(
            run_scraper_task,
            request.hotel_chain,
            request.country_code,
            session_id
        )
        
        logger.info(f"Scrape job queued: {session_id}")
        
        return ScrapeResponse(
            status="started",
            message=f"Scraping started for {request.hotel_chain}" + 
                   (f" (country: {request.country_code})" if request.country_code else ""),
            session_id=session_id
        )
        
    except Exception as e:
        logger.error(f"Error starting scrape: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape_hotel", response_model=HotelExtractionResponse)
async def scrape_hotel_data(
    request: HotelExtractionRequest,
    background_tasks: BackgroundTasks,
    synchronous: bool = Query(False, description="Run synchronously (waits for result)")
):
    """
    Extract detailed hotel information from any supported chain URL
    
    Parameters:
    - url: The hotel URL to scrape
    - save_to_db: Whether to save results to database (default: True)
    - extract_attributes: Whether to extract pet attributes (default: True)
    - chain: Expected hotel chain (optional, for verification)
    - synchronous: If True, waits for completion and returns result (default: False)
    
    Returns:
    - If synchronous=False: Returns job ID for tracking
    - If synchronous=True: Returns the full extraction result
    """
    try:
        # Generate session ID
        session_id = f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Detect chain from URL
        detected_chain = HotelChainDetector.detect_chain_from_url(request.url)
        
        # Verify chain if expected chain provided
        if request.chain:
            chain_info = HotelChainDetector.verify_chain(request.url, request.chain)
            if not chain_info["is_verified"]:
                raise HTTPException(
                    status_code=400,
                    detail=chain_info["message"]
                )
            chain_to_use = request.chain
        else:
            chain_to_use = detected_chain or "unknown"
        
        logger.info(f"Processing {chain_to_use} hotel: {request.url}")
        
        if synchronous:
            # Run synchronously
            logger.info(f"Running synchronous extraction for {chain_to_use}: {request.url}")
            
            try:
                pipeline = HotelExtractionPipeline(headless=False)
                
                if request.save_to_db:
                    # Use the full pipeline with database saving
                    result = pipeline.extract_hotel(request.url, chain_to_use)
                    
                    return HotelExtractionResponse(
                        status="success",
                        message=f"{chain_to_use.capitalize()} hotel extraction completed",
                        data=result,
                        session_id=session_id,
                        chain=chain_to_use
                    )
                else:
                    # Run extraction without saving to DB
                    scraper = HotelScraperFactory.create_scraper(chain_to_use, headless=False)
                    web_context_gen = WebContextGenerator()
                    pet_attr_extractor = PetAttributeExtractor()
                    
                    # Step 1: Scrape data
                    hotel_data = scraper.extract_all_data(request.url)
                    
                    # Step 2: Generate web context
                    web_context = web_context_gen.generate(hotel_data)
                    
                    # Step 3: Parse address
                    address_info = parse_address(hotel_data.get('contact_info', {}).get('address', ''))
                    
                    # Step 4: Generate web slug
                    web_slug = generate_combined_slug(
                        country_code=address_info.get('country_code', 'US'),
                        state_code=address_info.get('state', ''),
                        city=address_info.get('city', ''),
                        hotel_name=hotel_data.get('hotel_name', ''),
                        address_line_1=address_info.get('address_line_1', ''),
                        chain=chain_to_use
                    )
                    
                    # Step 5: Extract pet attributes (if requested)
                    pet_attributes = {}
                    if request.extract_attributes:
                        pet_attributes = pet_attr_extractor.extract(web_context)
                    
                    # Step 6: Generate hash
                    hash_value = generate_raw_content_hash(hotel_data)
                    
                    # Prepare result
                    result = {
                        "status": "success",
                        "hash": hash_value,
                        "hotel_data": hotel_data,
                        "address_info": address_info,
                        "web_context": web_context,
                        "pet_attributes": pet_attributes,
                        "web_slug": web_slug,
                        "url": request.url,
                        "chain": chain_to_use
                    }
                    
                    return HotelExtractionResponse(
                        status="success",
                        message=f"{chain_to_use.capitalize()} hotel extraction completed (not saved to database)",
                        data=result,
                        session_id=session_id,
                        chain=chain_to_use
                    )
                    
            except ImportError as e:
                logger.error(f"Import error: {e}")
                raise HTTPException(
                    status_code=501, 
                    detail="Hotel extraction functionality not available."
                )
                
        else:
            # Run asynchronously
            logger.info(f"Queuing hotel extraction for {chain_to_use}: {request.url}")
            
            background_tasks.add_task(
                run_hotel_extraction_task,
                request.url,
                request.save_to_db,
                request.extract_attributes,
                chain_to_use,
                session_id
            )
            
            return HotelExtractionResponse(
                status="queued",
                message=f"Hotel extraction job queued for {chain_to_use}",
                session_id=session_id,
                chain=chain_to_use
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting hotel extraction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scrape/status/{session_id}")
async def get_scrape_status(session_id: str):
    """Get scraping job status"""
    if session_id not in active_scrapes:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    return active_scrapes[session_id]

@app.get("/scrape/active")
async def get_active_scrapes():
    """Get all active scraping sessions"""
    return {
        "active_sessions": len(active_scrapes),
        "sessions": active_scrapes
    }

@app.delete("/scrape/cleanup")
async def cleanup_old_sessions(hours_old: int = 24):
    """Clean up old completed/failed sessions"""
    try:
        cutoff = datetime.now().timestamp() - (hours_old * 3600)
        to_delete = []
        
        for session_id, session_data in active_scrapes.items():
            if 'completed_at' in session_data or 'failed_at' in session_data:
                # Check if session is old enough to delete
                timestamp_str = session_data.get('completed_at') or session_data.get('failed_at')
                if timestamp_str:
                    session_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp()
                    if session_time < cutoff:
                        to_delete.append(session_id)
        
        for session_id in to_delete:
            del active_scrapes[session_id]
        
        return {
            "status": "success",
            "message": f"Cleaned up {len(to_delete)} old sessions",
            "deleted_sessions": to_delete
        }
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": len(active_scrapes)
    }

# ------------------------------------------------------------------
# Direct extraction endpoint (for backward compatibility)
# ------------------------------------------------------------------

@app.post("/extract_hotel/direct")
async def extract_hotel_direct(request: HotelExtractionRequest):
    """
    Direct hotel extraction (synchronous - deprecated, use /scrape_hotel with synchronous=True)
    
    Use this for testing or when you need immediate results
    """
    try:
        logger.warning("/extract_hotel/direct is deprecated, use /scrape_hotel with synchronous=True")
        
        # Reuse the synchronous logic from scrape_hotel
        try:
            from url.hilton_location_scraper import HotelExtractionPipeline
            from url.hilton_location_scraper import parse_address, generate_combined_slug, _hash_context
            
            pipeline = HotelExtractionPipeline()
            
            if request.save_to_db:
                result = pipeline.extract_hotel(str(request.url))
                return {
                    "status": "success",
                    "message": "Hotel extraction completed and saved to database",
                    "data": result
                }
            else:
                hotel_data = pipeline.scraper.extract_all_data(str(request.url))
                prompt = pipeline.build_prompt(hotel_data)
                web_context = pipeline.generate_web_context(prompt)
                
                address_info = parse_address(hotel_data.get('contact_info', {}).get('address', ''))
                web_slug = generate_combined_slug(
                    country_code=address_info.get('country_code', 'US'),
                    state_code=address_info.get('state', ''),
                    city=address_info.get('city', ''),
                    hotel_name=hotel_data.get('hotel_name', ''),
                    address_line_1=address_info.get('address_line_1', '')
                )
                
                pet_attributes = {}
                if request.extract_attributes:
                    pet_attributes = pipeline.extract_pet_attributes(web_context)
                
                raw_content_parts = [
                    hotel_data.get('hotel_name', ''),
                    hotel_data.get('description', ''),
                    hotel_data.get('contact_info', {}).get('address', ''),
                    hotel_data.get('contact_info', {}).get('phone', ''),
                    ', '.join(hotel_data.get('amenities', [])),
                    str(hotel_data.get('parking_policy', {})),
                    str(hotel_data.get('pets_policy', {})),
                    hotel_data.get('smoking_policy', ''),
                    hotel_data.get('wifi_policy', ''),
                    hotel_data.get('rating', ''),
                ]
                raw_content = ' '.join(raw_content_parts)
                hash_value = _hash_context(raw_content)
                
                result = {
                    "status": "success",
                    "hash": hash_value,
                    "hotel_data": hotel_data,
                    "address_info": address_info,
                    "web_context": web_context,
                    "pet_attributes": pet_attributes,
                    "web_slug": web_slug,
                    "url": str(request.url)
                }
                
                return {
                    "status": "success",
                    "message": "Hotel extraction completed",
                    "data": result
                }
                
        except ImportError as e:
            logger.error(f"Import error: {e}")
            raise HTTPException(status_code=501, detail="Hotel extraction functionality not available")
            
    except Exception as e:
        logger.error(f"Error in direct extraction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000)