"""
hotel_extraction/utils/chain_detector.py
"""
import re
from typing import Optional, Dict,Any

class HotelChainDetector:
    """Detect hotel chain from URL and content"""
    
    # Chain patterns
    CHAIN_PATTERNS = {
        "hilton": {
            "url_patterns": [r"hilton\.com", r"hamptoninn\.com", r"doubletree\.com"],
            "name_patterns": [r"Hilton", r"DoubleTree", r"Hampton", r"Conrad", r"Waldorf"]
        },
        "hyatt": {
            "url_patterns": [r"hyatt\.com", r"parkhyatt\.com", r"grandhyatt\.com"],
            "name_patterns": [r"Hyatt", r"Park Hyatt", r"Grand Hyatt", r"Andaz"]
        },
        "marriott": {
            "url_patterns": [r"marriott\.com", r"riott\.com", r"westin\.com", r"sheraton\.com"],
            "name_patterns": [r"Marriott", r"Westin", r"Sheraton", r"Ritz-Carlton"]
        },
        "ihg": {
            "url_patterns": [r"ihg\.com", r"intercontinental\.com", r"holidayinn\.com"],
            "name_patterns": [r"InterContinental", r"Holiday Inn", r"Crowne Plaza"]
        }
    }
    
    @staticmethod
    def detect_chain_from_url(url: str) -> Optional[str]:
        """Detect hotel chain from URL"""
        url_lower = url.lower()
        
        for chain, patterns in HotelChainDetector.CHAIN_PATTERNS.items():
            for pattern in patterns["url_patterns"]:
                if re.search(pattern, url_lower):
                    return chain
        return None
    
    @staticmethod
    def detect_chain_from_name(name: str) -> Optional[str]:
        """Detect hotel chain from hotel name"""
        for chain, patterns in HotelChainDetector.CHAIN_PATTERNS.items():
            for pattern in patterns["name_patterns"]:
                if re.search(pattern, name, re.IGNORECASE):
                    return chain
        return None
    
    @staticmethod
    def verify_chain(url: str, expected_chain: Optional[str] = None) -> Dict[str, Any]:
        """
        Verify if URL matches expected chain
        
        Returns:
            Dict with chain info and verification status
        """
        detected_chain = HotelChainDetector.detect_chain_from_url(url)
        
        result = {
            "detected_chain": detected_chain,
            "expected_chain": expected_chain,
            "is_verified": True,
            "message": ""
        }
        
        if expected_chain:
            if detected_chain and detected_chain.lower() != expected_chain.lower():
                result["is_verified"] = False
                result["message"] = f"URL chain mismatch. Expected: {expected_chain}, Detected: {detected_chain}"
            elif not detected_chain:
                result["is_verified"] = False
                result["message"] = f"Could not detect chain from URL. Expected: {expected_chain}"
        else:
            result["message"] = f"Detected chain: {detected_chain or 'Unknown'}"
        
        return result