"""
FIXED SeleniumBase script for Hyatt pet policy - using correct methods
"""

from seleniumbase import SB
import time

def scrape_hyatt_pet_policy():
    print("=" * 70)
    print("🚀 SELENIUMBASE HYATT PET POLICY EXTRACTOR")
    print("=" * 70)
    
    with SB(
        browser="chrome",
        headless=True,
        undetectable=True,
        headless2=True,
        incognito=True,
        agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        uc=True,
        disable_csp=True,
        block_images=False,
    ) as sb:
        
        try:
            url = "https://www.hyatt.com/hyatt-place/en-US/ontza-hyatt-place-ontario-airport"
            print(f"\n🌐 Navigating to: {url}")
            sb.open(url)
            
            print("⏳ Waiting for page load (15 seconds)...")
            time.sleep(15) 
            
            print("🖱️ Scrolling to load content...")
            sb.execute_script("window.scrollTo(0, 1000);")
            time.sleep(3)
            
            
            # --- START OF CORRECTED SECTION ---
            print("\n🔍 Looking for pet policy section...")
            print("Trying data-locator='pets-overview-text'...")
            
            try:
                # Use wait_for_element instead of is_element_present for timeouts
                if sb.wait_for_element('[data-locator="pets-overview-text"]', timeout=15):
                    print("✅ FOUND using data-locator!")
                    
                    full_html = sb.get_attribute('[data-locator="pets-overview-text"]', "outerHTML")
                    full_text = sb.get_text('[data-locator="pets-overview-text"]')
                    
                    print("\n" + "=" * 70)
                    print("🐾 PET POLICY FOUND:")
                    print("=" * 70)
                    print(full_text)
                    
                    with open("pet_policy_full.html", "w", encoding="utf-8") as f:
                        f.write(full_html)
                    with open("pet_policy_full.txt", "w", encoding="utf-8") as f:
                        f.write(full_text)
                    
                    print("\n📋 EXTRACTING SPECIFIC SECTIONS...")
                    
                    # Corrected "Pets Are Welcome" extraction
                    try:
                        # Find the first child div inside the locator which contains the text
                        welcome_text = sb.get_text('[data-locator="pets-overview-text"] div:first-of-type')
                        print("\n1. PETS WELCOME SECTION:")
                        print("-" * 40)
                        print(welcome_text)
                        with open("pets_welcome.txt", "w", encoding="utf-8") as f:
                            f.write(welcome_text)
                    except:
                        print("Could not extract welcome section separately")
                    
                    # Extract the "Pet Fees" div
                    try:
                        if sb.is_element_present('[data-locator="pet-policy-fees"]'):
                            fees_text = sb.get_text('[data-locator="pet-policy-fees"]')
                            print("\n2. PET FEES SECTION:")
                            print("-" * 40)
                            print(fees_text)
                            with open("pet_fees.txt", "w", encoding="utf-8") as f:
                                f.write(fees_text)
                            
                            # Extract individual fee items
                            fee_items = sb.find_elements('[data-locator="pet-policy-fees-item"]')
                            print("\n3. INDIVIDUAL FEE ITEMS:")
                            for i, item in enumerate(fee_items, 1):
                                item_text = item.text
                                print(f"   Item {i}: {item_text}")
                                with open(f"fee_item_{i}.txt", "w", encoding="utf-8") as f:
                                    f.write(item_text)
                    except:
                        print("Could not extract fees section separately")
                    
                    return # Exit if successful
            except Exception as e:
                print(f"❌ Data-locator method failed: {e}")
            
            # STRATEGY 2: Try your XPaths
            print("\nTrying XPaths...")
            xpaths = [
                "//*[@id='__next']/main/div[12]/div/div/div[1]",
                "/html/body/div[1]/main/div[12]/div/div/div[1]",
                "//*[contains(text(), 'Pets Are Welcome')]/ancestor::div[contains(@data-locator, 'pets')]"
            ]
            
            for xpath in xpaths:
                try:
                    if sb.is_element_present(xpath, timeout=5):
                        element = sb.find_element(xpath)
                        text = element.text
                        print(f"✅ Found with XPath: {xpath[:50]}...")
                        print("\n" + "=" * 70)
                        print("EXTRACTED TEXT:")
                        print("=" * 70)
                        print(text)
                        
                        with open(f"pet_policy_xpath.txt", "w", encoding="utf-8") as f:
                            f.write(text)
                        break
                except:
                    continue
            
            # STRATEGY 3: Search in page source
            print("\nSearching in page source...")
            page_source = sb.driver.page_source  # Direct Selenium access
            
            if 'Pets Are Welcome' in page_source:
                print("Found 'Pets Are Welcome' in page source")
                
                # Extract the relevant HTML section
                start = page_source.find('data-locator="pets-overview-text"')
                if start != -1:
                    # Find the closing div for this section
                    html_slice = page_source[start:start+3000]  # Take 3000 chars
                    # Find a reasonable closing point
                    closing_div = html_slice.find('</div></div></div>')
                    if closing_div != -1:
                        pet_html = html_slice[:closing_div+18]  # +18 for the closing tags
                        
                        # Save the HTML
                        with open("pet_policy_from_source.html", "w", encoding="utf-8") as f:
                            f.write(pet_html)
                        
                        # Extract text (crude method)
                        import re
                        clean_text = re.sub('<[^>]+>', ' ', pet_html)
                        clean_text = ' '.join(clean_text.split())
                        
                        print("\nEXTRACTED FROM SOURCE:")
                        print("-" * 40)
                        print(clean_text[:500] + "..." if len(clean_text) > 500 else clean_text)
            
            print("\n" + "=" * 70)
            print("✅ EXTRACTION ATTEMPT COMPLETE")
            print("=" * 70)
            
            # List all saved files
            import os
            print("\n📁 SAVED FILES:")
            for file in ["pet_policy_full.html", "pet_policy_full.txt", 
                        "pets_welcome.txt", "pet_fees.txt"]:
                if os.path.exists(file):
                    print(f"  ✓ {file}")
            
        except Exception as e:
            print(f"\n❌ Main error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    scrape_hyatt_pet_policy()