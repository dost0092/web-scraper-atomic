import time
import requests
import logging
from db.db_connection import DatabaseConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_hilton_hotels():
    try:
        # -------------------------
        # 1️⃣ Get DB connection
        # -------------------------
        db = DatabaseConnection()
        conn = db.get_connection()
        cursor = conn.cursor()

        # -------------------------
        # 2️⃣ Fetch Hilton URLs
        # -------------------------
        cursor.execute("""
            SELECT url
            FROM test.hotel_mapped_url
            WHERE chain = 'Hilton';
        """)
        urls = cursor.fetchall()

        logger.info(f"Total Hilton URLs found: {len(urls)}")

        # -------------------------
        # 3️⃣ API Endpoint
        # -------------------------
        api_url = "http://127.0.0.1:8000/scrape_hotel"

        # -------------------------
        # 4️⃣ Process one by one
        # -------------------------
        for index, (url,) in enumerate(urls, start=1):
            try:
                payload = {
                    "url": url,
                    "save_to_db": True,
                    "extract_attributes": True,
                    "chain": "Hilton"
                }

                logger.info(f"[{index}] Sending request for: {url}")

                response = requests.post(
                    api_url,
                    json=payload,
                    timeout=120
                )

                logger.info(
                    f"[{index}] Status: {response.status_code}"
                )

            except Exception as e:
                logger.error(f"[{index}] Error for {url}: {e}")

            # -------------------------
            # 5️⃣ Wait 5 seconds
            # -------------------------
            logger.info("Waiting 5 seconds...\n")
            time.sleep(5)

        cursor.close()
        logger.info("All Hilton URLs processed successfully.")

    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    scrape_hilton_hotels()
