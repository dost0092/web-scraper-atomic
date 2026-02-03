# 🏨 Hotel Scraper API

A powerful, production-ready API for scraping hotel locations and extracting detailed hotel information using **Selenium**, **FastAPI**, and **PostgreSQL**.

This project is **specifically optimized for Hilton hotels**, with advanced support for **pet policy extraction**, **LLM-powered attribute parsing**, and **content de-duplication**.

---

## ✨ Features

- 🔍 **Hotel Discovery**  
  Scrape Hilton hotel locations worldwide with optional country filtering

- 📊 **Rich Data Extraction**  
  Extract amenities, policies, addresses, and pet-friendly attributes

- 🤖 **Anti-Bot Protection**  
  Uses `undetected-chromedriver` to bypass bot detection

- 🗄️ **PostgreSQL Integration**  
  Structured storage with indexing, hashing, and JSONB attributes

- 🧬 **LLM Integration**  
  Web context generation and pet attribute extraction using OpenAI / Gemini

- ⚡ **FastAPI Backend**  
  High-performance async API with background job queueing

- 🔗 **Web Context Hashing**  
  Prevents duplicate scraping by detecting unchanged content

- 📦 **Docker Ready**  
  Fully containerized for easy deployment

---

## 🚀 Quick Start

### Prerequisites

- Python **3.9+**
- PostgreSQL **13+**
- Google Chrome (installed locally or in container)
- API keys (optional but recommended):
  - OpenRouter (OpenAI-compatible)
  - Google Gemini

---

## 📥 Installation

```bash
# Clone the repository
git clone <your-repo>
cd hotel-scraper-api

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt



# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password

# Browser Configuration
HEADLESS=true
RESTART_INTERVAL=100

# API / LLM Configuration
OPENAI_API_KEY=your_openrouter_key
MODEL_ID=openai/gpt-oss-120b

# Application Configuration
HOST=0.0.0.0
PORT=8000
DEBUG=false








{
  "status": "success",
  "message": "Hotel extraction completed",
  "data": {
    "hotel_name": "Hilton Phoenix Tapatio Cliffs Resort",
    "address": "11111 N 7th St, Phoenix, Arizona, 85020, USA",
    "amenities": ["Pool", "Spa", "Restaurant", "Fitness Center"],
    "pet_policy": {
      "is_pet_friendly": true,
      "pet_fee_amount": 75,
      "max_weight_lbs": 50,
      "pet_amenities": ["Pet Beds", "Pet Bowls"]
    },
    "web_slug": "us-az-phoenix-hilton-phoenix-tapatio-cliffs-resort-11111n7thst"
  },
  "session_id": "extract_20240115_103005"
}




# Hilton Hotel URL Scraping & Pet Policy Extraction Pipeline

This project scrapes Hilton hotel pages, securely hashes raw content, enriches data using Gemini LLMs, extracts structured pet policy attributes, and stores all results in PostgreSQL.

The pipeline is designed to be **deterministic, auditable, and LLM-safe**.

---

## High-Level Flow

1. Scrape Hilton hotel URL (already implemented via `HiltonScraper`)
2. Hash raw scraped content **before** sending anything to an LLM
3. Persist hash + core metadata to database
4. Send cleaned content to Gemini (`gemini-2.5-pro`) to generate `web_context`
5. Send `web_context` again to Gemini to extract structured pet policy JSON
6. Save `web_context` and `pet_attributes` JSON to DB
7. Generate and persist a deterministic `web_slug`

---

## Architecture Overview




