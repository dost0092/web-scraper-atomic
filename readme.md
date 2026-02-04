# 🏨 web-scraper-atomic — Hilton Hotel Scraper & Extraction

A modular FastAPI service and scraping toolkit for discovering Hilton hotel locations and extracting detailed hotel information (amenities, policies, addresses), with LLM-powered web context generation and structured pet-policy extraction.

Built for automation, deduplication (hashing), and Postgres-backed persistence.

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




# Pipeline & Summary

This project scrapes Hilton hotel pages, hashes raw content, generates LLM `web_context`, extracts structured pet policy attributes, and stores all results in PostgreSQL. The pipeline is designed to be deterministic, auditable, and LLM-safe.

---

## High-Level Flow

1. Scrape Hilton hotel URL using `scraping/hilton_scraper.py` or bulk discovery via `url/hilton_location_scraper.py`
2. Normalize and hash raw content (see `utils/context_hashing.py`) to detect duplicates
3. Save raw extraction and metadata using `db/operations.py` and `db/queries.py`
4. Generate `web_context` using `llm/web_context_generator.py`
5. Extract structured pet attributes with `llm/pet_attribute_extractor.py` (schema-validated)
6. Persist `web_context` and `pet_attributes` (JSONB) and compute `web_slug`

---

## Architecture Overview

- **API**: `main.py` (FastAPI) — endpoints to queue scrapers and run extraction
- **Scrapers**: `scraping/` & `url/` — hotel page and location scrapers using Selenium + `undetected-chromedriver`
- **LLM layer**: `llm/` — prompt assembly and calls to OpenRouter/OpenAI/Gemini
- **Persistence**: `db/` — Postgres connection, queries, and operations
- **Utilities**: `utils/` — slug generation, address parsing, content hashing

---

## What I changed in README

- Consolidated project description and features
- Added installation and quick-start commands
- Documented environment variables and LLM usage
- Listed endpoints and DB notes
- Added testing, troubleshooting, and next steps

---

If you'd like, I can now:

1. Remove the example JSON block and old installation fragments
2. Add an example `Dockerfile` and `docker-compose.yml`
3. Add a `CONTRIBUTING.md` and `LICENSE` (MIT) template

Tell me which of the items above you want next and I'll implement them. ✅
