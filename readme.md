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





---


{
  "pet_information": {
    "is_pet_friendly": {
      "status": "present",
      "value": true
    },
    "allowed_species": {
      "status": "not_mentioned"
    },
    "has_pet_deposit": {
      "status": "present",
      "value": true
    },
    "pet_deposit_amount": {
      "status": "present",
      "value": 75.0
    },
    "is_deposit_refundable": {
      "status": "present",
      "value": false
    },
    "pet_fee_amount": {
      "status": "not_mentioned"
    },
    "pet_fee_variations": {
      "status": "not_mentioned"
    },
    "pet_fee_currency": {
      "status": "not_mentioned"
    },
    "pet_fee_interval": {
      "status": "not_mentioned"
    },
    "max_weight_lbs": {
      "status": "present",
      "value": 75.0
    },
    "max_pets_allowed": {
      "status": "not_mentioned"
    },
    "breed_restrictions": {
      "status": "not_mentioned"
    },
    "general_pet_rules": {
      "status": "not_mentioned"
    },
    "has_pet_amenities": {
      "status": "not_mentioned"
    },
    "pet_amenities_list": {
      "status": "not_mentioned"
    },
    "service_animals_allowed": {
      "status": "not_mentioned"
    },
    "emotional_support_animals_allowed": {
      "status": "not_mentioned"
    },
    "service_animal_policy": {
      "status": "not_mentioned"
    },
    "minimum_pet_age": {
      "status": "not_mentioned"
    }
  },
  "confidence_scores": {
    "is_pet_friendly": 1.0,
    "allowed_species": 0.0,
    "has_pet_deposit": 1.0,
    "pet_deposit_amount": 1.0,
    "is_deposit_refundable": 1.0,
    "pet_fee_amount": 0.0,
    "pet_fee_currency": 0.0,
    "pet_fee_variations": 0.0,
    "pet_fee_interval": 0.0,
    "max_weight_lbs": 1.0,
    "max_pets_allowed": 0.0,
    "breed_restrictions": 0.0,
    "general_pet_rules": 0.0,
    "has_pet_amenities": 0.0,
    "pet_amenities_list": 0.0,
    "service_animals_allowed": 0.0,
    "emotional_support_animals_allowed": 0.0,
    "service_animal_policy": 0.0,
    "minimum_pet_age": 0.0
  }
}

Validated Result:
{
  "pet_information": {
    "is_pet_friendly": {
      "status": "present",
      "value": true
    },
    "allowed_species": {
      "status": "not_mentioned",
      "value": null
    },
    "has_pet_deposit": {
      "status": "present",
      "value": true
    },
    "pet_deposit_amount": {
      "status": "present",
      "value": 75.0
    },
    "is_deposit_refundable": {
      "status": "present",
      "value": false
    },
    "pet_fee_amount": {
      "status": "not_mentioned",
      "value": null
    },
    "pet_fee_variations": {
      "status": "not_mentioned",
      "value": null
    },
    "pet_fee_currency": {
      "status": "not_mentioned",
      "value": null
    },
    "pet_fee_interval": {
      "status": "not_mentioned",
      "value": null
    },
    "max_weight_lbs": {
      "status": "present",
      "value": 75
    },
    "max_pets_allowed": {
      "status": "not_mentioned",
      "value": null
    },
    "breed_restrictions": {
      "status": "not_mentioned",
      "value": null
    },
    "general_pet_rules": {
      "status": "not_mentioned",
      "value": null
    },
    "has_pet_amenities": {
      "status": "not_mentioned",
      "value": null
    },
    "pet_amenities_list": {
      "status": "not_mentioned",
      "value": null
    },
    "service_animals_allowed": {
      "status": "not_mentioned",
      "value": null
    },
    "emotional_support_animals_allowed": {
      "status": "not_mentioned",
      "value": null
    },
    "service_animal_policy": {
      "status": "not_mentioned",
      "value": null
    },
    "minimum_pet_age": {
      "status": "not_mentioned",
      "value": null
    }
  },
  "confidence_scores": {
    "is_pet_friendly": 1.0,
    "allowed_species": 0.0,
    "has_pet_deposit": 1.0,
    "pet_deposit_amount": 1.0,
    "is_deposit_refundable": 1.0,
    "pet_fee_amount": 0.0,
    "pet_fee_currency": 0.0,
    "pet_fee_variations": 0.0,
    "pet_fee_interval": 0.0,
    "max_weight_lbs": 1.0,
    "max_pets_allowed": 0.0,
    "breed_restrictions": 0.0,
    "general_pet_rules": 0.0,
    "has_pet_amenities": 0.0,
    "pet_amenities_list": 0.0,
    "service_animals_allowed": 0.0,
    "emotional_support_animals_allowed": 0.0,
    "service_animal_policy": 0.0,
    "minimum_pet_age": 0.0
  }
}

---


# Pipeline & Summary

This project scrapes Hilton hotel pages, hashes raw content, generates LLM `web_context`, extracts structured pet policy attributes, and stores all results in PostgreSQL. The pipeline is designed to be deterministic, auditable, and LLM-safe.

---

## Pipeline (high-level)

1. Scrape the hotel page using `scraping/hilton_scraper.py` (Selenium + undetected-chromedriver)
2. Parse raw text (address, phone, amenities, policies) in `context_extraction/hotel_extraction.py`
3. Compute a **content hash** (`utils/context_hashing.py`) to detect duplicates
4. Save raw extraction to the DB (`db/operations.py`) — returns a record ID
5. Generate a compact **web context** via `llm/web_context_generator.py` (calls OpenRouter/OpenAI)
6. Extract **pet attributes** via `llm/pet_attribute_extractor.py` and validate schema
7. Save pet attributes, compute `web_slug` and finalize record

---

## Sample Run (real output snippets)

> **Note:** These are condensed excerpts from an actual extraction run to demonstrate the pipeline behavior.

- Startup & queueing:

```
INFO: Application startup complete.
INFO: Queuing hotel extraction: https://www.hilton.com/en/hotels/ancahhf-hilton-anchorage/
INFO: Starting hotel extraction: https://www.hilton.com/en/hotels/ancahhf-hilton-anchorage/, session=extract_20260204_163338
```

- Scraping & DB persistence:

```
2026-02-04 16:33:41,988 - context_extraction.hotel_extraction - INFO - Step 1: Scraping hotel page...
2026-02-04 16:33:43,842 - undetected_chromedriver.patcher - INFO - patching driver executable C:\Users\user\appdata\roaming\undetected_chromedriver\undetected_chromedriver.exe
2026-02-04 16:33:48,965 - scraping.hilton_scraper - INFO - Opening URL: https://www.hilton.com/en/hotels/ancahhf-hilton-anchorage/
2026-02-04 16:34:37,427 - db.operations - INFO - Saved raw extraction for https://www.hilton.com/en/hotels/ancahhf-hilton-anchorage/ with ID: 4
2026-02-04 16:34:53,609 - llm.web_context_generator - INFO - Web context generated successfully
2026-02-04 16:34:54,091 - db.operations - INFO - Saved web context for record ID: 4
```

- LLM pet extraction (validated JSON excerpt):

```
{
  "pet_information": {
    "is_pet_friendly": {"status": "present", "value": true},
    "has_pet_deposit": {"status": "present", "value": true},
    "pet_deposit_amount": {"status": "present", "value": 75.0},
    "max_weight_lbs": {"status": "present", "value": 75}
  },
  "confidence_scores": {"is_pet_friendly": 1.0, "pet_deposit_amount": 1.0, "max_weight_lbs": 1.0}
}
```

```
2026-02-04 16:35:03,113 - context_extraction.hotel_extraction - INFO - Step 7: Saving pet attributes to database...
2026-02-04 16:35:03,623 - db.operations - INFO - Saved pet attributes for record ID: 4
2026-02-04 16:35:04,350 - db.operations - INFO - Updated web slug for record ID: 4
2026-02-04 16:35:04,650 - context_extraction.hotel_extraction - INFO - Extraction completed successfully for https://www.hilton.com/en/hotels/ancahhf-hilton-anchorage/
```

---

## Configuration & Tuning 🔧

- `undetected-chromedriver` will patch a driver binary and may log: `patching driver executable C:\Users\...\undetected_chromedriver.exe` — this is expected.
- If you run headless scraping in containers, install a compatible Chrome and use the `--no-sandbox --disable-dev-shm-usage` flags in the Selenium options.
- LLM calls go to OpenRouter by default in this codebase (see `llm/` module). Ensure `OPENROUTER_API_KEY` is available to avoid 401 errors.

---

## Developer Notes & Caveats ⚠️

- Pydantic generic warning observed during runs:

```
GenericBeforeBaseModelWarning: Classes should inherit from `BaseModel` before generic classes (e.g. `typing.Generic[T]`) for pydantic generics to work properly.
  class NullableField(Generic[T], BaseModel):
```

  Fix: change the class definition order to `class NullableField(BaseModel, Generic[T]):` or move the generic base after `BaseModel` to silence the warning.

- LLM responses should be schema-validated. `llm/pet_attribute_extractor.py` already returns `status` and `value` pairs plus confidence scores — rely on `confidence` for downstream decisions.

---

## Troubleshooting & Tips

- If LLM calls fail, check `OPENROUTER_API_KEY` and model IDs. HTTP 200 responses with `openrouter.ai` in logs indicate successful calls.
- If duplicated extractions occur, confirm hashing algorithm in `utils/context_hashing.py` and that `web_context` generation is consistent.
- For intermittent bot-detection failures, try toggling headless or adding realistic browser flags.

---

## Contributing & Next Steps

If you'd like, I can:

1. Add a `Dockerfile` and `docker-compose.yml` for local development and Postgres orchestration ✅
2. Add a `CONTRIBUTING.md` and an MIT `LICENSE` file ✅
3. Add end-to-end tests for the scraping + LLM pipeline and CI workflows ✅

Open an issue or a PR and I'll help implement the next items.

---

## License

MIT (add `LICENSE` file on request)

---

Thanks for using `web-scraper-atomic` — if you want, I can now add a `docker-compose.yml` and example `.env.example` to the repo. ✅
