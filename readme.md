# Hilton Hotel Locations Scraper

A production-ready web scraper for extracting Hilton hotel locations using FastAPI and Selenium.

## 📁 Project Structure

```
hilton_scraper_v2/
├── main.py                          # FastAPI endpoints (GET request handler)
├── hilton_locations_scraper.py      # Complete scraping logic
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variables template
└── README.md                        # This file
```

## ✨ Features

- ✅ Single GET endpoint for scraping
- ✅ Country-based filtering (scrape only US, CA, MX, etc.)
- ✅ Headless Selenium with anti-bot detection
- ✅ Automatic browser restart every 100 hotels
- ✅ Retry mechanism on failures
- ✅ Saves every hotel immediately to PostgreSQL
- ✅ Comprehensive logging (file + console)
- ✅ Modular class-based design

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Setup Database

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS test;

-- Countries reference table
CREATE TABLE IF NOT EXISTS test.countries (
    id SERIAL PRIMARY KEY,
    country VARCHAR(200) UNIQUE,
    country_code VARCHAR(10)
);

-- Insert countries
INSERT INTO test.countries (country, country_code) VALUES
('United Kingdom', 'GB'),
('Germany', 'DE'),
('France', 'FR'),
('Spain', 'ES'),
('Italy', 'IT'),
('Australia', 'AU')
ON CONFLICT DO NOTHING;

-- Hotels table
CREATE TABLE IF NOT EXISTS test.web_scraped_hotels (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500),
    url TEXT UNIQUE,
    city VARCHAR(200),
    state VARCHAR(100),
    country VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### 4. Run the API

```bash
python main.py
```

API runs at: `http://localhost:8000`

## 📖 API Usage

### Endpoint: `POST /scrape`

Start a scraping job.

**Request Body:**
```json
{
  "hotel_chain": "hilton",
  "country_code": "US"
}
```

**Parameters:**
- `hotel_chain` (string): Always "hilton" for now
- `country_code` (string, optional): Filter by country (US, CA, MX, GB, etc.)

### Examples

#### Scrape ALL Hilton Hotels

```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{"hotel_chain": "hilton"}'
```

#### Scrape US ONLY

```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{"hotel_chain": "hilton", "country_code": "US"}'
```

#### Scrape Canada ONLY

```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{"hotel_chain": "hilton", "country_code": "CA"}'
```

#### Scrape Mexico ONLY

```bash
curl -X POST "http://localhost:8000/scrape" \
  -H "Content-Type: application/json" \
  -d '{"hotel_chain": "hilton", "country_code": "MX"}'
```

**Response:**
```json
{
  "status": "started",
  "message": "Scraping started for hilton (country: US)",
  "session_id": "hilton_20260201_143052"
}
```

### Check Status: `GET /scrape/status/{session_id}`

```bash
curl "http://localhost:8000/scrape/status/hilton_20260201_143052"
```

**Response:**
```json
{
  "status": "running",
  "started_at": "2026-02-01T14:30:52",
  "hotel_chain": "hilton",
  "country_code": "US",
  "stats": {
    "total": 1250,
    "success": 1240,
    "failed": 10
  }
}
```

### List Active Scrapes: `GET /scrape/active`

```bash
curl "http://localhost:8000/scrape/active"
```

### Health Check: `GET /health`

```bash
curl "http://localhost:8000/health"
```

## 🔄 How It Works

### Scraping Flow

1. **Navigate** to `https://www.hilton.com/en/locations/`
2. **Click** "Hotel Locations by Region" accordion
3. **Iterate** through all region tabs (North America, Central America, Europe, etc.)
4. **Expand** each country accordion within regions
5. **Extract** all state/province links
6. **Visit** each state page
7. **Click** "Pet-Friendly" filter (if available)
8. **Scrape** hotel cards (name + URL)
9. **Paginate** through all pages (click "Next" button)
10. **Save** each hotel immediately to database
11. **Restart** browser every 100 hotels to prevent crashes

### Data Saved

For each hotel:

**North America (US, CA, MX):**
- `name` - Hotel name
- `url` - Hotel URL
- `country` - Country code (US/CA/MX)
- `state` - State/province name
- `city` - Empty for now

**Other Regions:**
- `name` - Hotel name
- `url` - Hotel URL
- `country` - Country code from `test.countries` table
- `state` - NULL
- `city` - Empty for now

### Country Code Logic

**North America** - Hardcoded mapping:
- United States of America → `US`
- Canada → `CA`
- Mexico → `MX`

**Other Regions** - Query from database:
```sql
SELECT country_code FROM test.countries WHERE country = 'United Kingdom'
```

## ⚙️ Configuration

### Environment Variables (.env)

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password

# Scraper
HEADLESS=true
RESTART_INTERVAL=100
```

### Anti-Bot Features

- Random user agent rotation
- Random delays (2-4 seconds between pages)
- Stealth mode (removes WebDriver detection)
- Human-like scrolling
- Browser fingerprint randomization

## 📊 Logging

Logs are saved to both console and `scraper.log`:

```
2026-02-01 14:30:22 - hilton_locations_scraper - INFO - Starting Hilton scrape - Filter: US
2026-02-01 14:30:25 - hilton_locations_scraper - INFO - Found 11 regions
2026-02-01 14:30:28 - hilton_locations_scraper - INFO - Processing region 1/11: North America
2026-02-01 14:30:30 - hilton_locations_scraper - INFO - Found 3 countries in North America
2026-02-01 14:30:32 - hilton_locations_scraper - INFO - Processing country: United States of America
2026-02-01 14:30:35 - hilton_locations_scraper - INFO - Found 52 locations in United States of America
2026-02-01 14:30:38 - hilton_locations_scraper - INFO - Processing: Alabama
2026-02-01 14:30:42 - hilton_locations_scraper - INFO - Scraping page 1 of Alabama
2026-02-01 14:30:45 - hilton_locations_scraper - INFO - Found 20 hotels on page
```

## 🔍 Troubleshooting

### Chrome/ChromeDriver Issues

```bash
# Install Chrome on Ubuntu
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f
```

ChromeDriver is auto-installed by `webdriver-manager`.

### Database Connection

```bash
# Test connection
psql -U your_user -d your_database -c "SELECT 1"
```

### Browser Crashes

Reduce `RESTART_INTERVAL` in `.env`:
```env
RESTART_INTERVAL=50
```

### View Logs

```bash
tail -f scraper.log
```

## 📈 Performance

- **Speed**: ~20-30 hotels per minute
- **Memory**: Restarts browser every 100 hotels to prevent memory leaks
- **Reliability**: Retries failed requests up to 3 times
- **Scalability**: Can run multiple instances with different country filters

## 🛠 Extending to Other Chains

To add Hyatt, Marriott, etc.:

1. Create `hyatt_locations_scraper.py` following same pattern
2. Update `main.py`:

```python
if hotel_chain == "hilton":
    scraper = HiltonLocationsScraper()
elif hotel_chain == "hyatt":
    scraper = HyattLocationsScraper()
```

## 📝 Database Queries

```sql
-- Total hotels
SELECT COUNT(*) FROM test.web_scraped_hotels;

-- Hotels by country
SELECT country, COUNT(*) 
FROM test.web_scraped_hotels 
GROUP BY country 
ORDER BY COUNT(*) DESC;

-- Hotels by US state
SELECT state, COUNT(*) 
FROM test.web_scraped_hotels 
WHERE country = 'US' 
GROUP BY state 
ORDER BY COUNT(*) DESC;

-- Recent scrapes
SELECT * FROM test.web_scraped_hotels 
WHERE DATE(created_at) = CURRENT_DATE;
```

## 🎯 Key Features Explained

### ✅ Saves Every Hotel Immediately

Hotels are saved to database right after scraping, not in batch at the end.

### ✅ Browser Restart Every 100 Hotels

Prevents memory leaks and crashes during long scraping sessions.

### ✅ Country Filtering

```python
# Scrape only US
scraper.scrape_all_locations(country_code_filter="US")

# Scrape only UK
scraper.scrape_all_locations(country_code_filter="GB")
```

### ✅ State-Based for North America

For US, CA, MX - saves both `country` AND `state` fields.

For other countries - saves only `country` field, `state` is NULL.

## 🔐 Security

- Database credentials in `.env` (never commit!)
- SQL injection protected (parameterized queries)
- No hardcoded passwords

## 📞 Support

Check logs in `scraper.log` for detailed error messages.

## 📄 License

MIT License - Free to use and modify