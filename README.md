## Project Structure

```
Project/
├── README.md                      
├── SETUP_GUIDE.md                 # Complete setup instructions
│
├── ip_processor.py                # IP processor
├── product_crawler.py             # Product details crawler 
│
├── IP-COUNTRY-REGION-CITY.BIN     # IP2Location database
│
├── output/                        # Processing output
│   ├── unique_ips.txt
│   ├── ip_locations.bson
│   ├── unique_product_ids.json
│   ├── product_details.bson
│
├── logs/                          # Processing logs
│   ├── ip_processor.log
│   └── product_crawler.log
│ 
├── summary.bson                   # MongoDB files
└── summary.metadata.json
```

---

## Quick Start

Use this command to download full data for output and summary.bson and IP-COUNTRY-REGION-CITY.BIN files (since cannot upload large files to Github):
```
pip install gsutil #if did not install before
gsutil -m cp -r gs://raw_glamira_data .
```
**[SETUP_GUIDE.md](SETUP_GUIDE.md)**

---

## Data Schemas

### IP Locations
```json
{
  "ip": "37.170.17.183",
  "country_short": "FR",
  "country_long": "France",
  "region": "Île-de-France",
  "city": "Paris",
  "processed_at": "2025-10-16T12:34:56"
}
```

### Product Details
```json
{
  "product_id": "110474",
  "product_name": "Damen halskette Kinsey",
  "price": 5165.00,
  "price_raw": "5.165,00 €",
  "currency": "EUR",
  "category": "Halsketten",
  "category_path": "Startseite > Schmuck > Damenschmuck > ...",
  "image_url": "https://cdn-media.glamira.com/...",
  "description": "...",
  "rating": 4.8,
  "rating_raw": "4.8 Sterne",
  "url": "https://www.glamira.de/glamira-halskette-kinsey.html",
  "domain": "glamira.de",
  "crawled_at": "2025-10-16T12:34:56"
}
```

---

## Scripts Overview

### `ip_processor.py`
**Purpose**: Extract unique IPs from MongoDB and enrich with geo-location data

**Features:**
- Extracts unique IPs using aggregation pipeline
- IP2Location lookup for country/region/city
- Resume functionality for interrupted runs
- Batch processing with progress tracking
- BSON output format

**Usage:**
```bash
poetry run python3 ip_processor.py
```

**Output:** `./output/ip_locations.bson`

---

### `product_crawler.py`
**Purpose**: Crawl product details from Glamira websites using Selenium

**Features:**
- Extracts products from multiple event types
- Selenium-based web scraping (bypasses bot protection)
- Crawls: name, price, currency, category, images, ratings
- Resume functionality
- Retry mechanism for failed products
- Batch processing with delays
- BSON output format

**Usage:**
```bash
poetry run python3 product_crawler.py
```

**Output:** `./output/product_details.bson`

**Configuration:**
```python
BATCH_SIZE = 50               # Products per batch
PAGE_LOAD_TIMEOUT = 30        # Seconds
DELAY_BETWEEN_PRODUCTS = 2    # Seconds
```

---

## Performance

### IP Processor: 3,239,628 IPs
- **Speed**: 0.0005-0.0007 s per IP
- **Throughput**: 81,000-108,000 IPs/minute
- **Batch Size**: 1000 IPs
- **Expected Time**: 30-40 minute

### Product Crawler: 19,417 products
- **Speed**: 5-8s per product
- **Throughput**: 7-12 products/minute
- **Batch Size**: 50 products
- **Expected Time**: 17K products = 23-41 hours

---

## Troubleshooting

### MongoDB Connection Issues
```bash
# Check MongoDB status
sudo systemctl status mongod

# Check connection
mongosh -u glamira_user -p --authenticationDatabase glamira
```

### Selenium Timeout Errors
- Increase `PAGE_LOAD_TIMEOUT` to 45-60s
- Increase `DELAY_BETWEEN_PRODUCTS` to 3-5s
- Check VM resources (CPU/RAM)

### Out of Memory
- Reduce `BATCH_SIZE`
- Close other applications
- Upgrade VM instance

### Resume After Crash
Scripts automatically save progress. Just re-run the same command:
```bash
# Resume automatically from last checkpoint
python3 product_crawler.py
```

---

## Security Notes

1. **MongoDB Authentication**: Enabled
2. **Network Binding**: localhost only (`bindIp: 127.0.0.1`)
3. **Firewall**: Block external MongoDB access
4. **Credentials**: Change default passwords in production!

---

## Workflow Summary

```
1. MongoDB Setup
   └─> Import summary.bson
   
2. IP Processing
   └─> Extract IPs → IP2Location → ip_locations.bson
   
3. Product Crawling
   └─> Extract product_ids → Selenium crawl → product_details.bson
   
4. Quality Check
   └─> verify_data_quality.py
```

---

## Support

For issues or questions:
1. Check logs: `./logs/*.log`
2. Review resume state: `./output/*_state.json`
3. Check MongoDB: `db.summary.findOne({collection: 'view_product_detail'})`
