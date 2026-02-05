# Glamira Data Pipeline - Setup Guide

> **← Back to [README.md](README.md)**

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [MongoDB Setup on VM](#mongodb-setup-on-VM)
5. [IP Processor Setup](#ip-processor-setup)
6. [Product Crawler Setup](#product-crawler-setup)

---

## Project Overview

This data pipeline processes Glamira e-commerce analytics data:
- **IP Location Enrichment**: Extract unique IPs and enrich with geo-location data
- **Product Details Crawling**: Crawl product information (name, price, category, images, ratings)
- 
**Tech Stack:**
- MongoDB (source data)
- Python 3.10+
- Selenium (web scraping)
- IP2Location (geo-location)
- Google Cloud Platform (storage and VM)

---

## Architecture

```
          MongoDB (glamira.summary)
                      ↓
┌─────────────────────┬─────────────────────┐
│  ip_processor.py    │ product_crawler.py  │
│  - Extract IPs      │ - Extract products  │
│  - IP2Location      │ - Selenium crawl    │
│  - Geo enrichment   │ - Product details   │
└──────────┬──────────┴──────────┬──────────┘
           ↓                     ↓
    ip_locations.bson    product_details.bson
```

---

## Prerequisites

### 1. System Requirements
- **OS**: Ubuntu 22.04 LTS (Jammy Jellyfish) on Google Cloud
- **RAM**: 8GB+ (16GB recommended for large datasets)
- **Disk**: 50GB+ free space

### 2. Python Packages

#### Using Poetry
```bash
# Install Poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Navigate to project directory
cd /your-project-directory

# Install all dependencies from pyproject.toml
poetry install

# Activate virtual environment
poetry shell

# Verify installation
poetry show

```

## MongoDB Setup on VM

### 1. Install MongoDB (Ubuntu/Debian)
```bash
# Import MongoDB public GPG key
wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | sudo apt-key add -

# Add MongoDB repository
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# Install
sudo apt-get update
sudo apt-get install -y mongodb-org

# Start service
sudo systemctl start mongod
sudo systemctl enable mongod
```

### 2. Configure MongoDB Authentication
```bash
# Connect to MongoDB
mongosh

# Create admin user
use admin
db.createUser({
  user: "admin",
  pwd: "<STRONG_PASSWORD>",
  roles: ["root"]
})

# Enable authentication
# Edit /etc/mongod.conf:
security:
  authorization: enabled
  
# Restart MongoDB
sudo systemctl restart mongod
```

### 3. Create Application User
```bash
# Connect as admin
mongosh -u admin -p --authenticationDatabase admin

# Create glamira_user
use glamira
db.createUser({
  user: "glamira_user",
  pwd: "Q1234qaz",  # Change later
  roles: [
    { role: "readWrite", db: "glamira" },
    { role: "dbAdmin", db: "glamira" }
  ]
})
```

### 4. Import Data
```bash
# Import summary collection from dump
mongorestore \
  --uri='mongodb://glamira_user:Q1234qaz@localhost:27017/glamira?authSource=glamira' \
  --collection=summary \
  --db=glamira \
  dump/countly/summary.bson

# Verify
mongosh -u glamira_user -p Q1234qaz --authenticationDatabase glamira
use glamira
db.summary.countDocuments()
db.summary.findOne()
```

---

## IP Processor Setup

### 1. Configuration
Edit `ip_processor.py`:
```python
# MongoDB connection
MONGODB_URI = 'mongodb://glamira_user:PASSWORD@localhost:27017/glamira?authSource=glamira'
MONGODB_DATABASE = 'glamira'
MONGODB_COLLECTION = 'summary'

# IP2Location database path
IP2LOCATION_DB = './IP-COUNTRY-REGION-CITY.BIN'

# Output files
OUTPUT_UNIQUE_IPS = './output/unique_ips.txt'
OUTPUT_BSON = './output/ip_locations.bson'
```

### 2. Run IP Processor
```bash
# Using Poetry
poetry run python3 ip_processor.py

# OR using python3 directly (if in venv)
python3 ip_processor.py

# Full run (use nohup for long-running)
nohup poetry run python3 ip_processor.py > ip_processor.log 2>&1 &

# Monitor progress
tail -f logs/ip_processor.log
```

### 4. Expected Output
```
./output/
  ├── unique_ips.txt         # List of unique IPs
  ├── ip_locations.bson      # Enriched IP data
  └── ip_processing_state.json  # Resume state
./logs/
  └── ip_processor.log       # Processing log
```

**Data Schema:**
```json
{
  "ip": "37.170.17.183",
  "country_short": "FR",
  "country_long": "France",
  "region": "Île-de-France",
  "city": "Paris",
  "processed_at": "2025-10-16T..."
}
```

---

## Product Crawler Setup

### 1. Install Google Chrome (Linux)
```bash
# Add Google Chrome repository
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'

# Install
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# Verify
google-chrome --version
```

### 2. Configuration
Edit `product_crawler.py`:
```python
# MongoDB connection
MONGODB_URI = 'mongodb://glamira_user:PASSWORD@localhost:27017/glamira?authSource=glamira'
MONGODB_DATABASE = 'glamira'
MONGODB_COLLECTION = 'summary'

# Event types to process
PRODUCT_EVENT_TYPES = [
    'view_product_detail',
    'select_product_option',
    'add_to_cart_action',
    # ... etc
]

# Crawling settings
BATCH_SIZE = 50               # Products per batch
PAGE_LOAD_TIMEOUT = 30        # Seconds
DELAY_BETWEEN_PRODUCTS = 2    # Seconds
```

### 3. Run Product Crawler
```bash
# Using Poetry
poetry run python3 product_crawler.py

# OR using python3 directly (if in venv)
python3 product_crawler.py

# Full run (long-running)
nohup poetry run python3 product_crawler.py > product_crawler.log 2>&1 &

# Monitor progress
tail -f logs/product_crawler.log
```

### 4. Expected Output
```
./output/
  ├── unique_product_ids.json      # Product IDs extracted
  ├── product_details.bson         # Crawled product data
  └── product_crawl_state.json     # Resume state
./logs/
  └── product_crawler.log          # Crawling log
```

**Data Schema:**
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
  "crawled_at": "2025-10-16T..."
}
```




