"""
Glamira Product Crawler with Selenium
Crawls: product_name, price, currency, category, image_url, rating, url, domain
"""

import logging
import sys
import json
import time
import bson
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from pymongo import MongoClient


# CONFIGURATION

MONGODB_URI = 'mongodb://glamira_user:Q1234qaz@localhost:27017/glamira?authSource=glamira'
MONGODB_DATABASE = 'glamira'
MONGODB_COLLECTION = 'summary'

PRODUCT_EVENT_TYPES = [
    'view_product_detail',
    'select_product_option',
    'select_product_option_quality',
    'add_to_cart_action',
    'product_detail_recommendation_visible',
    'product_detail_recommendation_noticed',
    'product_view_all_recommend_clicked'
]

# Output files
OUTPUT_PRODUCT_IDS = './output/unique_product_ids.json'
OUTPUT_BSON = './output/product_details.bson'
OUTPUT_JSON = './output/product_details.json'
OUTPUT_RESUME = './output/product_crawl_state.json'
LOG_FILE = './logs/product_crawler.log'

# Crawling configuration
BATCH_SIZE = 50  # Smaller batch for Selenium
PAGE_LOAD_TIMEOUT = 30  # Increased timeout
ELEMENT_WAIT_TIMEOUT = 20  # Wait for elements
DELAY_BETWEEN_PRODUCTS = 2  # Seconds between products
DELAY_BETWEEN_BATCHES = 5
MAX_RETRIES_PER_PRODUCT = 2  # Retry failed products


# LOGGING
def setup_logging():
    log_dir = Path(LOG_FILE).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


# SELENIUM SETUP
def setup_driver(headless=True):
    """Setup Chrome driver with anti-detection and performance settings"""
    chrome_options = Options()
    
    # Anti-detection
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User agent
    chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Headless mode
    if headless:
        chrome_options.add_argument('--headless=new')
    
    # Performance optimizations
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    
    # Speed up page load by disabling images (optional - comment out if needed)
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # chrome_options.add_experimental_option("prefs", prefs)
    
    # Memory optimization
    chrome_options.add_argument('--disable-background-networking')
    chrome_options.add_argument('--disable-default-apps')
    chrome_options.add_argument('--disable-sync')
    
    # Initialize driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    
    # Hide webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver


# MONGODB FUNCTIONS
def connect_mongodb():
    """Connect to MongoDB"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DATABASE]
        logging.info(f"Connected to MongoDB: {db.name}")
        return client, db
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None, None


def extract_product_ids_from_mongodb(db):
    """Extract unique product_ids with URLs and domains from MongoDB"""
    
    logging.info("Extracting product data from summary collection...")
    
    collection = db[MONGODB_COLLECTION]
    product_data = {}
    
    for event_type in PRODUCT_EVENT_TYPES:
        try:
            logging.info(f"Processing event type: {event_type}")
            
            # Query documents with this event type
            query = {'collection': event_type}
            
            # Handle product_view_all_recommend_clicked differently
            if event_type == 'product_view_all_recommend_clicked':
                docs = collection.find(query, {'viewing_product_id': 1, 'referrer_url': 1})
                for doc in docs:
                    product_id = doc.get('viewing_product_id')
                    url = doc.get('referrer_url')
                    
                    if product_id:
                        if product_id not in product_data:
                            product_data[product_id] = {'product_id': product_id, 'urls': set()}
                        if url:
                            product_data[product_id]['urls'].add(url)
            else:
                # Other event types use product_id and current_url
                docs = collection.find(query, {
                    'product_id': 1, 
                    'viewing_product_id': 1, 
                    'current_url': 1
                })
                
                for doc in docs:
                    product_id = doc.get('product_id') or doc.get('viewing_product_id')
                    url = doc.get('current_url')
                    
                    if product_id:
                        if product_id not in product_data:
                            product_data[product_id] = {'product_id': product_id, 'urls': set()}
                        if url:
                            product_data[product_id]['urls'].add(url)
            
            logging.info(f"  Found {len(product_data)} unique products so far")
            
        except Exception as e:
            logging.error(f"Error processing event type {event_type}: {e}")
    
    # Convert to list and prioritize URLs
    product_list = []
    for pid, data in product_data.items():
        urls = list(data['urls'])
        
        # Prioritize SEO-friendly URLs over /catalog/product/view/id/
        seo_urls = [u for u in urls if '/catalog/product/view/id/' not in u]
        sample_url = seo_urls[0] if seo_urls else (urls[0] if urls else None)
        
        # Extract domain
        domain = extract_domain_from_url(sample_url) if sample_url else None
        
        product_list.append({
            'product_id': pid,
            'url': sample_url,
            'domain': domain
        })
    
    logging.info(f"Total unique products found: {len(product_list)}")
    
    # Save to file
    Path(OUTPUT_PRODUCT_IDS).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PRODUCT_IDS, 'w', encoding='utf-8') as f:
        json.dump(product_list, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Saved product data to {OUTPUT_PRODUCT_IDS}")
    return product_list


# HELPER FUNCTIONS
def extract_domain_from_url(url):
    """Extract domain from URL"""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None


def build_product_url(product_id, domain):
    """Build product URL from ID and domain (fallback)"""
    if not domain:
        domain = 'glamira.com'
    return f"https://www.{domain}/catalog/product/view/id/{product_id}"


def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return None
    text = re.sub(r'\s+', ' ', text.strip())
    return text if text else None


def clean_product_name(name):
    """Clean product name"""
    if not name:
        return None
    
    name = clean_text(name)
    
    # Remove domain/site name
    name = re.sub(r'\s*[\|\-]\s*Glamira.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*[\|\-]\s*GLAMIRA.*$', '', name)
    name = re.sub(r'\s*[\|\-]\s*Buy.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*[\|\-]\s*Shop.*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*[\|\-]\s*Kaufen.*$', '', name, flags=re.IGNORECASE)
    
    return name.strip() if name and len(name) > 2 else None


def parse_price(price_text):
    """Parse price from text to float"""
    if not price_text:
        return None
    try:
        # Remove currency symbols and spaces
        price_text = re.sub(r'[^\d,.]', '', price_text)
        # Handle European format (1.234,56 -> 1234.56)
        if ',' in price_text and '.' in price_text:
            # European format: 1.234,56
            if price_text.rindex(',') > price_text.rindex('.'):
                price_text = price_text.replace('.', '').replace(',', '.')
            # US format: 1,234.56
            else:
                price_text = price_text.replace(',', '')
        elif ',' in price_text:
            # Ambiguous, assume European
            price_text = price_text.replace(',', '.')
        
        return float(price_text)
    except:
        return None


def parse_rating(rating_text):
    """Parse rating from text to float"""
    if not rating_text:
        return None
    try:
        # Extract numeric rating ("4.85 stars" -> 4.85)
        match = re.search(r'(\d+\.?\d*)', rating_text)
        if match:
            return float(match.group(1))
    except:
        pass
    return None


def load_product_ids_from_file():
    """Load product IDs from file"""
    try:
        if Path(OUTPUT_PRODUCT_IDS).exists():
            with open(OUTPUT_PRODUCT_IDS, 'r', encoding='utf-8') as f:
                products = json.load(f)
            logging.info(f"Loaded {len(products)} products from file")
            return products
    except Exception as e:
        logging.error(f"Error loading product IDs: {e}")
    return None


def load_resume_state():
    """Load resume state from file"""
    try:
        if Path(OUTPUT_RESUME).exists():
            with open(OUTPUT_RESUME, 'r') as f:
                state = json.load(f)
            logging.info(f"Loaded resume state: {state['processed_count']} products already crawled")
            return state
    except Exception as e:
        logging.error(f"Error loading resume state: {e}")
    
    return {'processed_products': [], 'processed_count': 0, 'failed_products': []}


def save_resume_state(state):
    """Save resume state to file"""
    try:
        Path(OUTPUT_RESUME).parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_RESUME, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logging.error(f"Error saving resume state: {e}")


def append_to_bson(product_data):
    """Append product data to BSON file"""
    try:
        Path(OUTPUT_BSON).parent.mkdir(parents=True, exist_ok=True)
        
        with open(OUTPUT_BSON, 'ab') as f:
            data_copy = product_data.copy()
            # Convert datetime to ISO string
            if 'crawled_at' in data_copy and isinstance(data_copy['crawled_at'], datetime):
                data_copy['crawled_at'] = data_copy['crawled_at'].isoformat()
            
            bson_data = bson.BSON.encode(data_copy)
            f.write(bson_data)
        
        return True
    except Exception as e:
        logging.error(f"Error appending to BSON: {e}")
        return False



# CRAWLING FUNCTIONS
def crawl_product_details(driver, product_id, url, domain):
    """Crawl product details from URL using Selenium"""
    
    try:
        # Load page with timeout handling
        try:
            driver.get(url)
        except TimeoutException:
            # Page load timeout - try to work with partial content
            logging.debug(f"Page load timeout for {product_id}, attempting to parse partial content")
        except WebDriverException as e:
            return None, f"WebDriver error: {str(e)[:100]}"
        
        # Wait for body to be present
        try:
            WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            return None, "Page body not loaded (timeout)"
        except Exception as e:
            return None, f"Error waiting for page: {str(e)[:100]}"
        
        # Additional wait for JavaScript to load
        time.sleep(3)
        
        # Get page source and parse with BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Initialize result
        result = {
            'product_id': product_id,
            'url': driver.current_url,
            'domain': domain,
            'crawled_at': datetime.utcnow()
        }
        
        # 1. Product Name
        product_name = None
        h1_tag = soup.find('h1')
        if h1_tag:
            product_name = clean_product_name(h1_tag.get_text())
        
        if not product_name:
            meta_tag = soup.find('meta', property='og:title')
            if meta_tag:
                product_name = clean_product_name(meta_tag.get('content', ''))
        
        result['product_name'] = product_name
        
        # 2. Price & Currency
        price_raw = None
        price_elem = soup.select_one('span.price')
        if price_elem:
            price_raw = clean_text(price_elem.get_text())
        
        result['price_raw'] = price_raw
        result['price'] = parse_price(price_raw)
        
        # Currency
        currency_meta = soup.find('meta', property='product:price:currency')
        result['currency'] = currency_meta.get('content', '').strip() if currency_meta else None
        
        # 3. Category (from breadcrumb)
        category = None
        category_path = None
        breadcrumb = soup.find('div', class_='breadcrumbs')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            categories = [clean_text(link.get_text()) for link in links if link.get_text().strip()]
            if categories:
                category = categories[-1]  # Last item
                category_path = ' > '.join(categories)
        
        result['category'] = category
        result['category_path'] = category_path
        
        # 4. Main Image
        main_image = None
        img_elem = (
            soup.find('img', class_='main-image') or
            soup.find('img', {'itemprop': 'image'}) or
            soup.find('meta', property='og:image') or
            soup.select_one('.product-image img')
        )
        if img_elem:
            main_image = img_elem.get('src') or img_elem.get('content') or img_elem.get('data-src')
        
        result['image_url'] = main_image
        
        # 5. Description (short)
        description = None
        desc_meta = soup.find('meta', {'name': 'description'})
        if desc_meta:
            description = clean_text(desc_meta.get('content', ''))
        
        result['description'] = description[:500] if description else None
        
        # 6. Rating
        rating_raw = None
        rating = None
        
        # Try amstars-rating-container (Glamira specific)
        amstars = soup.find('div', class_=re.compile('amstars-rating-container'))
        if amstars:
            rating_raw = amstars.get('title', '')
            rating = parse_rating(rating_raw)
        
        result['rating_raw'] = rating_raw
        result['rating'] = rating
        
        return result, None
        
    except Exception as e:
        error_msg = str(e)
        logging.debug(f"Error crawling {product_id}: {error_msg}")
        return None, error_msg


# MAIN CRAWLING

def crawl_products():
    """Main crawling function"""
    
    logging.info("=" * 70)
    logging.info("STARTING PRODUCT DETAILS CRAWLING (Selenium)")
    logging.info("=" * 70)
    
    # Connect to MongoDB
    logging.info("\nStep 1: Connecting to MongoDB...")
    client, db = connect_mongodb()
    
    if not client:
        logging.error("Cannot proceed without MongoDB connection")
        return False
    
    driver = None
    
    try:
        # Load or extract product IDs
        logging.info("\nStep 2: Loading product data...")
        products = load_product_ids_from_file()
        
        if products is None or not products:
            logging.info("Product data file not found or empty, extracting from MongoDB...")
            products = extract_product_ids_from_mongodb(db)
        
        if not products:
            logging.warning("No products found")
            return False
        
        # Load resume state
        logging.info("\nStep 3: Checking resume state...")
        resume_state = load_resume_state()
        processed_set = set(resume_state['processed_products'])
        
        products_to_crawl = [p for p in products if p['product_id'] not in processed_set]
        
        logging.info(f"Total products: {len(products)}")
        logging.info(f"Already crawled: {len(processed_set)}")
        logging.info(f"Remaining: {len(products_to_crawl)}")
        
        if not products_to_crawl:
            logging.info("All products already crawled!")
            return True
        
        # Setup Selenium driver
        logging.info("\nStep 4: Setting up Selenium driver...")
        driver = setup_driver(headless=True)
        logging.info("Chrome driver ready")
        
        # Crawl products
        logging.info(f"\nStep 5: Crawling {len(products_to_crawl)} products...")
        total_batches = (len(products_to_crawl) + BATCH_SIZE - 1) // BATCH_SIZE
        
        start_time = time.time()
        batch_num = 0
        
        for i in range(0, len(products_to_crawl), BATCH_SIZE):
            batch_num += 1
            batch = products_to_crawl[i:i + BATCH_SIZE]
            
            batch_start_time = time.time()
            batch_success = 0
            batch_failed = 0
            
            for product in batch:
                product_id = product['product_id']
                url = product.get('url')
                domain = product.get('domain')
                
                # Use URL from MongoDB, fallback to constructed URL
                if not url:
                    url = build_product_url(product_id, domain)
                
                # Crawl product
                result, error = crawl_product_details(driver, product_id, url, domain)
                
                if result:
                    # Save to BSON
                    append_to_bson(result)
                    
                    # Update state
                    resume_state['processed_products'].append(product_id)
                    resume_state['processed_count'] += 1
                    batch_success += 1
                else:
                    # Track failed product
                    resume_state['failed_products'].append({
                        'product_id': product_id,
                        'url': url,
                        'domain': domain,
                        'error': error
                    })
                    batch_failed += 1
                    logging.warning(f"Failed: {product_id} | {error}")
                
                # Small delay between products
                time.sleep(DELAY_BETWEEN_PRODUCTS)
            
            batch_time = time.time() - batch_start_time
            avg_time = batch_time / len(batch) if batch else 0
            progress_pct = (resume_state['processed_count'] / len(products)) * 100
            
            logging.info(
                f"Batch {batch_num}/{total_batches} | "
                f"Success: {batch_success}/{len(batch)} | "
                f"Failed: {batch_failed} | "
                f"Time: {batch_time:.1f}s | "
                f"Avg: {avg_time:.1f}s/product | "
                f"Progress: {resume_state['processed_count']}/{len(products)} ({progress_pct:.1f}%)"
            )
            
            # Save state after each batch
            save_resume_state(resume_state)
            
            # Delay between batches
            if batch_num < total_batches:
                time.sleep(DELAY_BETWEEN_BATCHES)
        
        processing_time = time.time() - start_time
        
        # Retry failed products
        retry_time = 0
        failed_list = resume_state.get('failed_products', [])
        if failed_list:
            retry_start = time.time()
            logging.info(f"\n{'='*70}")
            logging.info(f"RETRYING {len(failed_list)} FAILED PRODUCTS")
            logging.info(f"{'='*70}")
            
            retry_success = 0
            retry_still_failed = []
            
            for idx, failed_product in enumerate(failed_list, 1):
                product_id = failed_product['product_id']
                url = failed_product.get('url')
                domain = failed_product.get('domain')
                
                result, error = crawl_product_details(driver, product_id, url, domain)
                
                if result:
                    append_to_bson(result)
                    resume_state['processed_products'].append(product_id)
                    resume_state['processed_count'] += 1
                    retry_success += 1
                else:
                    retry_still_failed.append(failed_product)
                
                if idx % 20 == 0:
                    logging.info(f"Retry progress: {idx}/{len(failed_list)} ({retry_success} succeeded)")
                
                time.sleep(DELAY_BETWEEN_PRODUCTS)
            
            logging.info(f"Retry complete: {retry_success}/{len(failed_list)} succeeded")
            
            resume_state['failed_products'] = retry_still_failed
            save_resume_state(resume_state)
            
            retry_time = time.time() - retry_start
        
        total_time = processing_time + retry_time
        
        # Final summary
        logging.info("\n" + "=" * 70)
        logging.info("CRAWLING COMPLETED")
        logging.info("=" * 70)
        logging.info(f"Total products crawled: {resume_state['processed_count']}")
        logging.info(f"Processing time: {processing_time/60:.1f} minutes")
        if retry_time > 0:
            logging.info(f"Retry time: {retry_time/60:.1f} minutes")
        logging.info(f"Total time: {total_time/60:.1f} minutes")
        if resume_state['processed_count'] > 0:
            logging.info(f"Average: {processing_time/resume_state['processed_count']:.1f}s/product")
        logging.info(f"Output file: {OUTPUT_BSON}")
        
        # Report failures
        final_failed = resume_state.get('failed_products', [])
        if final_failed:
            logging.warning(f"\n{len(final_failed)} products failed (after retry):")
            for failed in final_failed[:5]:
                logging.warning(f"  - {failed['product_id']}: {failed.get('error', 'Unknown')}")
            if len(final_failed) > 5:
                logging.warning(f"  ... and {len(final_failed) - 5} more")
        
        # Clear resume state
        if Path(OUTPUT_RESUME).exists():
            Path(OUTPUT_RESUME).unlink()
            logging.info("Resume state cleared")
        
        return True
        
    except Exception as e:
        logging.error(f"Error during crawling: {e}", exc_info=True)
        return False
        
    finally:
        logging.info("\nCleaning up...")
        if driver:
            driver.quit()
        if client:
            client.close()
        logging.info("Cleanup completed")


def main():
    setup_logging()
    
    try:
        success = crawl_products()
        
        if success:
            logging.info("\nScript completed successfully!")
            sys.exit(0)
        else:
            logging.error("\nScript completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.warning("\nScript interrupted by user")
        logging.info("Resume state saved. Run script again to continue.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"\nUnexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
