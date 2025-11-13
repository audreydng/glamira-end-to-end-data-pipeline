import logging
import sys
import json
import time
import bson
from pathlib import Path
from datetime import datetime

from pymongo import MongoClient, errors, UpdateOne
import IP2Location


# Configuration
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_DATABASE = 'glamira'
MONGODB_COLLECTION = 'summary'
MONGODB_LOCATION_COLLECTION = 'ip_locations'

# Auth-enabled connection URI
MONGODB_URI = 'mongodb://glamira_user:Q1234qaz@localhost:27017/glamira?authSource=glamira'

IP2LOCATION_BIN_FILE = './IP-COUNTRY-REGION-CITY.BIN'
BATCH_SIZE = 1000
IP_FIELD_NAME = 'ip'

# Output files
OUTPUT_UNIQUE_IPS = './output/unique_ips.txt'
OUTPUT_BSON = './output/ip_locations.bson'
OUTPUT_RESUME = './output/processing_state.json'
LOG_FILE = './logs/ip_processing.log'


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


def connect_mongodb():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        
        db = client.get_default_database()
        if db is None:
            db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        location_collection = db[MONGODB_LOCATION_COLLECTION]
        
        logging.info(f"Connected to MongoDB: {MONGODB_DATABASE}")
        return client, db, collection, location_collection
        
    except errors.ServerSelectionTimeoutError as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None, None, None, None
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None, None, None, None


def save_unique_ips_to_file(collection, ip_field):
    # Create output folder
    Path(OUTPUT_UNIQUE_IPS).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        logging.info("Extracting unique IP addresses from MongoDB...")
        
        pipeline = [
            {'$match': {ip_field: {'$exists': True, '$ne': None, '$ne': ''}}},
            {'$group': {'_id': f'${ip_field}'}},
            {'$project': {'_id': 0, 'ip': '$_id'}}
        ]
        
        unique_ips = []
        logging.info("Running aggregation pipeline...")
        cursor = collection.aggregate(pipeline, allowDiskUse=True)
        
        # Save file txt
        with open(OUTPUT_UNIQUE_IPS, 'w', encoding='utf-8') as f:
            count = 0
            for doc in cursor:
                ip = doc.get('ip', '').strip()
                if ip:
                    f.write(f"{ip}\n")
                    unique_ips.append(ip)
                    count += 1
                    
                    if count % 50000 == 0:
                        logging.info(f"Extracted {count:,} unique IPs so far...")
            
            if count > 0:
                logging.info(f"Extraction complete: {count:,} unique IPs")
        
        logging.info(f"Saved {len(unique_ips)} unique IPs to {OUTPUT_UNIQUE_IPS}")
        return unique_ips
        
    except Exception as e:
        logging.error(f"Error saving unique IPs: {e}")
        return []


def load_unique_ips_from_file():
    try:
        if not Path(OUTPUT_UNIQUE_IPS).exists():
            return None
        
        with open(OUTPUT_UNIQUE_IPS, 'r', encoding='utf-8') as f:
            ips = [line.strip() for line in f if line.strip()]
        
        logging.info(f"Loaded {len(ips)} unique IPs from file")
        return ips
        
    except Exception as e:
        logging.error(f"Error loading unique IPs from file: {e}")
        return None


def load_resume_state():
    try:
        if Path(OUTPUT_RESUME).exists():
            with open(OUTPUT_RESUME, 'r') as f:
                state = json.load(f)
            logging.info(f"Loaded resume state: {state['processed_count']} IPs already processed")
            return state
    except Exception as e:
        logging.error(f"Error loading resume state: {e}")
    
    return {'processed_ips': [], 'processed_count': 0, 'last_batch': 0}


def save_resume_state(state):
    try:
        Path(OUTPUT_RESUME).parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_RESUME, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logging.error(f"Error saving resume state: {e}")


def initialize_ip2location(bin_file):
    try:
        ip2loc = IP2Location.IP2Location()
        ip2loc.open(bin_file)
        logging.info(f"IP2Location initialized: {bin_file}")
        return ip2loc
        
    except Exception as e:
        logging.error(f"Failed to initialize IP2Location: {e}")
        return None


def get_ip_location(ip2loc, ip_address):
    try:
        rec = ip2loc.get_all(ip_address)
        
        location_data = {
            'ip_address': ip_address,
            'country_code': rec.country_short if rec.country_short != '-' else None,
            'country_name': rec.country_long if rec.country_long != '-' else None,
            'region_name': rec.region if rec.region != '-' else None,
            'city_name': rec.city if rec.city != '-' else None,
            'processed_at': datetime.utcnow()
        }
        
        return location_data
        
    except Exception as e:
        logging.warning(f"Error processing IP {ip_address}: {e}")
        return None


def save_to_mongodb(location_collection, location_data):
    if not location_data:
        return True, []
        
    try:
        operations = [
            UpdateOne(
                {'ip_address': data['ip_address']},
                {'$set': data},
                upsert=True
            )
            for data in location_data
        ]
        
        if operations:
            result = location_collection.bulk_write(
                operations, 
                ordered=False,
                bypass_document_validation=True
            )
            
            return True, []
        
        return True, []
        
    except Exception as e:
        from pymongo.errors import BulkWriteError
        
        # Handle partial success in bulk write
        if isinstance(e, BulkWriteError):
            details = e.details
            write_errors = details.get('writeErrors', [])
            
            # Get indexes of failed IPs
            failed_indexes = [err['index'] for err in write_errors]
            failed_ips = [location_data[idx]['ip_address'] for idx in failed_indexes if idx < len(location_data)]
            
            success_count = len(location_data) - len(failed_indexes)
            
            logging.warning(f"Partial MongoDB write: {success_count}/{len(location_data)} succeeded, {len(failed_indexes)} failed")
            for ip in failed_ips[:5]:
                logging.warning(f"  Failed IP: {ip}")
            
            # Return partial success with list of failed IPs
            return True, failed_ips
        else:
            logging.error(f"Error saving to MongoDB: {e}")
            # Complete failure - all IPs failed
            return False, [data['ip_address'] for data in location_data]


def append_to_bson(location_data):
    if not location_data:
        return False
    
    try:
        Path(OUTPUT_BSON).parent.mkdir(parents=True, exist_ok=True)
        
        # Append to .bson file
        with open(OUTPUT_BSON, 'ab') as f:
            for data in location_data:
                # Convert datetime to string for .bson
                data_copy = data.copy()
                if 'processed_at' in data_copy and isinstance(data_copy['processed_at'], datetime):
                    data_copy['processed_at'] = data_copy['processed_at'].isoformat()
                
                bson_data = bson.BSON.encode(data_copy)
                f.write(bson_data)
        
        return True
        
    except Exception as e:
        logging.error(f"Error appending to BSON: {e}")
        return False


def drop_indexes(location_collection):
    try:
        # Drop all indexes except _id
        location_collection.drop_indexes()
        logging.info("Dropped all indexes (except _id)")
    except Exception as e:
        logging.warning(f"Error dropping indexes: {e}")


def create_indexes(location_collection):
    try:
        location_collection.create_index('ip_address', unique=True)
        location_collection.create_index('country_code')
        location_collection.create_index('country_name')
        location_collection.create_index('region_name')
        location_collection.create_index('city_name')
        logging.info("Created MongoDB indexes")
    except Exception as e:
        logging.error(f"Error creating indexes: {e}")


def get_statistics(location_collection):
    try:
        total_locations = location_collection.count_documents({})
        
        country_pipeline = [
            {'$group': {'_id': '$country_name', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 5}
        ]
        top_countries = list(location_collection.aggregate(country_pipeline))
        
        return {
            'total_locations': total_locations,
            'top_countries': top_countries
        }
        
    except Exception as e:
        logging.error(f"Error getting statistics: {e}")
        return {}


def process_ip_locations():
    
    logging.info("=" * 70)
    logging.info("STARTING IP LOCATION PROCESSING")
    logging.info("=" * 70)
    
    # Connect to MongoDB
    logging.info("\nConnecting to MongoDB...")
    client, db, collection, location_collection = connect_mongodb()
    
    if not client:
        logging.error("Cannot proceed without MongoDB connection")
        return False
    
    # Load or extract unique IPs
    logging.info("\nLoading unique IP addresses...")
    unique_ips = load_unique_ips_from_file()
    
    if unique_ips is None or not unique_ips:
        logging.info("Unique IPs file not found or empty, extracting from MongoDB...")
        unique_ips = save_unique_ips_to_file(collection, IP_FIELD_NAME)
    
    if not unique_ips:
        logging.warning("No IP addresses found in MongoDB")
        client.close()
        return False
    
    # Load resume state
    logging.info("\nChecking resume state...")
    resume_state = load_resume_state()
    processed_ips_set = set(resume_state['processed_ips'])
    
    ips_to_process = [ip for ip in unique_ips if ip not in processed_ips_set]
    
    logging.info(f"Total unique IPs: {len(unique_ips)}")
    logging.info(f"Already processed: {len(processed_ips_set)}")
    logging.info(f"Remaining to process: {len(ips_to_process)}")
    
    if not ips_to_process:
        logging.info("All IPs already processed!")
        client.close()
        return True
    
    # Initialize IP2Location
    logging.info("\nInitializing IP2Location...")
    ip2loc = initialize_ip2location(IP2LOCATION_BIN_FILE)
    
    if not ip2loc:
        logging.error("Cannot proceed without IP2Location")
        client.close()
        return False
    
    try:
        # Process IPs in batches
        logging.info(f"\nProcessing {len(ips_to_process)} IP addresses...")
        total_batches = (len(ips_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
        
        start_batch = resume_state['last_batch']
        total_start_time = time.time()
        
        for i in range(start_batch * BATCH_SIZE, len(ips_to_process), BATCH_SIZE):
            batch_num = i // BATCH_SIZE + 1
            batch = ips_to_process[i:i + BATCH_SIZE]
            
            batch_start_time = time.time()
            
            # Process batch - IP lookups
            lookup_start = time.time()
            batch_results = []
            for ip in batch:
                location_data = get_ip_location(ip2loc, ip)
                if location_data:
                    batch_results.append(location_data)
            lookup_time = time.time() - lookup_start
            
            # Save results
            bson_time = 0 
            state_time = 0
            
            if batch_results:
                # Save to BSON file
                bson_start = time.time()
                append_to_bson(batch_results)
                bson_time = time.time() - bson_start
                
                # Update resume state
                state_start = time.time()
                resume_state['processed_ips'].extend([r['ip_address'] for r in batch_results])
                resume_state['processed_count'] += len(batch_results)
                resume_state['last_batch'] = batch_num
                save_resume_state(resume_state)
                state_time = time.time() - state_start
            
            batch_total_time = time.time() - batch_start_time
            
            # Progress reporting
            total_processed = resume_state['processed_count']
            progress_pct = (total_processed / len(unique_ips)) * 100
            
            progress_msg = (f"Batch {batch_num}/{total_batches} | "
                           f"IPs: {len(batch_results)}/{len(batch)} | "
                           f"Lookup: {lookup_time:.3f}s | "
                           f"BSON: {bson_time:.3f}s | "
                           f"State: {state_time:.3f}s | "
                           f"Total: {batch_total_time:.3f}s | "
                           f"Progress: {total_processed}/{len(unique_ips)} ({progress_pct:.1f}%)")
            
            logging.info(progress_msg)
        
        total_time = time.time() - total_start_time
        
        # Final summary
        logging.info("\n" + "=" * 70)
        logging.info("PROCESSING COMPLETED SUCCESSFULLY")
        logging.info("=" * 70)
        logging.info(f"Total IPs processed: {resume_state['processed_count']}")
        logging.info(f"Total processing time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
        logging.info(f"Average time per IP: {total_time/resume_state['processed_count']:.4f}s")
        logging.info(f"Unique IPs file: {OUTPUT_UNIQUE_IPS}")
        logging.info(f"BSON output file: {OUTPUT_BSON}")
        logging.info(f"Resume state: {OUTPUT_RESUME}")
        logging.info(f"Log file: {LOG_FILE}")
        
        # Instructions for importing to MongoDB
        logging.info("\n" + "=" * 70)
        logging.info("TO IMPORT INTO MONGODB:")
        logging.info("=" * 70)
        logging.info("Run the following command:")
        logging.info(f"  mongorestore \\")
        logging.info(f"    --uri='mongodb://glamira_user:PASSWORD@localhost:27017/glamira?authSource=glamira' \\")
        logging.info(f"    --collection=ip_locations \\")
        logging.info(f"    --db=glamira \\")
        logging.info(f"    {OUTPUT_BSON} \\")
        logging.info(f"    --numInsertionWorkersPerCollection=4 \\")
        logging.info(f"    --writeConcern='{{\"w\":0}}'")
        logging.info("\nOptional: Create unique index after import:")
        logging.info("  mongosh -u admin -p --authenticationDatabase admin")
        logging.info("  use glamira")
        logging.info("  db.ip_locations.createIndex({ip_address: 1}, {unique: true})")
        
        # Clear resume state after successful completion
        if Path(OUTPUT_RESUME).exists():
            Path(OUTPUT_RESUME).unlink()
            logging.info("\nResume state cleared")
        
        return True
        
    except Exception as e:
        logging.error(f"Error during processing: {e}", exc_info=True)
        return False
        
    finally:
        logging.info("\nCleaning up...")
        if ip2loc:
            ip2loc.close()
        if client:
            client.close()
        logging.info("Cleanup completed")


def main():
    setup_logging()
    
    try:
        success = process_ip_locations()
        
        if success:
            logging.info("\nScript completed successfully!")
            sys.exit(0)
        else:
            logging.error("\nScript completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.warning("\nScript interrupted by user")
        logging.info("Resume state saved. Run script again to continue from where it stopped.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"\nUnexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()