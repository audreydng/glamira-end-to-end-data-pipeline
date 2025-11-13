import logging
import sys
import json
from pathlib import Path
from datetime import datetime
import time

from pymongo import MongoClient, errors
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# === Configuration ===
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_DATABASE = 'glamira'
MONGODB_USERNAME = 'admin'
MONGODB_PASSWORD = 'Q1234qaz@@'
MONGODB_AUTH_SOURCE = 'admin'

# GCS Configuration
GCS_BUCKET_NAME = 'glamira_data'
GCS_PROJECT_ID = 'data-collection-and-storage'
GCS_CREDENTIALS_PATH = './gcs-service-account-key.json'

# Export Configuration
BATCH_SIZE = 10000  # Số lượng document mỗi chunk
EXPORT_FORMAT = 'parquet'
GCS_TARGET_PATH = 'data_in_parquet' # Thư mục trên GCS

# === ĐÂY LÀ PHẦN THAY ĐỔI ===

# Collections to export (Script sẽ tạo ra các file này)
COLLECTIONS_TO_EXPORT = [
    'ip_locations',
    'product_details',
    'summary'
]

# Local Parquet files to upload (File đã có sẵn, chỉ upload)
LOCAL_PARQUET_FILES = [
    './output/ip_locations.parquet',
    './output/product_details.parquet',
    './output/summary.parquet'
]

#Input

FORCE_STRING_COLS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid']

# Output
LOG_FILE = './logs/gcs_export.log'
TEMP_DIR = './temp_export' # Sẽ dùng thư mục này để lưu file parquet tạm

def setup_logging():
    log_dir = Path(LOG_FILE).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Xóa file log cũ (nếu có)
    if Path(LOG_FILE).exists():
        Path(LOG_FILE).unlink()
        
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def connect_to_gcs():
    """Kết nối GCS và trả về đối tượng bucket."""
    log = logging.getLogger()
    try:
        storage_client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
        return bucket
    except Exception as e:
        log.error(f"Failed to connect to GCS: {e}")
        return None

def connect_to_mongo():
    """Kết nối MongoDB và trả về đối tượng client."""
    log = logging.getLogger()
    try:
        client = MongoClient(
            host=MONGODB_HOST,
            port=MONGODB_PORT,
            username=MONGODB_USERNAME,
            password=MONGODB_PASSWORD,
            authSource=MONGODB_AUTH_SOURCE,
            serverSelectionTimeoutMS=5000  # Timeout 5s
        )
        # Kiểm tra kết nối
        client.server_info()
        return client
    except errors.ServerSelectionTimeoutError as err:
        log.error(f"Failed to connect to MongoDB (Timeout): {err}")
        return None
    except errors.OperationFailure as err:
        log.error(f"Failed to connect to MongoDB (Auth Failed): {err}")
        return None

def export_collection_to_parquet_chunked(db_client, collection_name, output_path):
    """
    Xuất một collection từ MongoDB ra file Parquet theo từng chunk.
    
    (CẬP NHẬT "MASTER SCHEMA": Giải quyết lỗi schema không nhất quán)
    """
    log = logging.getLogger()
    log.info(f"Starting chunked export for collection: {collection_name}")
    
    FORCE_STRING_COLS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid']

    collection = db_client[MONGODB_DATABASE][collection_name]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    writer = None
    master_schema = None  # <--- THÊM BIẾN NÀY
    chunk = []
    total_docs = 0
    
    try:
        with db_client.start_session() as session:
            cursor = collection.find({}, no_cursor_timeout=True, session=session)
            
            for doc in cursor:
                doc.pop('_id', None)
                chunk.append(doc)
                
                if len(chunk) >= BATCH_SIZE:
                    df = pd.DataFrame(chunk)
                    
                    # === Logic dọn dẹp kiểu dữ liệu (Giữ nguyên) ===
                    for col in FORCE_STRING_COLS:
                        if col in df.columns:
                            df[col] = df[col].astype(str)
                    
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            if col not in FORCE_STRING_COLS:
                                try:
                                    df[col] = pd.to_numeric(df[col], errors='raise')
                                except (ValueError, TypeError):
                                    df[col] = df[col].astype(str)
                    # === Hết logic dọn dẹp ===
                        
                    
                    # === LOGIC SCHEMA MỚI ===
                    if writer is None: # Đây là Batch 1
                        table = pa.Table.from_pandas(df, preserve_index=False)
                        writer = pq.ParquetWriter(output_path, table.schema)
                        master_schema = table.schema # <--- LƯU SCHEMA "CHUẨN"
                    else: # Đây là Batch 2, 3...
                        # Ép batch này tuân theo schema chuẩn
                        table = pa.Table.from_pandas(df, preserve_index=False, schema=master_schema) # <--- SỬ DỤNG SCHEMA
                    # === HẾT LOGIC MỚI ===

                    writer.write_table(table)
                    total_docs += len(chunk)
                    log.info(f"  ... exported {total_docs} docs from {collection_name}")
                    chunk = []

            # Xử lý chunk cuối cùng (nếu còn)
            if chunk:
                df = pd.DataFrame(chunk)

                # === Logic dọn dẹp kiểu dữ liệu (Lặp lại) ===
                for col in FORCE_STRING_COLS:
                    if col in df.columns:
                        df[col] = df[col].astype(str)
                
                for col in df.columns:
                    if df[col].dtype == 'object':
                        if col not in FORCE_STRING_COLS:
                            try:
                                df[col] = pd.to_numeric(df[col], errors='raise')
                            except (ValueError, TypeError):
                                df[col] = df[col].astype(str)
                # === Hết logic dọn dẹp ===

                # === LOGIC SCHEMA MỚI (Lặp lại) ===
                if writer is None: # Đây là batch DUY NHẤT
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    writer = pq.ParquetWriter(output_path, table.schema)
                else: # Đây là batch cuối cùng (không phải đầu tiên)
                    table = pa.Table.from_pandas(df, preserve_index=False, schema=master_schema) # <--- SỬ DỤNG SCHEMA
                # === HẾT LOGIC MỚI ===

                writer.write_table(table)
                total_docs += len(chunk)
                log.info(f"  ... finished exporting {total_docs} docs from {collection_name}")
            
            if total_docs == 0:
                log.warning(f"Collection {collection_name} is empty. No file created.")
                return False
                
    except Exception as e:
        log.error(f"Error during chunked export for {collection_name}: {e}", exc_info=True)
        return False
    finally:
        if writer:
            writer.close()
        
    log.info(f"Successfully exported {total_docs} documents for {collection_name} to {output_path}")
    return True
        
def upload_file_to_gcs(bucket, local_path, gcs_path):
    """Upload một file lên GCS."""
    log = logging.getLogger()
    local_file = Path(local_path)
    
    if not local_file.exists():
        log.warning(f"Local file not found, skipping: {local_path}")
        return None, 0

    file_size_mb = local_file.stat().st_size / (1024 * 1024)
    log.info(f"Uploading {local_path} ({file_size_mb:.2f} MB) to gs://{GCS_BUCKET_NAME}/{gcs_path}")
    
    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        log.info(f"Successfully uploaded to {gcs_path}")
        return gcs_path, file_size_mb
    except Exception as e:
        log.error(f"Failed to upload {local_path}: {e}")
        return None, 0

# === Hàm Main (Đã cập nhật) ===

def main():
    setup_logging()
    log = logging.getLogger()
    start_time = time.time()
    
    log.info("=" * 70)
    log.info("STARTING DATA EXPORT TO GCS")
    log.info("=" * 70)
    
    exported_files_manifest = [] # Dùng để tạo file manifest
    total_files_exported = 0

    # Step 1: Kết nối GCS
    log.info("\nStep 1: Connecting to GCS...")
    gcs_bucket = connect_to_gcs()
    if gcs_bucket is None:
        log.error("Cannot proceed without GCS connection")
        log.error("\nScript completed with errors")
        return
    log.info(f"Connected to GCS bucket: {GCS_BUCKET_NAME}")

    # Step 2: Kết nối MongoDB
    log.info("\nStep 2: Connecting to MongoDB...")
    mongo_client = connect_to_mongo()
    if mongo_client is None:
        log.error("Cannot proceed without MongoDB connection")
        log.error("\nScript completed with errors")
        return
    log.info(f"Connected to MongoDB: {MONGODB_DATABASE}")

    # Step 3: Export MongoDB collections to Parquet
    log.info("\nStep 3: Exporting MongoDB collections to Parquet...")
    
    # Tạo thư mục tạm
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    
    local_files_from_export = [] # Danh sách file vừa export để upload
    
    for collection_name in COLLECTIONS_TO_EXPORT:
        local_path = Path(TEMP_DIR) / f"{collection_name}.{EXPORT_FORMAT}"
        
        try:
            success = export_collection_to_parquet_chunked(
                mongo_client, 
                collection_name, 
                str(local_path)
            )
            
            if success: # Chỉ thêm nếu file được tạo (không rỗng)
                local_files_from_export.append(str(local_path))
        except Exception as e:
            log.error(f"Error exporting collection {collection_name}: {e}", exc_info=True)

    mongo_client.close()

    # === THAY ĐỔI CHÍNH NẰM Ở ĐÂY ===
    # Step 4: Uploading local Parquet files
    log.info("\nStep 4: Uploading all local Parquet files...")
    
    # Gộp 2 danh sách: file vừa export + file đã có sẵn
    all_files_to_upload = local_files_from_export + LOCAL_PARQUET_FILES
    log.info(f"Found {len(all_files_to_upload)} total files to upload.")
    
    for local_path in all_files_to_upload:
        file_name = Path(local_path).name
        gcs_path = f"{GCS_TARGET_PATH}/{file_name}"
        
        # Xác định nguồn
        source_collection = Path(local_path).stem
        if local_path in local_files_from_export:
            source_type = "exported"
        else:
            source_type = "pre-converted"

        uploaded_path, file_size = upload_file_to_gcs(gcs_bucket, local_path, gcs_path)
        if uploaded_path:
            exported_files_manifest.append({
                "path": gcs_path, 
                "size_mb": file_size, 
                "source_collection": source_collection,
                "source_type": source_type
            })
            total_files_exported += 1
    # === HẾT THAY ĐỔI ===

    # Step 5: Creating export manifest
    log.info("\nStep 5: Creating export manifest...")
    manifest_content = {
        "export_timestamp": datetime.utcnow().isoformat(),
        "project_id": GCS_PROJECT_ID,
        "bucket": GCS_BUCKET_NAME,
        "export_format": EXPORT_FORMAT,
        "files": exported_files_manifest
    }
    
    local_manifest_path = Path(TEMP_DIR) / "export_manifest.json"
    with open(local_manifest_path, 'w') as f:
        json.dump(manifest_content, f, indent=4)

    upload_file_to_gcs(gcs_bucket, str(local_manifest_path), f"{GCS_TARGET_PATH}/export_manifest.json")

    # Final Summary
    end_time = time.time()
    total_time_s = end_time - start_time
    total_time_min = total_time_s / 60
    
    log.info("\n" + "=" * 70)
    if total_files_exported > 0:
        log.info("EXPORT COMPLETED SUCCESSFULLY")
    else:
        log.warning("EXPORT COMPLETED, BUT 0 FILES WERE UPLOADED. CHECK LOGS.")
    log.info("=" * 70)
    log.info(f"Total files uploaded: {total_files_exported}")
    log.info(f"Export format: {EXPORT_FORMAT}")
    log.info(f"Total time: {total_time_s:.2f}s ({total_time_min:.2f} minutes)")
    log.info(f"GCS bucket: gs://{GCS_BUCKET_NAME}")
    log.info(f"Log file: {LOG_FILE}")
    log.info("\nUploaded files:")
    for item in exported_files_manifest:
        log.info(f"  - {item['path']} ({item['size_mb']:.2f} MB) [Source: {item['source_type']}]")
        
    log.info("\nNext steps:")
    log.info("1. Verify files in GCS Console")
    log.info("2. Create BigQuery external tables")
    log.info("3. Run data validation queries")
    
    log.info("\nCleaning up...")
    # (Bạn có thể thêm code xóa file trong TEMP_DIR ở đây nếu muốn)
    log.info("Cleanup completed")
    log.info("")


if __name__ == "__main__":
    main()
