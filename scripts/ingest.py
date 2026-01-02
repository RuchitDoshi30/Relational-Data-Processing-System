import csv
import os
import sys

# Ensure proper path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.database import get_db_connection, logger

DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), '../data')

# Explicit mapping of CSV filenames to their target raw tables
CSV_TO_TABLE_MAP = {
    'customer.csv': 'raw_customers',
    'products.csv': 'raw_products',
    'orders.csv': 'raw_orders',
    'order_items.csv': 'raw_order_items'
}

def initialize_schema(cursor):
    """
    Executes schema definition to ensure clean state.
    """
    schema_path = os.path.join(os.path.dirname(__file__), '../sql/schema.sql')
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file missing at {schema_path}")
        
    logger.info("Initializing database schema...")
    with open(schema_path, 'r', encoding='utf-8') as f:
        cursor.execute(f.read())
    logger.info("Schema initialized.")

def ingest_csv_data():
    """
    Orchestrates full CSV ingestion: Connect -> Init -> Bulk Insert.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # 1. Ensure fresh schema (Idempotency)
            initialize_schema(cur)

            # 2. Iterate through defined mappings and load data
            for filename, table_name in CSV_TO_TABLE_MAP.items():
                file_path = os.path.join(DATA_DIRECTORY, filename)
                
                if not os.path.exists(file_path):
                    logger.warning(f"Skipping {filename}: File not found.")
                    continue

                logger.info(f"Ingesting {filename} into {table_name}...")
                
                # Truncate to ensure no stale data in raw layer
                cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                
                with open(file_path, 'r', encoding='utf-8') as csv_file:
                    reader = csv.reader(csv_file)
                    headers = next(reader, None)
                    
                    if not headers:
                        logger.warning(f"Skipping {filename}: Empty file.")
                        continue
                        
                    # Bulk insert using executemany for performance
                    placeholders = ','.join(['%s'] * len(headers))
                    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    
                    row_batch = []
                    for row in reader:
                        row_batch.append(row)
                    
                    if row_batch:
                        cur.executemany(insert_query, row_batch)
                        logger.info(f" -> Successfully loaded {len(row_batch)} records into {table_name}.")
                    else:
                        logger.info(f" -> No data rows found in {filename}.")
        
        conn.commit()
        logger.info("Data Ingestion pipeline completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Critical error during ingestion: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    ingest_csv_data()
