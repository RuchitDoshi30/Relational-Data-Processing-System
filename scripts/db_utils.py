import os
import sys
import logging

# Ensure we can import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.database import get_db_connection, logger

def execute_sql_script(script_path):
    """
    Transactional execution of SQL script files.
    """
    if not os.path.exists(script_path):
        logger.error(f"SQL script not found: {script_path}")
        raise FileNotFoundError(f"SQL script not found: {script_path}")

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            logger.info(f"Reading SQL script: {os.path.basename(script_path)}")
            with open(script_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            logger.info(f"Executing SQL script...")
            cur.execute(sql_content)
            
        conn.commit()
        logger.info(f"Successfully executed {os.path.basename(script_path)}")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to execute {os.path.basename(script_path)}: {e}")
        raise e
    finally:
        if conn:
            conn.close()
