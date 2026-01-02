import os
import sys

# Ensure proper path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.db_utils import execute_sql_script, logger

def run_loading():
    """
    Executes processed -> reporting SQL aggregations.
    """
    try:
        sql_path = os.path.join(os.path.dirname(__file__), '../sql/reporting.sql')
        logger.info("Starting Data Loading Phase (Processed -> Reporting)...")
        
        execute_sql_script(sql_path)
        
        logger.info("Data Loading Phase Complete.")
        
    except Exception as e:
        logger.error(f"Loading Phase Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_loading()
