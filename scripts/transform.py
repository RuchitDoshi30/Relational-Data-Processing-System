import os
import sys

# Ensure proper path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.db_utils import execute_sql_script, logger

def run_transformations():
    """
    Executes raw -> processed SQL transformations.
    """
    try:
        sql_path = os.path.join(os.path.dirname(__file__), '../sql/transform.sql')
        logger.info("Starting Data Transformation Phase (Raw -> Processed)...")
        
        execute_sql_script(sql_path)
        
        logger.info("Data Transformation Phase Complete.")
        
    except Exception as e:
        logger.error(f"Transformation Phase Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_transformations()
