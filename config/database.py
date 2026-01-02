import os
import logging
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establishes connection to the PostgreSQL database.
    Expects appropriate environment variables for production security.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "relational_data_processing") 
    db_user = os.getenv("DB_USER", "postgres")
    
    # Production security: Prefer env vars over defaults.
    db_pass = os.getenv("DB_PASS")
    
    if not db_pass:
        # Provide credentials via environment variables.
        error_msg = "DB_PASS environment variable is missing. Connection refused."
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_pass
        )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Operational Error connecting to database: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {e}")
        raise
