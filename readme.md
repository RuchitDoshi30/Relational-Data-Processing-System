# Relational Data Processing System (PostgreSQL)

## Overview
This project validates core Data Engineering fundamentals by building a robust, local ETL pipeline that processes transactional business data. It demonstrates the ability to enforce strict data integrity using PostgreSQL 3NF schemas, orchestrate independent extraction/loading steps via Python, and perform complex data transformations using raw SQL logic rather than relying on abstraction frameworks.

## Tech Stack
- Python
- PostgreSQL
- SQL

## Database Setup (PostgreSQL)
### Step 1: Install PostgreSQL
Ensure PostgreSQL is installed locally. Do not hardcode credentials in scripts.

### Step 2: Create Database
Run the following SQL logic (e.g., via pgAdmin or psql):
```sql
CREATE DATABASE relational_data_processing;
```

### Step 3: Create PostgreSQL User (Optional)
You may use the default `postgres` user or create a dedicated one:
```sql
CREATE USER data_engineer WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE relational_data_processing TO data_engineer;
```

### Step 4: Configure Credentials (.env)
Create a file named `.env` in the root directory (this file is ignored by git).
Add your PostgreSQL credentials:

```ini
DB_NAME=relational_data_processing
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

Alternatively, you can set system-level environment variables.

### Step 5: Verification
The file `config/database.py` reads these values. Scripts will fail if `DB_PASSWORD` is not set.

## Architecture Overview
- **Raw Layer**: Immutable staging area where CSVs are ingested directly without modification.
- **Processed Layer**: 3NF normalized schema enforcing Primary/Foreign Key constraints and correct data types.
- **Reporting Layer**: Aggregated business metrics (e.g., Sales Summary, Customer Stats) optimized for analytics.
- **Why PostgreSQL?**: Chosen for MVCC concurrency control, ACID compliance, and rich SQL support necessary for complex joins.
- **Why SQL?**: Transformations are pushed to the database engine to leverage set-based processing efficiency over row-based scripting.

## ETL Workflow (High Level)
1. **Extract**: Reads structured CSV data and loads it into raw database tables (`ingest.py`).
2. **Transform**: Executes SQL scripts to clean, normalize, and insert data into the processed layer (`transform.py`).
3. **Load**: Aggregates processed data into final reporting tables for business consumption (`load.py`).

## How to Run (Minimal)
1. **Prerequisites**: Ensure PostgreSQL is running and a database (default: `relational_data_processing`) exists.
2. **Setup**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Execution**:
   ```bash
   # Ingest data (Extract)
   python scripts/ingest.py
   
   # Run SQL Transformations (Transform)
   python scripts/transform.py
   
   # Generate Reports (Load)
   python scripts/load.py
   ```

## Resume Alignment
- **Python**: Backend orchestration, file handling, DB connectivity.
- **SQL**: Complex joins, window functions, aggregations, DDL/DML.
- **PostgreSQL**: Schema design, constraints, indexing, transactions.
- **ETL Processes**: End-to-end pipeline design (Extract -> Transform -> Load).
- **Data Ingestion**: Bulk loading structured data from files.
- **Data Transformation**: Cleaning and normalizing raw data into 3NF.
- **Backend Development**: Modular script architecture.
- **DBMS**: Relational modeling and integrity enforcement.
