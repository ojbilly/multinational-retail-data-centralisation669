# Data Handling Project

## Table of Contents

1. [Project Description](#project-description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Optimizations and Performance Improvements](#optimizations-and-performance-improvements)
5. [File Structure](#file-structure)
6. [License](#license)

## Project Description

This project is designed to handle data extraction, cleaning, and uploading processes from various sources, including databases, APIs, PDFs, and S3 storage. The aim is to ensure data integrity and usability by performing the following tasks:

- Extracting data from RDS databases, APIs, PDFs, and S3.
- Cleaning data to remove inconsistencies, NULL values, and formatting errors.
- Optimizing database queries for improved performance.
- Uploading cleaned data into a target database for further use.

### What I Learned

- Implementing data cleaning processes with `pandas`.
- Utilizing `SQLAlchemy` to manage database connections and operations.
- Automating API interactions and extracting structured data from JSON and PDF formats.
- Working with AWS S3 for data storage and retrieval.
- Using YAML files to manage sensitive credentials securely.
- Optimizing SQL queries using indexes and efficient join strategies.

## Installation

To run this project, follow these steps:

1. Clone this repository:

   ```bash
   git clone https://github.com/your-repo/data-handling-project.git
   cd data-handling-project
   ```

2. Install required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up database credentials:

   - Place your database credentials in the `db_creds.yaml` file. Ensure the format matches:

     ```yaml
     source_db:
       RDS_HOST: <source-host>
       RDS_USER: <source-user>
       RDS_PASSWORD: <source-password>
       RDS_DATABASE: <source-database>
       RDS_PORT: <source-port>

     target_db:
       RDS_HOST: <target-host>
       RDS_USER: <target-user>
       RDS_PASSWORD: <target-password>
       RDS_DATABASE: <target-database>
       RDS_PORT: <target-port>
     ```

## Usage

1. **Extract Data**  
   Use the `data_extraction.py` script to retrieve data from various sources, including databases, APIs, and S3.

2. **Clean Data**  
   Utilize the `data_cleaning.py` script to clean and process extracted data. Various functions handle specific cleaning needs such as handling NULL values, standardizing weights, and fixing date formats.

3. **Upload Cleaned Data**  
   Run the `upload_cleaned_data.py` script to upload cleaned data to the target database.

   Example usage:

   ```bash
   python upload_cleaned_data.py
   ```

## Optimizations and Performance Improvements

### 1. Added Indexes for Faster Query Execution

To improve query performance, indexes were added to the necessary foreign key columns:

```sql
-- Add index on user_uuid in dim_users
CREATE INDEX idx_user_uuid ON dim_users (user_uuid);

-- Add index on store_code in dim_store_details
CREATE INDEX idx_store_code ON dim_store_details (store_code);

-- Add index on uuid in dim_products
CREATE INDEX idx_product_uuid ON dim_products (uuid);

-- Add index on date_uuid in dim_date_times
CREATE INDEX idx_date_uuid ON dim_date_times (date_uuid);

-- Add index on card_id in dim_card_details
CREATE INDEX idx_card_id ON dim_card_details (card_id);
```

### 2. Limited Batch Insertion to Prevent Overload

Instead of inserting all records at once, batch inserts were implemented to improve efficiency. Example:

```sql
INSERT INTO orders_table (
    user_uuid, store_code, product_uuid, card_id, date_uuid, order_date, total_amount, status
)
SELECT
    u.user_uuid,
    s.store_code,
    p.uuid AS product_uuid,
    c.card_id,
    dt.date_uuid,
    CURRENT_DATE,
    100.50,
    'pending'
FROM dim_users u
JOIN dim_store_details s ON s.store_code IS NOT NULL
JOIN dim_products p ON p.uuid IS NOT NULL
JOIN dim_card_details c ON c.card_id IS NOT NULL
JOIN dim_date_times dt ON dt.date_uuid IS NOT NULL
LIMIT 100;  -- Process smaller batches to improve performance
```

### 3. Checked Query Execution Plan

To analyze query performance and optimize slow queries, the `EXPLAIN ANALYZE` command was used:

```sql
EXPLAIN ANALYZE
SELECT
    u.user_uuid,
    s.store_code,
    p.uuid AS product_uuid,
    c.card_id,
    dt.date_uuid,
    CURRENT_DATE,
    100.50,
    'pending'
FROM dim_users u
JOIN dim_store_details s ON s.store_code IS NOT NULL
JOIN dim_products p ON p.uuid IS NOT NULL
JOIN dim_card_details c ON c.card_id IS NOT NULL
JOIN dim_date_times dt ON dt.date_uuid IS NOT NULL;
```

## File Structure

```plaintext
├── data_cleaning.py          # Functions for cleaning datasets
├── data_extraction.py        # Functions for extracting data from various sources
├── database_utils.py         # Database connection and utility functions
├── db_creds.yaml             # YAML file for storing database credentials
├── upload_cleaned_data.py    # Script to upload cleaned data to the target database
├── optimizations.md          # Documentation on optimizations applied
└── requirements.txt          # List of dependencies
```

## License

This project was created by **Osaze Omoruyi (OJ)**.
