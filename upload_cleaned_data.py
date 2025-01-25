import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor  # Ensure DataExtractor is imported
import requests

# Initialize classes
connector = DatabaseConnector()
extractor = DataExtractor()  # Use DataExtractor for JSON data extraction

# Path to database credentials file
creds_file = "db_creds.yaml"

try:
    print("Extracting date details data from JSON link...")
    # URL of the JSON file
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    # Extract data directly from the JSON URL
    response = requests.get(json_url)
    response.raise_for_status()  # Check for HTTP request errors
    raw_date_data = pd.read_json(response.text)  # Load JSON into a DataFrame

    print("Date details data successfully extracted from JSON link.")
except Exception as e:
    print(f"Error: Failed to extract date data from JSON link: {e}")
    exit(1)

try:
    print("Uploading date details data to the target database...")
    # Initialize the database engine using the target_db key
    engine = connector.init_db_engine(creds_file, db_key="target_db")

    # Upload the extracted data to the dim_date_times table
    raw_date_data.to_sql(
        "dim_date_times", engine, if_exists="replace", index=False
    )
    print("Date details data successfully uploaded to the dim_date_times table in the sales_data database.")
except Exception as e:
    print(f"Error: Failed to upload date details data: {e}")
    exit(1)
