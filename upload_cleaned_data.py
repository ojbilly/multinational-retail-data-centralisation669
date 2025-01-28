import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import requests


def extract_data_from_json(json_url: str) -> pd.DataFrame:
    """
    Extract data from a JSON URL and load it into a Pandas DataFrame.

    Args:
        json_url (str): URL of the JSON file.

    Returns:
        pd.DataFrame: DataFrame containing the extracted JSON data.
    """
    try:
        print(f"Extracting data from JSON URL: {json_url}")
        response = requests.get(json_url)
        response.raise_for_status()
        data_frame = pd.read_json(response.text)
        print("Data successfully extracted from JSON URL.")
        return data_frame
    except requests.RequestException as req_error:
        print(f"HTTP error occurred: {req_error}")
        raise
    except ValueError as json_error:
        print(f"Error decoding JSON: {json_error}")
        raise
    except Exception as error:
        print(f"Unexpected error occurred: {error}")
        raise


def upload_data_to_database(data_frame: pd.DataFrame, table_name: str, connector: DatabaseConnector, creds_file: str, db_key: str):
    """
    Upload a Pandas DataFrame to a specified database table.

    Args:
        data_frame (pd.DataFrame): DataFrame to upload.
        table_name (str): Name of the target table in the database.
        connector (DatabaseConnector): Instance of the DatabaseConnector class.
        creds_file (str): Path to the database credentials file.
        db_key (str): Key in the credentials file for the target database.
    """
    try:
        print(f"Uploading data to the database table: {table_name}")
        engine = connector.init_db_engine(creds_file, db_key=db_key)
        data_frame.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Data successfully uploaded to table '{table_name}'.")
    except Exception as error:
        print(f"Error uploading data to the database: {error}")
        raise


if __name__ == "__main__":
    # Configuration
    creds_file_path = "db_creds.yaml"
    target_table_name = "dim_date_times"
    database_key = "target_db"
    json_data_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    # Initialize classes
    db_connector = DatabaseConnector()

    try:
        # Step 1: Extract data from the JSON URL
        extracted_data = extract_data_from_json(json_data_url)

        # Step 2: Upload extracted data to the database
        upload_data_to_database(extracted_data, target_table_name, db_connector, creds_file_path, database_key)
    except Exception as e:
        print(f"Process failed: {e}")
        exit(1)
