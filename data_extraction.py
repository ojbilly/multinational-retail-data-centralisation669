import pandas as pd
from sqlalchemy.engine.base import Engine
from database_utils import DatabaseConnector
from yaml import safe_load
from tabula import read_pdf
import requests
import boto3
from io import StringIO
from typing import Dict


class DataExtractor:
    """
    A class for extracting data from various sources, including databases, APIs, PDFs, and S3 storage.
    """

    def extract_table_from_rds(self, connector: DatabaseConnector, table_name: str, creds_file: str, db_key: str) -> pd.DataFrame:
        """
        Extract a table from an RDS database into a Pandas DataFrame.

        Args:
            connector (DatabaseConnector): Instance to manage the database connection.
            table_name (str): Name of the table to extract.
            creds_file (str): Path to the credentials file.
            db_key (str): Key in the credentials file for the specific database.

        Returns:
            pd.DataFrame: DataFrame containing the table data.
        """
        try:
            engine = connector.init_db_engine(creds_file, db_key)
            tables = connector.list_db_tables(engine)

            if table_name not in tables:
                raise ValueError(
                    f"Table '{table_name}' not found. Available tables: {tables}")

            data_frame = pd.read_sql_table(table_name, con=engine)
            print(f"Data successfully extracted from table: {table_name}")
            return data_frame
        except Exception as error:
            print(f"Error extracting table from RDS: {error}")
            raise

    def extract_data_from_pdf(self, pdf_url: str) -> pd.DataFrame:
        """
        Extract data from a PDF file and return it as a Pandas DataFrame.

        Args:
            pdf_url (str): URL or local path to the PDF file.

        Returns:
            pd.DataFrame: DataFrame containing extracted data.
        """
        try:
            tables = read_pdf(pdf_url, pages="all",
                              multiple_tables=True, stream=True)
            data_frame = pd.concat(tables, ignore_index=True)
            print("Data successfully extracted from PDF.")
            return data_frame
        except Exception as error:
            print(f"Error extracting data from PDF: {error}")
            raise

    def fetch_store_count(self, api_endpoint: str, headers: Dict[str, str]) -> int:
        """
        Fetch the total number of stores from an API endpoint.

        Args:
            api_endpoint (str): API endpoint to retrieve the count.
            headers (dict): Headers for the API request.

        Returns:
            int: Number of stores.
        """
        try:
            response = requests.get(api_endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            store_count = data.get("number_stores", 0)
            print(f"Number of stores retrieved: {store_count}")
            return store_count
        except Exception as error:
            print(f"Error fetching store count: {error}")
            raise

    def retrieve_pdf_data(self, pdf_url: str) -> pd.DataFrame:
        """
        Extract data from a PDF file hosted at the provided URL and return it as a Pandas DataFrame.

        Args:
            pdf_url (str): URL of the PDF file.

        Returns:
            pd.DataFrame: DataFrame containing extracted data.
        """
        try:
            tables = read_pdf(pdf_url, pages="all",
                              multiple_tables=True, stream=True)
            data_frame = pd.concat(tables, ignore_index=True)
            print(f"Data successfully extracted from PDF: {pdf_url}")
            return data_frame
        except Exception as error:
            print(f"Error extracting data from PDF: {error}")
            raise

    def fetch_store_data(self, store_endpoint: str, headers: Dict[str, str], total_stores: int) -> pd.DataFrame:
        """
        Retrieve store data for a given number of stores.

        Args:
            store_endpoint (str): API endpoint for store details.
            headers (dict): Headers for the API request.
            total_stores (int): Total number of stores to fetch.

        Returns:
            pd.DataFrame: DataFrame containing store data.
        """
        store_data = []
        for store_id in range(1, total_stores + 1):
            try:
                response = requests.get(store_endpoint.format(
                    store_number=store_id), headers=headers)
                response.raise_for_status()
                store_data.append(response.json())
            except requests.RequestException as error:
                print(f"Error fetching store {store_id}: {error}")

        return pd.DataFrame(store_data)

    def extract_data_from_s3(self, s3_path: str) -> pd.DataFrame:
        """
        Extract data from an S3 bucket.

        Args:
            s3_path (str): S3 file path in the format s3://bucket-name/file-path.

        Returns:
            pd.DataFrame: DataFrame containing extracted data.
        """
        try:
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            s3_client = boto3.client("s3")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            data_frame = pd.read_csv(
                StringIO(obj["Body"].read().decode("utf-8")))
            print(f"Data successfully extracted from S3: {s3_path}")
            return data_frame
        except Exception as error:
            print(f"Error extracting data from S3: {error}")
            raise


if __name__ == "__main__":
    # Example usage
    extractor = DataExtractor()
    headers = {"x-api-key": "your_api_key"}
    endpoint_count = "https://api.example.com/number_stores"
    endpoint_store = "https://api.example.com/store_details/{store_number}"
    s3_address = "s3://bucket-name/products.csv"

    try:
        store_count = extractor.fetch_store_count(endpoint_count, headers)
        print(f"Store Count: {store_count}")
    except Exception as e:
        print(e)

    try:
        data_frame = extractor.extract_data_from_s3(s3_address)
        print(data_frame.head())
    except Exception as e:
        print(e)
