import pandas as pd
from sqlalchemy.engine.base import Engine
from database_utils import DatabaseConnector
from yaml import safe_load
from tabula import read_pdf
import requests
import boto3
from io import StringIO


class DataExtractor:
    def read_rds_table(self, connector: DatabaseConnector, table_name: str, creds_file: str, db_key: str) -> pd.DataFrame:
        """
        Extract data from an RDS database table into a Pandas DataFrame.

        Args:
            connector (DatabaseConnector): Instance of the DatabaseConnector class.
            table_name (str): Name of the table to extract data from.
            creds_file (str): Path to the YAML file containing database credentials.
            db_key (str): Key in the credentials file to use for database connection (e.g., "source_db" or "target_db").

        Returns:
            pd.DataFrame: DataFrame containing the table data.
        """
        try:
            engine = connector.init_db_engine(creds_file, db_key)
            tables = connector.list_db_tables(engine)

            if table_name not in tables:
                raise ValueError(
                    f"Table '{table_name}' not found in the database. Available tables: {tables}"
                )

            df = pd.read_sql_table(table_name, con=engine)
            print(f"Data successfully extracted from table: {table_name}")
            return df
        except Exception as e:
            print(f"Error extracting data from RDS table: {e}")
            raise

    def retrieve_pdf_data(self, pdf_link: str) -> pd.DataFrame:
        """
        Extract data from a PDF file using tabula-py and return a Pandas DataFrame.

        Args:
            pdf_link (str): URL or path to the PDF file.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        try:
            df_list = read_pdf(pdf_link, pages="all",
                               multiple_tables=True, stream=True)
            df = pd.concat(df_list, ignore_index=True)
            print("Data successfully extracted from PDF.")
            return df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            raise

    def list_number_of_stores(self, endpoint: str, headers: dict) -> int:
        """
        Retrieve the number of stores from the API.

        Args:
            endpoint (str): API endpoint to retrieve the number of stores.
            headers (dict): Dictionary containing the API header details.

        Returns:
            int: Number of stores.
        """
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            print(f"API response: {data}")

            number_of_stores = data.get("number_stores", 0)
            print(f"Number of stores retrieved: {number_of_stores}")
            return number_of_stores
        except Exception as e:
            print(f"Error retrieving the number of stores: {e}")
            raise

    def retrieve_stores_data(self, store_endpoint: str, headers: dict, number_of_stores: int) -> pd.DataFrame:
        """
        Retrieve data for all stores from the API and save them in a Pandas DataFrame.

        Args:
            store_endpoint (str): API endpoint to retrieve store details.
            headers (dict): Dictionary containing the API header details.
            number_of_stores (int): Number of stores to retrieve.

        Returns:
            pd.DataFrame: DataFrame containing data for all stores.
        """
        all_stores = []
        errors = []

        for store_number in range(1, number_of_stores + 1):
            try:
                url = store_endpoint.format(store_number=store_number)
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                store_data = response.json()
                all_stores.append(store_data)
            except requests.exceptions.RequestException as req_err:
                print(f"Error for store_number={store_number}: {req_err}")
                errors.append(
                    {"store_number": store_number, "error": str(req_err)})

        df = pd.DataFrame(all_stores)
        if errors:
            print(f"Errors encountered for {len(errors)} stores.")
        return df

    def extract_from_s3(self, s3_address: str) -> pd.DataFrame:
        """
        Download and extract data from an S3 bucket and return it as a Pandas DataFrame.

        Args:
            s3_address (str): S3 address of the file.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        try:
            s3_components = s3_address.replace("s3://", "").split("/", 1)
            bucket_name, key = s3_components[0], s3_components[1]

            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))
            print("Data successfully extracted from S3.")
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            raise


# Example usage for testing:
if __name__ == "__main__":
    extractor = DataExtractor()
    headers = {"x-api-key": "your_api_key"}
    number_stores_endpoint = "https://api.example.com/number_stores"
    store_endpoint = "https://api.example.com/store_details/{store_number}"
    s3_address = "s3://bucket-name/products.csv"

    # List number of stores
    try:
        number_of_stores = extractor.list_number_of_stores(
            number_stores_endpoint, headers)
        print(f"Total stores: {number_of_stores}")
    except Exception as e:
        print(e)

    # Retrieve and extract S3 data
    try:
        df = extractor.extract_from_s3(s3_address)
        print(df.head())
    except Exception as e:
        print(e)
