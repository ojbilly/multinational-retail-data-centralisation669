import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning  # Import DataCleaning class
from data_extraction import DataExtractor  # Import DataExtractor class


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
        print(f"Uploading cleaned data to the database table: {table_name}")
        engine = connector.initialize_db_engine(creds_file, db_key=db_key)
        connector.upload_data_to_db(data_frame, table_name, creds_file, db_key)
        print(f"Cleaned data successfully uploaded to table '{table_name}'.")
    except Exception as error:
        print(f"Error uploading data to the database: {error}")
        raise


if __name__ == "__main__":
    # Configuration
    creds_file_path = "db_creds.yaml"
    target_table_name = "dim_card_details"  # Table in target database
    database_key = "target_db"  # Key for target database in creds file
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

    # Initialize classes
    db_connector = DatabaseConnector()
    cleaner = DataCleaning()  # Initialize DataCleaning class
    extractor = DataExtractor()  # Initialize DataExtractor class

    try:
        # Step 1: Extract data from the PDF link
        extracted_data = extractor.retrieve_pdf_data(pdf_url)

        # Step 2: Clean the extracted data
        cleaned_data = cleaner.clean_card_data(extracted_data)

        # Step 3: Upload cleaned data to the target database table (dim_card_details)
        upload_data_to_database(
            cleaned_data, target_table_name, db_connector, creds_file_path, database_key
        )

    except Exception as e:
        print(f"Process failed: {e}")
        exit(1)
