import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning  # Import the DataCleaning class


def pull_data_from_database(connector: DatabaseConnector, table_name: str, creds_file: str, db_key: str) -> pd.DataFrame:
    """
    Pull data from the database table into a Pandas DataFrame.

    Args:
        connector (DatabaseConnector): Instance of the DatabaseConnector class.
        table_name (str): Name of the target table in the database (dim_products).
        creds_file (str): Path to the database credentials file.
        db_key (str): Key in the credentials file for the target database (sales_data).

    Returns:
        pd.DataFrame: DataFrame containing the data from the specified database table.
    """
    try:
        print(f"Pulling data from the database table: {table_name}")
        engine = connector.initialize_db_engine(
            creds_file, db_key=db_key)  # Initialize DB engine
        data_frame = connector.fetch_data(
            table_name, engine)  # Use the fetch_data method
        print(f"Data successfully pulled from table '{table_name}'.")
        return data_frame
    except Exception as error:
        print(f"Error pulling data from the database: {error}")
        raise


def upload_data_to_database(data_frame: pd.DataFrame, table_name: str, connector: DatabaseConnector, creds_file: str, db_key: str):
    """
    Upload a Pandas DataFrame to a specified database table.

    Args:
        data_frame (pd.DataFrame): DataFrame to upload.
        table_name (str): Name of the target table in the database (dim_products).
        connector (DatabaseConnector): Instance of the DatabaseConnector class.
        creds_file (str): Path to the database credentials file.
        db_key (str): Key in the credentials file for the target database (sales_data).
    """
    try:
        print(f"Uploading cleaned data to the database table: {table_name}")
        engine = connector.initialize_db_engine(
            creds_file, db_key=db_key)  # Initialize DB engine
        connector.upload_data_to_db(
            data_frame, table_name, creds_file, db_key)  # Upload cleaned data
        print(f"Cleaned data successfully uploaded to table '{table_name}'.")
    except Exception as error:
        print(f"Error uploading data to the database: {error}")
        raise


if __name__ == "__main__":
    # Configuration
    creds_file_path = "db_creds.yaml"
    target_table_name = "dim_products"  # Table in sales_data database
    database_key = "target_db"  # Key for target database in creds file

    # Initialize classes
    db_connector = DatabaseConnector()
    cleaner = DataCleaning()  # Initialize DataCleaning class

    try:
        # Step 1: Pull data from the target database table (dim_products) in the sales_data database
        extracted_data = pull_data_from_database(
            db_connector, target_table_name, creds_file_path, database_key)

        # Step 2: Clean the extracted data using the DataCleaning class
        cleaned_data = cleaner.clean_and_cast_products_data(
            extracted_data)  # Clean and cast data

        # Step 3: Upload cleaned data back to the dim_products table in the sales_data database
        upload_data_to_database(
            cleaned_data, target_table_name, db_connector, creds_file_path, database_key)

    except Exception as e:
        print(f"Process failed: {e}")
        exit(1)
