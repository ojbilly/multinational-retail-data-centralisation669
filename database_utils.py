from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Engine
import pandas as pd
import yaml
from typing import Dict, List


class DatabaseConnector:
    """
    A utility class for managing database connections and operations.
    """

    def read_db_credentials(self, creds_file: str) -> Dict[str, Dict[str, str]]:
        """
        Read database credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.

        Returns:
            Dict[str, Dict[str, str]]: A dictionary containing the database credentials.
        """
        try:
            with open(creds_file, "r") as file:
                credentials = yaml.safe_load(file)
            print("Database credentials successfully loaded.")
            return credentials
        except FileNotFoundError:
            print(f"Error: File '{creds_file}' not found.")
            raise
        except yaml.YAMLError as error:
            print(f"Error reading YAML file: {error}")
            raise
        except Exception as error:
            print(f"Unexpected error reading database credentials: {error}")
            raise

    def initialize_db_engine(self, creds_file: str, db_key: str) -> Engine:
        """
        Initialize and return a database engine using credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.
            db_key (str): Key in the credentials file to select specific database credentials.

        Returns:
            Engine: SQLAlchemy engine object for database connection.
        """
        try:
            credentials = self.read_db_credentials(creds_file)

            if db_key not in credentials:
                raise KeyError(f"Database key '{db_key}' not found in credentials file.")

            db_creds = credentials[db_key]
            connection_string = (
                f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}"
                f"@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
            )
            engine = create_engine(connection_string)
            print(f"Database engine for '{db_key}' initialized successfully.")
            return engine
        except KeyError as error:
            print(f"Error: {error}")
            raise
        except Exception as error:
            print(f"Error initializing database engine: {error}")
            raise

    def list_database_tables(self, engine: Engine) -> List[str]:
        """
        List all tables in the connected database.

        Args:
            engine (Engine): SQLAlchemy engine object.

        Returns:
            List[str]: A list of table names in the database.
        """
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            print(f"Tables in the database: {tables}")
            return tables
        except Exception as error:
            print(f"Error listing database tables: {error}")
            raise

    def upload_dataframe(self, df: pd.DataFrame, table_name: str, engine: Engine, if_exists: str = "replace", index: bool = False):
        """
        Upload a Pandas DataFrame to a database table.

        Args:
            df (pd.DataFrame): DataFrame to upload.
            table_name (str): Name of the target table in the database.
            engine (Engine): SQLAlchemy engine object.
            if_exists (str): Behavior if the table already exists ('fail', 'replace', 'append').
            index (bool): Whether to include the DataFrame's index as a column in the table.
        """
        try:
            df.to_sql(table_name, con=engine, if_exists=if_exists, index=index)
            print(f"Data successfully uploaded to table '{table_name}'.")
        except Exception as error:
            print(f"Error uploading data to the database: {error}")
            raise

    def upload_data_to_db(self, df: pd.DataFrame, table_name: str, creds_file: str, db_key: str, if_exists: str = "replace", index: bool = False):
        """
        Upload a Pandas DataFrame to a database table using credentials from a YAML file.

        Args:
            df (pd.DataFrame): DataFrame to upload.
            table_name (str): Name of the target table in the database.
            creds_file (str): Path to the YAML file containing database credentials.
            db_key (str): Key in the credentials file to select specific database credentials.
            if_exists (str): Behavior if the table already exists ('fail', 'replace', 'append').
            index (bool): Whether to include the DataFrame's index as a column in the table.
        """
        try:
            engine = self.initialize_db_engine(creds_file, db_key)
            self.upload_dataframe(df, table_name, engine, if_exists=if_exists, index=index)
        except Exception as error:
            print(f"Error uploading data to the database: {error}")
            raise


if __name__ == "__main__":
    # Example usage
    connector = DatabaseConnector()
    example_df = pd.DataFrame({"column1": [1, 2], "column2": ["A", "B"]})
    creds_path = "db_creds.yaml"
    table_name = "example_table"

    try:
        connector.upload_data_to_db(example_df, table_name, creds_path, db_key="target_db")
    except Exception as e:
        print(f"Error during upload: {e}")
