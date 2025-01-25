from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Engine
import pandas as pd
import yaml


class DatabaseConnector:
    def read_db_creds(self, creds_file: str) -> dict:
        """
        Read database credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        try:
            with open(creds_file, 'r') as file:
                creds = yaml.safe_load(file)
            print("Database credentials successfully read.")
            return creds
        except Exception as e:
            print(f"Error reading database credentials: {e}")
            raise

    def init_db_engine(self, creds_file: str, db_key: str) -> Engine:
        """
        Initialize and return a database engine using credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.
            db_key (str): Key in the credentials file to select specific database credentials.

        Returns:
            Engine: SQLAlchemy engine object.
        """
        try:
            creds = self.read_db_creds(creds_file)

            # Validate the db_key
            if db_key not in creds:
                raise ValueError(
                    f"Database key '{db_key}' not found in credentials file.")

            db_creds = creds[db_key]
            connection_string = (
                f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}"
                f"@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
            )
            engine = create_engine(connection_string)
            print(f"Database engine for '{db_key}' successfully initialized.")
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            raise

    def list_db_tables(self, engine: Engine) -> list:
        """
        List all tables in the connected database.

        Args:
            engine (Engine): SQLAlchemy engine object.

        Returns:
            list: A list of table names in the database.
        """
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            print("Tables in the database:", tables)
            return tables
        except Exception as e:
            print(f"Error listing database tables: {e}")
            raise

    def upload_data(self, df: pd.DataFrame, table_name: str, engine: Engine, if_exists: str = 'replace', index: bool = False):
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
            df.to_sql(table_name, engine, if_exists=if_exists, index=index)
            print(f"Data successfully uploaded to the table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
            raise

    def upload_to_db(self, df: pd.DataFrame, table_name: str, creds_file: str, db_key: str, if_exists: str = 'replace', index: bool = False):
        """
        Upload a Pandas DataFrame to a database table using credentials from a YAML file.

        Args:
            df (pd.DataFrame): DataFrame to upload.
            table_name (str): Name of the target table in the database.
            creds_file (str): Path to the YAML file containing database credentials.
            db_key (str): Key in the credentials file to select specific database credentials.
            if_exists (str): Behavior if the table already exists ('fail', 'replace', 'append'). Defaults to 'replace'.
            index (bool): Whether to include the DataFrame's index as a column in the table. Defaults to False.
        """
        try:
            engine = self.init_db_engine(creds_file, db_key)
            self.upload_data(df, table_name, engine,
                             if_exists=if_exists, index=index)
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
            raise

# Example usage:
# connector = DatabaseConnector()
# df = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
# connector.upload_to_db(df, "test_table", "db_creds.yaml", db_key="target_db")
