import pandas as pd
import re


class DataCleaning:
    """
    A utility class for cleaning user data, card details data, and store data.

    Store Details Cleaning Requirements:
    - Change "NULL" strings data type into NULL data type.
    - Remove NULL values.
    - Convert "opening_date" column into a datetime data type.
    - Strip away symbols, letters, and white spaces from "staff_number" column.
    """

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the user data by handling NULL values, correcting date formats, and removing incorrect rows.

        Args:
            df (pd.DataFrame): Input DataFrame containing user data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        pass  # Retain for reference but not necessary for this task.

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the card details data by handling NULL values, duplicates, and formatting errors.

        Args:
            df (pd.DataFrame): Input DataFrame containing card details data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        try:
            df.replace("NULL", pd.NA, inplace=True)
            df.dropna(inplace=True)

            if "card_number" in df.columns:
                df.drop_duplicates(subset=["card_number"], inplace=True)
                df = df[df["card_number"].apply(lambda x: str(x).isdigit())]

            if "date_payment_confirmed" in df.columns:
                df["date_payment_confirmed"] = pd.to_datetime(
                    df["date_payment_confirmed"], errors="coerce"
                )
                df.dropna(subset=["date_payment_confirmed"], inplace=True)

            print(f"Card data successfully cleaned. Rows remaining: {len(df)}")
            return df
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            raise

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the store data retrieved from the API by handling NULL values, formatting errors, and adjusting data types.

        Args:
            df (pd.DataFrame): Input DataFrame containing store data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        try:
            df.replace("NULL", pd.NA, inplace=True)

            if "opening_date" in df.columns:
                df["opening_date"] = pd.to_datetime(
                    df["opening_date"], errors="coerce")

            if "staff_number" in df.columns:
                df["staff_number"] = df["staff_number"].astype(
                    str).str.replace(r"\D", "", regex=True)

            print(f"Store data cleaned for upload. Rows remaining: {len(df)}")
            return df
        except Exception as e:
            print(f"Error cleaning store data: {e}")
            raise

    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the weights in the products DataFrame to kilograms (kg) as float values.

        Args:
            df (pd.DataFrame): Input DataFrame containing product data with a 'weight' column.

        Returns:
            pd.DataFrame: DataFrame with cleaned and standardized weight values.
        """
        try:
            def weight_to_kg(weight):
                if pd.isna(weight):
                    return None
                weight = str(weight).lower().strip()
                if "kg" in weight:
                    return float(weight.replace("kg", "").strip())
                elif "g" in weight:
                    return float(weight.replace("g", "").strip()) / 1000
                elif "ml" in weight:
                    # Assuming 1:1 conversion
                    return float(weight.replace("ml", "").strip()) / 1000
                elif "l" in weight:
                    # Convert liters directly to kg
                    return float(weight.replace("l", "").strip()) * 1
                else:
                    return None  # If the format is unrecognized

            if "weight" in df.columns:
                df["weight"] = df["weight"].apply(weight_to_kg)
            else:
                raise ValueError(
                    "The 'weight' column is missing in the DataFrame.")

            print("Product weights successfully standardized to kg.")
            return df
        except Exception as e:
            print(f"Error converting product weights: {e}")
            raise

    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the products data by handling NULL values, formatting errors, and converting weights to a uniform unit (kg).

        Args:
            df (pd.DataFrame): Input DataFrame containing products data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        try:
            # Change "NULL" strings to actual NULL (NaN) values
            df.replace("NULL", pd.NA, inplace=True)

            # Remove rows with NULL values
            df.dropna(inplace=True)

            # Handle and standardize the 'weight' column
            if "weight" in df.columns:
                def parse_weight(value):
                    """
                    Parse weight strings to floats representing weight in kg.
                    - Supports values in formats like '500g', '0.5kg', '12 x 100g', etc.
                    """
                    value = value.lower().strip()
                    if "x" in value:  # Handle cases like '12 x 100g'
                        parts = value.split("x")
                        if len(parts) == 2:
                            multiplier = float(parts[0].strip())
                            unit_value = float(parts[1].strip().replace(
                                "g", "").replace("ml", ""))
                            return multiplier * unit_value / 1000.0  # Convert grams/ml to kg
                    elif "kg" in value:
                        return float(value.replace("kg", "").strip())
                    elif "g" in value or "ml" in value:  # Treat ml as equivalent to g
                        return float(value.replace("g", "").replace("ml", "").strip()) / 1000.0
                    raise ValueError(f"Unexpected weight format: {value}")

                df["weight"] = df["weight"].apply(parse_weight)

            print(
                f"Product data successfully cleaned. Rows remaining: {len(df)}")
            return df
        except Exception as e:
            print(f"Error cleaning product data: {e}")
            raise

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the orders data by removing unnecessary columns.

        Args:
            df (pd.DataFrame): Input DataFrame containing orders data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        try:
            # Remove unwanted columns
            columns_to_remove = ["first_name", "last_name", "1"]
            df = df.drop(columns=columns_to_remove, errors="ignore")
            print("Unnecessary columns removed.")
            return df
        except Exception as e:
            print(f"Error cleaning orders data: {e}")
            raise

    def clean_order_time_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the order time table data by:
        - Changing "NULL" strings to NULL (NaN).
        - Removing NULL values.
        - Converting values in "day", "month", and "year" columns to numeric.

        Args:
            df (pd.DataFrame): Input DataFrame containing order time data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        try:
            # Replace "NULL" strings with actual NULL (NaN)
            df.replace("NULL", pd.NA, inplace=True)

            # Remove rows with NULL values
            df.dropna(inplace=True)

            # Convert "day", "month", and "year" columns to numeric
            for col in ["day", "month", "year"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Drop rows where conversion resulted in NaN for "day", "month", or "year"
            df.dropna(subset=["day", "month", "year"], inplace=True)

            print("Order time data successfully cleaned.")
            return df
        except Exception as e:
            print(f"Error cleaning order time data: {e}")
            raise


# Example usage:
# cleaner = DataCleaning()
# product_data = pd.DataFrame({
#     'product_id': [1, 2, 3],
#     'weight': ['1kg', '500g', '2l']
# })
# cleaned_product_data = cleaner.convert_product_weights(product_data)
# print(cleaned_product_data)
