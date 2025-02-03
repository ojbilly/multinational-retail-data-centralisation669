import pandas as pd
from typing import Optional


class DataCleaning:
    """
    A utility class for cleaning user data, card details data, and store data.
    This class focuses on cleaning various datasets, ensuring they meet the required standards
    for further analysis or processing.
    """

    def clean_user_data(self, user_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean user data by adjusting data types for specific columns.
        """
        # Clean first_name and last_name columns to ensure proper string format
        if "first_name" in user_data.columns:
            user_data["first_name"] = user_data["first_name"].astype(
                str).str.strip()

        if "last_name" in user_data.columns:
            user_data["last_name"] = user_data["last_name"].astype(
                str).str.strip()

        # Convert date_of_birth to a date column
        if "date_of_birth" in user_data.columns:
            user_data["date_of_birth"] = pd.to_datetime(
                user_data["date_of_birth"], errors="coerce").dt.date

        # Convert country_code to VARCHAR(10) (a string column)
        if "country_code" in user_data.columns:
            user_data["country_code"] = user_data["country_code"].astype(
                str).str.strip()

        # Convert user_uuid to UUID format (although pandas does not have native UUID, we can use string for it)
        if "user_uuid" in user_data.columns:
            user_data["user_uuid"] = user_data["user_uuid"].astype(
                str).str.strip()

        # Convert join_date to a date column
        if "join_date" in user_data.columns:
            user_data["join_date"] = pd.to_datetime(
                user_data["join_date"], errors="coerce").dt.date

        print(f"User data cleaned. Remaining rows: {len(user_data)}")
        return user_data

    def clean_store_data(self, store_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the store data retrieved from the API by handling NULL values and adjusting data types.
        """
        store_data.replace("NULL", pd.NA, inplace=True)

        # Clean staff_number to ensure it's a numeric value
        if "staff_number" in store_data.columns:
            store_data["staff_number"] = store_data["staff_number"].str.replace(
                r"\D", "", regex=True)

        # Convert longitude and latitude to numeric and handle invalid values
        if "longitude" in store_data.columns:
            store_data["longitude"] = pd.to_numeric(
                store_data["longitude"], errors="coerce")

        if "latitude" in store_data.columns:
            store_data["latitude"] = pd.to_numeric(
                store_data["latitude"], errors="coerce")

        # Convert opening_date to a date
        if "opening_date" in store_data.columns:
            store_data["opening_date"] = pd.to_datetime(
                store_data["opening_date"], errors="coerce").dt.date

        # Update invalid longitude and latitude values to NULL
        store_data["longitude"].where(
            store_data["longitude"].notnull(), None, inplace=True)
        store_data["latitude"].where(
            store_data["latitude"].notnull(), None, inplace=True)

        print(f"Store data cleaned. Remaining rows: {len(store_data)}")
        return store_data

    def convert_weights_to_kg(self, products_data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the weights in the products DataFrame to kilograms (kg).
        """
        def parse_weight(weight: str) -> Optional[float]:
            weight = str(weight).lower().strip()
            if "kg" in weight:
                return float(weight.replace("kg", "").strip())
            elif "g" in weight:
                return float(weight.replace("g", "").strip()) / 1000
            elif "ml" in weight or "l" in weight:
                factor = 1 if "l" in weight else 1 / 1000
                return float(weight.replace("ml", "").replace("l", "").strip()) * factor
            return None

        if "weight" in products_data.columns:
            products_data["weight"] = products_data["weight"].apply(
                parse_weight)
        else:
            raise KeyError("The 'weight' column is missing.")

        print("Weights converted to kg.")
        return products_data

    def add_weight_class(self, products_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add a human-readable 'weight_class' column based on the product weight.
        """
        def get_weight_class(weight: float) -> str:
            if weight < 2:
                return "Light"
            elif 2 <= weight < 40:
                return "Mid_Sized"
            elif 40 <= weight < 140:
                return "Heavy"
            elif weight >= 140:
                return "Truck_Required"
            return "Unknown"

        if "weight" in products_data.columns:
            products_data["weight_class"] = products_data["weight"].apply(
                get_weight_class)
        print("Weight classification added.")
        return products_data

    def clean_orders_data(self, orders_data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove unnecessary columns from orders data.
        """
        columns_to_remove = ["first_name", "last_name", "1"]
        cleaned_data = orders_data.drop(
            columns=columns_to_remove, errors="ignore")
        print("Unnecessary columns removed from orders data.")
        return cleaned_data

    def clean_time_data(self, time_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the order time table by removing NULL values and ensuring numeric columns.
        """
        time_data.replace("NULL", pd.NA, inplace=True)
        time_data.dropna(inplace=True)

        for col in ["day", "month", "year"]:
            if col in time_data.columns:
                time_data[col] = pd.to_numeric(time_data[col], errors="coerce")

        time_data.dropna(subset=["day", "month", "year"], inplace=True)
        print("Time data cleaned.")
        return time_data

    def clean_product_data(self, products_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean product data, including removing '£' from product price and adding weight class.
        """
        if "product_price" in products_data.columns:
            products_data["product_price"] = products_data["product_price"].replace(
                "£", "", regex=True)

        print("Product price cleaned.")
        return products_data

    def clean_and_cast_products_data(self, products_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and cast the data types of the columns in the products data table.

        Args:
            products_data (pd.DataFrame): DataFrame containing the products data.

        Returns:
            pd.DataFrame: Cleaned and properly typed DataFrame.
        """

        # Cast 'product_price' to NUMERIC (float)
        if "product_price" in products_data.columns:
            products_data["product_price"] = pd.to_numeric(
                products_data["product_price"], errors="coerce")

        # Cast 'weight' to NUMERIC (float)
        if "weight" in products_data.columns:
            products_data["weight"] = pd.to_numeric(
                products_data["weight"], errors="coerce")

        # Cast 'EAN' to VARCHAR (string)
        if "EAN" in products_data.columns:
            products_data["EAN"] = products_data["EAN"].astype(str).str.strip()

        # Cast 'product_code' to VARCHAR (string)
        if "product_code" in products_data.columns:
            products_data["product_code"] = products_data["product_code"].astype(
                str).str.strip()

        # Cast 'date_added' to DATE
        if "date_added" in products_data.columns:
            products_data["date_added"] = pd.to_datetime(
                products_data["date_added"], errors="coerce").dt.date

        # Cast 'uuid' to UUID (this will remain as string since pandas does not directly support UUID)
        if "uuid" in products_data.columns:
            products_data["uuid"] = products_data["uuid"].astype(
                str).str.strip()

        # Cast 'still_available' to BOOL
        if "still_available" in products_data.columns:
            products_data["still_available"] = products_data["still_available"].astype(
                "bool", errors="ignore")

        # Cast 'weight_class' to VARCHAR (string)
        if "weight_class" in products_data.columns:
            products_data["weight_class"] = products_data["weight_class"].astype(
                str).str.strip()

        print(
            f"Products data cleaned and columns casted. Remaining rows: {len(products_data)}")
        return products_data

    def clean_and_cast_date_times_data(self, date_times_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and cast the data types of the columns in the dim_date_times data table.

        Args:
            date_times_data (pd.DataFrame): DataFrame containing the date_times data.

        Returns:
            pd.DataFrame: Cleaned and properly typed DataFrame.
        """

        # Cast 'month' to VARCHAR
        if "month" in date_times_data.columns:
            date_times_data["month"] = date_times_data["month"].astype(
                str).str.strip()

        # Cast 'year' to VARCHAR
        if "year" in date_times_data.columns:
            date_times_data["year"] = date_times_data["year"].astype(
                str).str.strip()

        # Cast 'day' to VARCHAR
        if "day" in date_times_data.columns:
            date_times_data["day"] = date_times_data["day"].astype(
                str).str.strip()

        # Cast 'time_period' to VARCHAR
        if "time_period" in date_times_data.columns:
            date_times_data["time_period"] = date_times_data["time_period"].astype(
                str).str.strip()

        # Cast 'date_uuid' to UUID, replacing invalid values with NULL
        if "date_uuid" in date_times_data.columns:
            date_times_data["date_uuid"] = date_times_data["date_uuid"].apply(
                lambda x: self.convert_to_uuid(x)
            )

        print(
            f"DateTimes data cleaned and columns casted. Remaining rows: {len(date_times_data)}")
        return date_times_data

    def convert_to_uuid(self, value: str) -> Optional[str]:
        """
        Helper function to convert a value to UUID. If invalid, return None.
        """
        try:
            return str(value) if len(value) == 36 else None
        except Exception:
            return None

    def clean_and_cast_card_details_data(self, card_details_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and cast the data types of the columns in the dim_card_details data table.

        Args:
            card_details_data (pd.DataFrame): DataFrame containing the card_details data.

        Returns:
            pd.DataFrame: Cleaned and properly typed DataFrame.
        """
        # Cast 'card_number' to VARCHAR (string)
        if "card_number" in card_details_data.columns:
            card_details_data["card_number"] = card_details_data["card_number"].astype(
                str).str.strip()

        # Cast 'expiry_date' to VARCHAR (string)
        if "expiry_date" in card_details_data.columns:
            card_details_data["expiry_date"] = card_details_data["expiry_date"].astype(
                str).str.strip()

        # Cast 'date_payment_confirmed' to DATE
        if "date_payment_confirmed" in card_details_data.columns:
            card_details_data["date_payment_confirmed"] = pd.to_datetime(
                card_details_data["date_payment_confirmed"], errors="coerce").dt.date

        print(
            f"CardDetails data cleaned and columns casted. Remaining rows: {len(card_details_data)}")
        return card_details_data


if __name__ == "__main__":
    # Example usage
    cleaner = DataCleaning()

    # Example usage for store data
    store_df = pd.DataFrame({
        "staff_number": ["123", "456", "abc"],
        "longitude": ["-73.935242", "invalid", "40.730610"],
        "latitude": ["40.712776", "invalid", "73.935242"],
        "opening_date": ["2020-01-01", "2021-02-01", "2019-03-01"]
    })
    cleaned_store_data = cleaner.clean_store_data(store_df)
    print(cleaned_store_data)

    # Example usage for products data
    product_df = pd.DataFrame({
        "weight": ["1kg", "500g", "2l"]
    })
    cleaned_product_data = cleaner.convert_weights_to_kg(product_df)
    cleaned_product_data = cleaner.add_weight_class(cleaned_product_data)
    print(cleaned_product_data)
