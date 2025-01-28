import pandas as pd
from typing import Optional


class DataCleaning:
    """
    A utility class for cleaning user data, card details data, and store data.

    This class focuses on cleaning various datasets, ensuring they meet the required standards
    for further analysis or processing.
    """

    def clean_card_data(self, card_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the card details data by handling NULL values, duplicates, and formatting errors.

        Args:
            card_data (pd.DataFrame): DataFrame containing card details data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        card_data.replace("NULL", pd.NA, inplace=True)
        card_data.dropna(inplace=True)

        if "card_number" in card_data.columns:
            card_data.drop_duplicates(subset=["card_number"], inplace=True)
            card_data = card_data[card_data["card_number"].apply(lambda x: str(x).isdigit())]

        if "date_payment_confirmed" in card_data.columns:
            card_data["date_payment_confirmed"] = pd.to_datetime(
                card_data["date_payment_confirmed"], errors="coerce"
            )
            card_data.dropna(subset=["date_payment_confirmed"], inplace=True)

        print(f"Card data cleaned. Remaining rows: {len(card_data)}")
        return card_data

    def clean_store_data(self, store_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the store data retrieved from the API by handling NULL values and adjusting data types.

        Args:
            store_data (pd.DataFrame): DataFrame containing store data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        store_data.replace("NULL", pd.NA, inplace=True)

        if "opening_date" in store_data.columns:
            store_data["opening_date"] = pd.to_datetime(
                store_data["opening_date"], errors="coerce"
            )

        if "staff_number" in store_data.columns:
            store_data["staff_number"] = store_data["staff_number"].str.replace(
                r"\D", "", regex=True
            )

        print(f"Store data cleaned. Remaining rows: {len(store_data)}")
        return store_data

    def convert_weights_to_kg(self, products_data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the weights in the products DataFrame to kilograms (kg).

        Args:
            products_data (pd.DataFrame): DataFrame with a 'weight' column.

        Returns:
            pd.DataFrame: DataFrame with standardized weight values.
        """

        def parse_weight(weight: str) -> Optional[float]:
            weight = str(weight).lower().strip()
            if "kg" in weight:
                return float(weight.replace("kg", "").strip())
            elif "g" in weight:
                return float(weight.replace("g", "").strip()) / 1000
            elif "ml" in weight or "l" in weight:
                # Assuming 1:1 conversion for ml and l
                factor = 1 if "l" in weight else 1 / 1000
                return float(weight.replace("ml", "").replace("l", "").strip()) * factor
            return None

        if "weight" in products_data.columns:
            products_data["weight"] = products_data["weight"].apply(parse_weight)
        else:
            raise KeyError("The 'weight' column is missing.")

        print("Weights converted to kg.")
        return products_data

    def clean_orders_data(self, orders_data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove unnecessary columns from orders data.

        Args:
            orders_data (pd.DataFrame): DataFrame containing orders data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        columns_to_remove = ["first_name", "last_name", "1"]
        cleaned_data = orders_data.drop(columns=columns_to_remove, errors="ignore")
        print("Unnecessary columns removed from orders data.")
        return cleaned_data

    def clean_time_data(self, time_data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the order time table by removing NULL values and ensuring numeric columns.

        Args:
            time_data (pd.DataFrame): DataFrame containing order time data.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        time_data.replace("NULL", pd.NA, inplace=True)
        time_data.dropna(inplace=True)

        for col in ["day", "month", "year"]:
            if col in time_data.columns:
                time_data[col] = pd.to_numeric(time_data[col], errors="coerce")

        time_data.dropna(subset=["day", "month", "year"], inplace=True)
        print("Time data cleaned.")
        return time_data


if __name__ == "__main__":
    # Example usage
    cleaner = DataCleaning()
    example_df = pd.DataFrame({"weight": ["1kg", "500g", "2l"]})
    cleaned_data = cleaner.convert_weights_to_kg(example_df)
    print(cleaned_data)
