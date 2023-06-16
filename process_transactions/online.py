
import pandas as pd
from datetime import datetime

from practical_data_engineering.io.extract import extract_table
from practical_data_engineering.io.load import load_dataframe
from practical_data_engineering.utils import get_lookup_fn, hash_id
from practical_data_engineering.constants import TAX_RATE

def break_down_total(row, name_to_price):
    net = row["amount"] / (1 + TAX_RATE)
    unit_price = name_to_price(row["description"])
    quantity = int(net / unit_price)
    tax = round(row["amount"] * TAX_RATE, 2)

    return unit_price, quantity, tax

def transform_online_transactions(df, products_df=None):
    if products_df is None:
        products_df = extract_table("products")

    name_to_sku = get_lookup_fn(products_df, from_col="name", to_col="sku")
    name_to_price = get_lookup_fn(products_df, from_col="name", to_col="unit_price")

    transactions = []
    for i, row in df.iterrows():
        data = row["stripe_data"]
        
        if pd.isna(data):
            continue

        unit_price, quantity, tax = break_down_total(data, name_to_price)
        transactions.append(
            {
                "transaction_id": hash_id(data["id"]),
                "created_at": datetime.utcfromtimestamp(data["created"]),
                "location": "online",
                "product_name": data["description"],
                "sku": name_to_sku(data["description"]),
                "source": "online",
                "payment_method": data["object"],
                "unit_price": unit_price,
                "quantity": quantity,
                "tax": tax,
                "total": data["amount"]
            }
        )

    return pd.DataFrame(transactions)

def process_online_transactions():
    df = extract_table("online_transactions")
    df = transform_online_transactions(df)
    load_dataframe(df)
    #E: Extract
    #T: Transform
    #L: Load