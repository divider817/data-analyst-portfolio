import os
import logging
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage

# ----- Configuration -----
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

NUM_CUSTOMERS = 500
OVERALL_START = datetime(2022, 1, 1)
OVERALL_END = datetime.now()
LOW_START = datetime(2022, 2, 25)
LOW_END = datetime(2022, 5, 31)
LAMBDA_HIGH = 10
LAMBDA_LOW = 3

LOCAL_FOLDER = "data_output"
BUCKET_NAME = "coffee-shop-showcase"
GCS_FOLDER = "csv_sources/"
STORE_IDS = [1, 2, 3, 4, 5]
STORE_WEIGHTS = [0.3, 0.2, 0.25, 0.15, 0.1]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----- Data Generation Functions -----
def generate_customers():
    customer_levels = ["None", "3%", "5%", "7%", "10%"]
    customers = []
    customer_reg_days = (OVERALL_END - OVERALL_START).days
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        level = random.choice(customer_levels)
        reg_date = (
            OVERALL_START + timedelta(days=random.randint(0, customer_reg_days))
        ).strftime("%Y-%m-%d")
        customers.append({
            "CustomerId": str(customer_id),
            "LevelOfDiscount": level,
            "RegistrationDate": reg_date
        })
    logging.info("Generated customers table with %d entries.", len(customers))
    return pd.DataFrame(customers)

def generate_products():
    product_data = [
        {"ProductName": "Espresso", "ProductCategory": "Beverage", "Price": 2.5},
        {"ProductName": "Latte", "ProductCategory": "Beverage", "Price": 4.0},
        {"ProductName": "Cappuccino", "ProductCategory": "Beverage", "Price": 4.5},
        {"ProductName": "Americano", "ProductCategory": "Beverage", "Price": 3.0},
        {"ProductName": "Flat White", "ProductCategory": "Beverage", "Price": 3.5},
        {"ProductName": "Matcha", "ProductCategory": "Beverage", "Price": 4.0},
        {"ProductName": "Cold Brew", "ProductCategory": "Beverage", "Price": 4.0},
        {"ProductName": "Espresso Tonic", "ProductCategory": "Beverage", "Price": 4.5},
        {"ProductName": "Croissant", "ProductCategory": "Pastry", "Price": 3.0},
        {"ProductName": "Blueberry Muffin", "ProductCategory": "Pastry", "Price": 2.0},
        {"ProductName": "Chocolate Chip Cookie", "ProductCategory": "Pastry", "Price": 2.0},
        {"ProductName": "Cheesecake Slice", "ProductCategory": "Pastry", "Price": 4.0},
        {"ProductName": "Bagel with Cream Cheese", "ProductCategory": "Savory", "Price": 3.0},
        {"ProductName": "Ham & Cheese Sandwich", "ProductCategory": "Savory", "Price": 4.0},
        {"ProductName": "Chicken Sandwich", "ProductCategory": "Savory", "Price": 4.0},
    ]
    products = []
    for product_id, product in enumerate(product_data, start=1):
        products.append({
            "ProductId": product_id,
            "ProductName": product["ProductName"],
            "ProductCategory": product["ProductCategory"],
            "Price": product["Price"]
        })
    logging.info("Generated products table with %d entries.", len(products))
    return pd.DataFrame(products)

def generate_stores():
    store_data = [
        {"StoreId": 1, "StoreName": "Brazil Coffee", "District": "Shevchenkivskyi", "City": "Kyiv",
         "Address": "1 Shevchenko St, Kyiv", "Latitude": 50.4501, "Longitude": 30.5234},
        {"StoreId": 2, "StoreName": "Colombia Coffee", "District": "Podilskyi", "City": "Kyiv",
         "Address": "5 Podil St, Kyiv", "Latitude": 50.4410, "Longitude": 30.5140},
        {"StoreId": 3, "StoreName": "Ethiopia Coffee", "District": "Pecherskyi", "City": "Kyiv",
         "Address": "10 Pechersk St, Kyiv", "Latitude": 50.4350, "Longitude": 30.5550},
        {"StoreId": 4, "StoreName": "Vietnam Coffee", "District": "Obolonskyi", "City": "Kyiv",
         "Address": "20 Obolon Ave, Kyiv", "Latitude": 50.4450, "Longitude": 30.4800},
        {"StoreId": 5, "StoreName": "Indonesia Coffee", "District": "Darnytskyi", "City": "Kyiv",
         "Address": "15 Darnytsia Rd, Kyiv", "Latitude": 50.4580, "Longitude": 30.5980}
    ]
    logging.info("Generated stores table with %d entries.", len(store_data))
    return pd.DataFrame(store_data)

def generate_orders(customers_df, products_df):
    orders = []
    order_details = []
    order_detail_id = 1
    order_id = 1
    current_day = OVERALL_START

    while current_day <= OVERALL_END:
        daily_lambda = LAMBDA_LOW if LOW_START <= current_day <= LOW_END else LAMBDA_HIGH
        orders_today = np.random.poisson(lam=daily_lambda)
        for _ in range(orders_today):
            order_date = current_day.strftime("%Y-%m-%d")
            order_type = random.choice(["In-store", "Takeaway"])
            customer_id = random.choice(customers_df["CustomerId"]) if random.random() > 0.85 else None
            store_id = random.choices(STORE_IDS, weights=STORE_WEIGHTS, k=1)[0]
            num_items = random.choices([1, 2, 3, 4, 5], [0.4, 0.3, 0.2, 0.07, 0.03])[0]
            order_subtotal = 0.0

            for _ in range(num_items):
                product = random.choice(products_df.to_dict("records"))
                quantity = random.choices([1, 2, 3], [0.8, 0.15, 0.05])[0]
                line_total = product["Price"] * quantity
                order_subtotal += line_total

                order_details.append({
                    "OrderDetailId": order_detail_id,
                    "OrderId": order_id,
                    "ProductId": product["ProductId"],
                    "Quantity": quantity
                })
                order_detail_id += 1

            discount_rate = 0.0
            if customer_id:
                discount_str = customers_df.loc[customers_df["CustomerId"] == customer_id].iloc[0]["LevelOfDiscount"]
                discount_rate = float(discount_str.strip('%')) / 100.0 if discount_str != "None" else 0.0

            discount_amt = round(order_subtotal * discount_rate, 2)
            total = round(order_subtotal - discount_amt, 2)

            orders.append({
                "OrderId": order_id,
                "OrderDate": order_date,
                "OrderType": order_type,
                "CustomerId": customer_id,
                "StoreId": store_id,
                "SubTotal": round(order_subtotal, 2),
                "TotalAmount": total,
                "DiscountApplied": discount_rate > 0,
                "DiscountAmount": discount_amt
            })
            order_id += 1
        current_day += timedelta(days=1)

    logging.info("Generated orders table with %d orders.", order_id - 1)
    logging.info("Generated order details table with %d entries.", order_detail_id - 1)
    return pd.DataFrame(orders), pd.DataFrame(order_details)

def store_data(dfs, folder=LOCAL_FOLDER):
    os.makedirs(folder, exist_ok=True)
    for filename, df in dfs.items():
        filepath = os.path.join(folder, filename)
        df.to_csv(filepath, index=False)
        logging.info("Saved %s", filepath)

def upload_to_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".csv"):
            local_path = os.path.join(LOCAL_FOLDER, file_name)
            gcs_path = os.path.join(GCS_FOLDER, file_name)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            logging.info(f"Uploaded {file_name} to gs://{BUCKET_NAME}/{gcs_path}")

def main():
    logging.info("Starting data generation and upload pipeline...")
    customers_df = generate_customers()
    products_df = generate_products()
    stores_df = generate_stores()
    orders_df, order_details_df = generate_orders(customers_df, products_df)
    dfs = {
        "Orders.csv": orders_df,
        "Customers.csv": customers_df,
        "Products.csv": products_df,
        "OrderDetails.csv": order_details_df,
        "Stores.csv": stores_df,
    }
    store_data(dfs)
    upload_to_gcs()
    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()