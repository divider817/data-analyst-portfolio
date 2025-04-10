import pandas as pd
import random
import numpy as np
import os
import logging
from datetime import datetime, timedelta

# -----------------------------
# CONFIGURATION PARAMETERS
# -----------------------------
# Seed for reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

NUM_CUSTOMERS = 500

# Overall order date range
OVERALL_START = datetime(2022, 1, 1)
OVERALL_END = datetime.now()  # current date

# Low-frequency window (fewer orders in this period)
LOW_START = datetime(2022, 2, 25)
LOW_END = datetime(2025, 5, 31)

# Poisson parameters for order generation:
# Poisson distribution is used to simulate the random number of orders per day.
# LAMBDA_HIGH is the average number of orders per day outside the low-frequency window,
# while LAMBDA_LOW is used within the low-frequency window.
LAMBDA_HIGH = 10  # average orders per day outside the low-frequency window
LAMBDA_LOW = 3    # average orders per day inside the low-frequency window

# File output folder
DATA_FOLDER = r"E:\github_repos\data-analyst-portfolio\showcase_local_coffee_shop\dataset_generation"

# Store weights for uneven distribution across stores
STORE_IDS = [1, 2, 3, 4, 5]
STORE_WEIGHTS = [0.3, 0.2, 0.25, 0.15, 0.1]

# -----------------------------
# LOGGING CONFIGURATION
# -----------------------------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def generate_customers():
    """
    Generates the customers table.
    Customers register from the business start date until the current date.
    """
    customer_levels = ["None", "3%", "5%", "7%", "10%"]
    customers = []
    customer_reg_start = OVERALL_START  # customers register from business start
    customer_reg_days = (OVERALL_END - OVERALL_START).days  # registration period
    
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        level_of_discount = random.choice(customer_levels)
        registration_date = (
            customer_reg_start + timedelta(days=random.randint(0, customer_reg_days))
        ).strftime("%Y-%m-%d")
        customers.append({
            "CustomerId": str(customer_id),
            "LevelOfDiscount": level_of_discount,
            "RegistrationDate": registration_date
        })
    logging.info("Generated customers table with %d entries.", len(customers))
    return pd.DataFrame(customers)


def generate_products():
    """
    Generates the products table.
    """
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
    """
    Generates the stores table for 5 Kyiv-based coffee shops.
    Each store includes address and geographic coordinates matching its district.
    """
    store_data = [
        {
            "StoreId": 1,
            "StoreName": "Brazil Coffee",
            "District": "Shevchenkivskyi",
            "City": "Kyiv",
            "Address": "1 Shevchenko St, Kyiv",
            "Latitude": 50.4501,
            "Longitude": 30.5234
        },
        {
            "StoreId": 2,
            "StoreName": "Colombia Coffee",
            "District": "Podilskyi",
            "City": "Kyiv",
            "Address": "5 Podil St, Kyiv",
            "Latitude": 50.4410,
            "Longitude": 30.5140
        },
        {
            "StoreId": 3,
            "StoreName": "Ethiopia Coffee",
            "District": "Pecherskyi",
            "City": "Kyiv",
            "Address": "10 Pechersk St, Kyiv",
            "Latitude": 50.4350,
            "Longitude": 30.5550
        },
        {
            "StoreId": 4,
            "StoreName": "Vietnam Coffee",
            "District": "Obolonskyi",
            "City": "Kyiv",
            "Address": "20 Obolon Ave, Kyiv",
            "Latitude": 50.4450,
            "Longitude": 30.4800
        },
        {
            "StoreId": 5,
            "StoreName": "Indonesia Coffee",
            "District": "Darnytskyi",
            "City": "Kyiv",
            "Address": "15 Darnytsia Rd, Kyiv",
            "Latitude": 50.4580,
            "Longitude": 30.5980
        }
    ]
    logging.info("Generated stores table with %d entries.", len(store_data))
    return pd.DataFrame(store_data)


def generate_orders(customers_df, products_df):
    """
    Generates the orders and order details tables using Poisson sampling for daily orders.
    Assigns each order to a store based on weighted random selection.
    Stores only the ProductId in order details.
    """
    orders = []
    order_details = []
    order_detail_id = 1

    current_day = OVERALL_START
    order_id = 1

    while current_day <= OVERALL_END:
        # Determine if current_day is in low-frequency window
        if LOW_START <= current_day <= LOW_END:
            daily_lambda = LAMBDA_LOW
        else:
            daily_lambda = LAMBDA_HIGH

        # Sample number of orders for current day using Poisson distribution
        orders_today = np.random.poisson(lam=daily_lambda)
        
        for _ in range(orders_today):
            order_date_str = current_day.strftime("%Y-%m-%d")
            order_type = random.choice(["In-store", "Takeaway"])
            # Assign customer only about 10% of the time
            customer_id = random.choice(customers_df["CustomerId"]) if random.random() > 0.9 else None
            # Assign store based on weighted random selection
            store_id = random.choices(STORE_IDS, weights=STORE_WEIGHTS, k=1)[0]

            # Generate a random number of items for this order (most orders have 1-3 items)
            num_items = random.choices(population=[1, 2, 3, 4, 5],
                                       weights=[0.4, 0.3, 0.2, 0.07, 0.03], k=1)[0]
            order_subtotal = 0.0

            for _ in range(num_items):
                product = random.choice(products_df.to_dict('records'))
                product_id = product["ProductId"]
                base_price = product["Price"]
                price = base_price  # no inflation adjustment
                # Generate quantity (most order details have quantity = 1)
                quantity = random.choices(population=[1, 2, 3],
                                          weights=[0.8, 0.15, 0.05], k=1)[0]
                line_total = price * quantity
                order_subtotal += line_total

                order_details.append({
                    "OrderDetailId": order_detail_id,
                    "OrderId": order_id,
                    "ProductId": product_id,
                    "Quantity": quantity
                })
                order_detail_id += 1

            # Determine discount based on customer information
            if customer_id is not None:
                customer_record = customers_df.loc[customers_df["CustomerId"] == customer_id].iloc[0]
                discount_str = customer_record["LevelOfDiscount"]
                discount_rate = 0 if discount_str == "None" else float(discount_str.strip('%')) / 100.0
            else:
                discount_rate = 0

            discount_amount = round(order_subtotal * discount_rate, 2)
            final_total = round(order_subtotal - discount_amount, 2)

            orders.append({
                "OrderId": order_id,
                "OrderDate": order_date_str,
                "OrderType": order_type,
                "CustomerId": customer_id,
                "StoreId": store_id,
                "SubTotal": round(order_subtotal, 2),
                "TotalAmount": final_total,
                "DiscountApplied": discount_rate > 0,
                "DiscountAmount": discount_amount
            })
            order_id += 1

        current_day += timedelta(days=1)

    logging.info("Generated orders table with %d orders.", order_id - 1)
    logging.info("Generated order details table with %d entries.", order_detail_id - 1)
    return pd.DataFrame(orders), pd.DataFrame(order_details)


def store_data(dfs, folder=DATA_FOLDER):
    """
    Saves the provided DataFrames to CSV files in the specified folder.
    Expects a dictionary with keys as filenames and values as DataFrames.
    """
    try:
        os.makedirs(folder, exist_ok=True)
        for filename, df in dfs.items():
            filepath = os.path.join(folder, filename)
            df.to_csv(filepath, index=False)
            logging.info("Saved %s", filepath)
    except Exception as e:
        logging.error("Error saving files: %s", e)
        raise


def main():
    """
    Main function to generate and store data.
    """
    logging.info("Data generation started.")

    customers_df = generate_customers()
    products_df = generate_products()
    stores_df = generate_stores()
    orders_df, order_details_df = generate_orders(customers_df, products_df)

    # Dictionary of DataFrames to store
    dfs = {
        "Orders.csv": orders_df,
        "Customers.csv": customers_df,
        "Products.csv": products_df,
        "OrderDetails.csv": order_details_df,
        "Stores.csv": stores_df
    }
    store_data(dfs)
    logging.info("Data generation completed successfully.")


if __name__ == "__main__":
    main()