import pandas as pd
import random
from datetime import datetime, timedelta
import os

# STEP 1: IMPORT EXISTING DATA (OPTIONAL)
# Uncomment the following lines if you have existing datasets
# customers_df = pd.read_csv('customers.csv')  # Replace with your file path
# products_df = pd.read_csv('products.csv')   # Replace with your file path
# orders_df = pd.read_csv('orders.csv')       # Replace with your file path
# order_details_df = pd.read_csv('order_details.csv')  # Replace with your file path

# Parameters for data generation
num_orders = 1000
num_customers = 500
num_products = 20

# STEP 2: GENERATE CUSTOMERS TABLE
customer_levels = ["None", "3%", "5%", "7%", "10%"]
customers = []
for customer_id in range(1, num_customers + 1):
    level_of_discount = random.choice(customer_levels)
    registration_date = (
        datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1095))
    ).strftime("%Y-%m-%d")  # Dates between 2020 and 2022
    customers.append({
        "CustomerId": str(customer_id),
        "LevelOfDiscount": level_of_discount,
        "RegistrationDate": registration_date
    })
customers_df = pd.DataFrame(customers)

# STEP 3: GENERATE PRODUCTS TABLE
product_data = [
    {"ProductName": "Espresso", "ProductCategory": "Beverage", "PriceRange": (2.5, 4.0)},
    {"ProductName": "Latte", "ProductCategory": "Beverage", "PriceRange": (3.5, 5.5)},
    {"ProductName": "Cappuccino", "ProductCategory": "Beverage", "PriceRange": (3.5, 5.0)},
    {"ProductName": "Americano", "ProductCategory": "Beverage", "PriceRange": (2.5, 4.5)},
    {"ProductName": "Flat White", "ProductCategory": "Beverage", "PriceRange": (3.5, 5.0)},
    {"ProductName": "Mocha", "ProductCategory": "Beverage", "PriceRange": (4.0, 6.0)},
    {"ProductName": "Cold Brew", "ProductCategory": "Beverage", "PriceRange": (3.0, 5.0)},
    {"ProductName": "Iced Latte", "ProductCategory": "Beverage", "PriceRange": (3.5, 5.5)},
    {"ProductName": "Croissant", "ProductCategory": "Pastry", "PriceRange": (2.5, 4.0)},
    {"ProductName": "Blueberry Muffin", "ProductCategory": "Pastry", "PriceRange": (3.0, 4.5)},
    {"ProductName": "Chocolate Chip Cookie", "ProductCategory": "Snack", "PriceRange": (2.0, 3.5)},
    {"ProductName": "Cheesecake Slice", "ProductCategory": "Pastry", "PriceRange": (4.0, 6.0)},
    {"ProductName": "Bagel with Cream Cheese", "ProductCategory": "Snack", "PriceRange": (3.0, 4.5)},
    {"ProductName": "Ham & Cheese Sandwich", "ProductCategory": "Snack", "PriceRange": (4.0, 6.5)},
    {"ProductName": "Granola Bar", "ProductCategory": "Snack", "PriceRange": (2.0, 3.5)},
]
products = []
for product_id, product in enumerate(product_data, start=1):
    products.append({
        "ProductId": product_id,
        "ProductName": product["ProductName"],
        "ProductCategory": product["ProductCategory"],
        "PriceMin": product["PriceRange"][0],
        "PriceMax": product["PriceRange"][1]
    })
products_df = pd.DataFrame(products)

# STEP 4: GENERATE ORDERS TABLE
order_types = ["In-store", "Takeaway"]
discount_rates = [0, 0.03, 0.05, 0.07, 0.1]
tip_amounts = [0, 1, 2, 3, 5]

orders = []
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

for order_id in range(1, num_orders + 1):
    order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    order_type = random.choice(order_types)
    customer_id = random.choice(customers_df["CustomerId"]) if random.random() > 0.5 else None
    total_amount = round(random.uniform(5.0, 50.0), 2)
    discount_applied = random.choice(discount_rates) if random.random() > 0.7 else 0
    discount_amount = round(total_amount * discount_applied, 2)
    tip_amount = random.choice(tip_amounts) if random.random() > 0.6 else 0
    orders.append({
        "OrderId": order_id,
        "OrderDate": order_date.strftime("%Y-%m-%d"),
        "TotalAmount": total_amount,
        "OrderType": order_type,
        "CustomerId": customer_id if customer_id else "No Customer",
        "DiscountApplied": discount_applied > 0,
        "DiscountAmount": discount_amount,
        "TipAmount": tip_amount
    })
orders_df = pd.DataFrame(orders)

# STEP 5: GENERATE ORDER DETAILS TABLE
order_details = []
for _, order in orders_df.iterrows():
    num_items = random.randint(1, 5)  # Random number of items per order
    for _ in range(num_items):
        product = random.choice(products)
        product_name = product["ProductName"]
        product_category = product["ProductCategory"]
        price = round(random.uniform(product["PriceMin"], product["PriceMax"]), 2)
        quantity = random.randint(1, 3)
        order_details.append({
            "OrderDetailId": len(order_details) + 1,
            "OrderId": order["OrderId"],
            "ProductName": product_name,
            "ProductCategory": product_category,
            "Quantity": quantity,
            "Price": price
        })
order_details_df = pd.DataFrame(order_details)

# STEP 6: Store data

# Define the folder path
folder_path = "E:\github_repos\data-analyst-portfolio\showcase_local_coffee_shop\dataset_generation"

# Create the folder if it doesn't exist
os.makedirs(folder_path, exist_ok=True)

# Save DataFrames as CSV files
orders_df.to_csv(f"{folder_path}/Orders.csv", index=False)
customers_df.to_csv(f"{folder_path}/Customers.csv", index=False)
products_df.to_csv(f"{folder_path}/Products.csv", index=False)
order_details_df.to_csv(f"{folder_path}/OrderDetails.csv", index=False)

print(f"Files saved in: {folder_path}")
