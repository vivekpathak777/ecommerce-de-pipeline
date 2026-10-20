import csv
import random
from pathlib import Path

random.seed(42)

# Create a data folder in the project root
data_dir = Path(__file__).resolve().parent.parent / "data"
data_dir.mkdir(exist_ok=True)

# Generate 100 fictional customers
with open(data_dir / "customers.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["customer_id", "name", "city", "email"])

    for i in range(1, 101):
        writer.writerow([
            i,
            f"Customer_{i}",
            random.choice(["Mumbai", "Delhi", "Pune", "Bangalore"]),
            f"customer{i}@example.com"
        ])

print("Successfully generated 100 customers!")


# Generate 50 products
categories = {
    "Electronics": ["Laptop", "Phone", "Headphones", "Monitor"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Shoes"],
    "Home": ["Chair", "Table", "Lamp", "Bedsheet"]
}

with open(data_dir / "products.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["product_id", "product_name", "category", "price"])

    for i in range(1, 51):
        category = random.choice(list(categories.keys()))
        product_name = random.choice(categories[category])

        writer.writerow([
            i,
            f"{product_name}_{i}",
            category,
            random.randint(200, 50000)
        ])

print("Successfully generated 50 products!")


from datetime import datetime, timedelta

# Generate 500 orders
start_date = datetime(2026, 1, 1)

with open(data_dir / "orders.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([
        "order_id", "customer_id", "product_id",
        "quantity", "order_date", "status"
    ])

    for i in range(1, 501):
        order_date = start_date + timedelta(
            days=random.randint(0, 270)
        )

        writer.writerow([
            i,
            random.randint(1, 100),  # Existing customer
            random.randint(1, 50),   # Existing product
            random.randint(1, 5),
            order_date.strftime("%Y-%m-%d"),
            random.choice(["Completed", "Pending", "Cancelled"])
        ])

print("Successfully generated 500 orders!")


# Generate payments based on existing orders
with open(data_dir / "orders.csv", "r", newline="") as file:
    orders = list(csv.DictReader(file))  # Read orders as dictionaries

payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking"]

with open(data_dir / "payments.csv", "w", newline="") as file:
    writer = csv.writer(file)

    # Define payment table columns
    writer.writerow([
        "payment_id", "order_id", "payment_method",
        "payment_status", "payment_date"
    ])

    # Create one payment record for each order
    for i, order in enumerate(orders, start=1):

        # Map order status to corresponding payment status
        status_mapping = {
            "Completed": "Success",
            "Pending": "Pending",
            "Cancelled": "Refunded"
        }

        payment_status = status_mapping[order["status"]]

        writer.writerow([
            i,
            order["order_id"],  # Foreign key referencing orders.csv
            random.choice(payment_methods),
            payment_status,
            order["order_date"]
        ])

print("Successfully generated 500 payments!")