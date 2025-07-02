import pandas as pd
from faker import Faker
import numpy as np
import os

print("Generating a large dummy dataset...")

# Initialize Faker for generating fake data
fake = Faker()

# Configuration
NUM_ROWS = 10_000_000
FILENAME = "large_transactions.parquet"

if os.path.exists(FILENAME):
    print(f"'{FILENAME}' already exists. Skipping generation.")
else:
    # Create data dictionary
    data = {
        'transaction_id': range(NUM_ROWS),
        'product_id': np.random.randint(1000, 2000, size=NUM_ROWS),
        'customer_id': np.random.randint(10000, 20000, size=NUM_ROWS),
        'transaction_date': pd.to_datetime([fake.date_time_this_decade() for _ in range(NUM_ROWS)]),
        'quantity': np.random.randint(1, 6, size=NUM_ROWS),
        'price_per_item': np.round(np.random.uniform(5.50, 500.99, size=NUM_ROWS), 2),
        'store_location': np.random.choice(['New York', 'London', 'Online', 'Tokyo', 'Sydney'], size=NUM_ROWS, p=[0.3, 0.2, 0.3, 0.1, 0.1]),
        'product_category': np.random.choice(['Electronics', 'Apparel', 'Groceries', 'Books', 'Home Goods'], size=NUM_ROWS, p=[0.25, 0.25, 0.2, 0.15, 0.15])
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to Parquet format for efficiency
    df.to_parquet(FILENAME, engine='pyarrow')

    print(f"Successfully generated '{FILENAME}' with {NUM_ROWS} rows.")