import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import itertools

# Provided list of numeric customer IDs
customer_ids_input = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50
]

# Define date range and transaction types
start_date = datetime(2025, 6, 10)
end_date = datetime(2025, 10, 20)
transaction_types = ['mortgage', 'withdrawal', 'fee', 'deposit', 'direct_debit']

# Counter for numeric transaction IDs
transaction_id_counter = itertools.count(100000) # Start IDs from 100000

# Generate transaction data
transactions = []
current_date = start_date
while current_date <= end_date:
    for customer_id in customer_ids_input:
        # Generate a random number of transactions (0 to 3) for each customer on this day
        num_transactions_for_customer = random.randint(0, 3) 
        for _ in range(num_transactions_for_customer):
            transaction_id = next(transaction_id_counter)
            transaction_type = random.choice(transaction_types)
            
            # Adjust transaction value based on type for more realistic data
            if transaction_type == 'mortgage':
                transaction_amount = round(random.uniform(500.0, 5000.0), 2)
            elif transaction_type == 'withdrawal':
                transaction_amount = round(random.uniform(10.0, 500.0), 2)
            elif transaction_type == 'deposit':
                transaction_amount = round(random.uniform(20.0, 1000.0), 2)
            elif transaction_type == 'fee':
                transaction_amount = round(random.uniform(1.0, 50.0), 2)
            elif transaction_type == 'direct_debit':
                transaction_amount = round(random.uniform(50.0, 500.0), 2)
            
            transaction_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
            
            transactions.append({
                'transaction_id': transaction_id,
                'consumer_id': customer_id,
                'transaction_created_at': transaction_date_str,
                'transaction_update_date': transaction_date_str, # Using creation date as initial update date
                'transaction_type': transaction_type,
                'transaction_payment_value': transaction_amount
            })
    current_date += timedelta(days=1)

# Create a Pandas DataFrame and export to CSV
df = pd.DataFrame(transactions)
df.to_csv('customer_transactions_linked_numeric.csv', index=False)

print("customer_transactions_linked_numeric.csv file has been generated with random transaction data.")
print(f"Total transactions generated: {len(df)}")
print(df.head())

