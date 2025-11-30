import random
from faker import Faker
from datetime import datetime, timedelta
import csv

# Initialize Faker with a locale that supports relevant addresses (e.g., 'en_GB')
fake = Faker('en_GB')

def generate_customer_data(num_customers):
    """Generates a list of dictionaries containing synthetic customer data."""
    customers = []
    customer_ids = list(range(1, num_customers + 1))
    random.shuffle(customer_ids)

    # Define a time range for realistic update dates (e.g., within the last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    for i in range(num_customers):
        # Generate a random datetime object within the range
        update_datetime_obj = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Format the datetime object as a string: YYYY-MM-DD HH:MM:SS
        formatted_date_string = update_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

        customer = {
            'customer_id': customer_ids[i],
            'full_name': fake.name(),
            'post_code': fake.postcode(),
            'city': fake.city(),
            'region': fake.county(),
            'last_update_date': formatted_date_string
        }
        customers.append(customer)
    return customers

def save_to_csv_with_header(data, filename="customers_final.csv"):
    """Saves the generated data to a CSV file WITH a header."""
    fieldnames = ['customer_id', 'full_name', 'post_code', 'city', 'region', 'last_update_date']
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # --- Writes the header row ---
        writer.writeheader() 
        
        writer.writerows(data)
    print(f"Generated {len(data)} customer records and saved to {filename}")

# Generate exactly 50 customers
num_of_customers_to_generate = 50
customer_data = generate_customer_data(num_of_customers_to_generate)

# Save the data to a new CSV file named customers_final.csv
save_to_csv_with_header(customer_data)








