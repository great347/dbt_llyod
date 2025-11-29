from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def load_csv_from_gcs_to_bigquery(
    project_id,
    dataset_id,
    table_id,
    gcs_uri,
    table_schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
):

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  
        write_disposition=write_disposition,
    )

    try:
        # Start the load job
        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        print(f"load the table '{table_id}' using URI: {gcs_uri}")
        load_job.result() 
        print(f"Successfully loaded data into {project_id}.{dataset_id}.{table_id}.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")

# --- BIGQuery Configuration ---

PROJECT_ID = "int-dev-001"
DATASET_ID = "dbt_dataset"

# cloud storage bucket path
GCS_BASE_PATH_API = "llyod_bucket1234/api" 
GCS_BASE_PATH_SQLSERVER = "llyod_bucket1234/sqlserver" 

# --- Customer Data ---
customer_table_id = "customer_raw"
# Construct the full URI, including the subfolder and file name
customer_gcs_uri = f"gs://{GCS_BASE_PATH_SQLSERVER}customer.csv"
customer_schema = [
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("full_name", "STRING"),
    bigquery.SchemaField("customer_postcode", "STRING"),
    bigquery.SchemaField("address_city", "STRING"),
    bigquery.SchemaField("address_region", "STRING"),
    bigquery.SchemaField("last_update_date", "DATE"),
]

# --- Transaction Data ---
transaction_table_id = "transaction_raw"
# Construct the full URI, including the subfolder and file name
transaction_gcs_uri = f"gs://{GCS_BASE_PATH_API}transaction.csv"
transaction_schema = [
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("consumer_id", "STRING"),
    bigquery.SchemaField("transaction_type", "STRING"),
    bigquery.SchemaField("transaction_update_date", "DATE"),
    bigquery.SchemaField("transaction_created_at", "DATE"),
    bigquery.SchemaField("transaction_payment_value", "BIGNUMERIC"),
    bigquery.SchemaField("transaction_update_date", "DATE"),
]

# --- Main Execution ---
if __name__ == "__main__":
    # Load Customer data
    load_csv_from_gcs_to_bigquery(
        PROJECT_ID,
        DATASET_ID,
        customer_table_id,
        customer_gcs_uri,
        customer_schema
    )

    print("-" * 30)

    # Load Transaction data
    load_csv_from_gcs_to_bigquery(
        PROJECT_ID,
        DATASET_ID,
        transaction_table_id,
        transaction_gcs_uri,
        transaction_schema
    )
