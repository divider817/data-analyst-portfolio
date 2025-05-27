from google.cloud import bigquery
import functions_framework
import os

@functions_framework.http
def load_csvs(request):
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    bucket = os.environ["BUCKET"]
    gcs_prefix = os.environ["GCS_PREFIX"]

    tables = {
        "Customers.csv": "customers",
        "Orders.csv": "orders",
        "Products.csv": "products",
        "OrderDetails.csv": "order_details",
        "Stores.csv": "stores"
    }

    client = bigquery.Client(project=project)

    for filename, table in tables.items():
        uri = f"gs://{bucket}/{gcs_prefix}{filename}"
        table_id = f"{project}.{dataset}.{table}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {filename} into {table_id}")

    return "All tables loaded.", 200
