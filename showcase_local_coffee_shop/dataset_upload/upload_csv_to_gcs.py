import os
from google.cloud import storage

# Configure
BUCKET_NAME = "coffee-shop-showcase" 
folder_path = "E:\github_repos\data-analyst-portfolio\showcase_local_coffee_shop\dataset_generation"
GCS_FOLDER = "csv_sources/"  # Folder inside the bucket

def upload_files():
    """Uploads all CSVs from a local folder to a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            local_path = os.path.join(folder_path, file_name)
            gcs_path = os.path.join(GCS_FOLDER, file_name)

            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)

            print(f"Uploaded {file_name} to gs://{BUCKET_NAME}/{gcs_path}")

if __name__ == "__main__":
    upload_files()
