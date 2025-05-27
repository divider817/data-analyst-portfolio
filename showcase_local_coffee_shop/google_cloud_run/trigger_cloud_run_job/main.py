import functions_framework
import google.auth
from google.auth.transport.requests import Request
import requests
import os

@functions_framework.http
def trigger_job(request):
    project_id = os.environ["PROJECT_ID"]
    job_name = os.environ["JOB_NAME"]
    region = os.environ["REGION"]

    url = f"https://{region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/{project_id}/jobs/{job_name}:run"

    # Get default credentials (uses Cloud Function's service account)
    credentials, _ = google.auth.default()
    auth_req = Request()
    credentials.refresh(auth_req)
    id_token = credentials.token

    headers = {
        "Authorization": f"Bearer {id_token}"
    }

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        return f"✅ Job {job_name} triggered successfully.", 200
    else:
        return f"❌ Failed to trigger job: {response.status_code} - {response.text}", 500
