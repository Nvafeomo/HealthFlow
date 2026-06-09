import requests
import os

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://community.cloud.databricks.com").rstrip("/")
VOLUME_PATH = "/Volumes/workspace/default/healthflow_staging"


def upload_to_volume(local_path: str, filename: str, token: str):
    """Upload a local file to the Unity Catalog Volume using the Files API."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{VOLUME_PATH}/{filename}"

    with open(local_path, "rb") as f:
        resp = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream"
            },
            data=f
        )
    resp.raise_for_status()
    print(f"Uploaded {local_path} to {VOLUME_PATH}/{filename}")


def download_from_volume(filename: str, local_path: str, token: str):
    """Download a file from the Unity Catalog Volume to local filesystem."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{VOLUME_PATH}/{filename}"

    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        stream=True
    )
    resp.raise_for_status()

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded {VOLUME_PATH}/{filename} to {local_path}")
