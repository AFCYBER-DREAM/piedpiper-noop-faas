import zipfile
import requests
import json
from minio import Minio
from .config import Config

gman_url = Config.get("gman").get("url")


def read_secrets():
    secrets = {}
    with open("/var/openfaas/secrets/storage-access-key") as access_key:
        secrets.update({"access_key": access_key.read().strip("\n")})
    with open("/var/openfaas/secrets/storage-secret-key") as secret_key:
        secrets.update({"secret_key": secret_key.read().strip("\n")})

    return secrets


def upload_artifact(bucket_name, object_name, file_path, url, access_key, secret_key):
    minioClient = Minio(url, access_key=access_key, secret_key=secret_key, secure=False)

    minioClient.fput_object(bucket_name, object_name, file_path)
    return minioClient.stat_object(bucket_name, object_name)


def download_artifact(bucket_name, object_name, file_path, url, access_key, secret_key):
    minioClient = Minio(url, access_key=access_key, secret_key=secret_key, secure=False)
    return minioClient.fget_object(bucket_name, object_name, file_path)


def unzip_files(zip_file, directory):
    zip_ref = zipfile.ZipFile(zip_file, "r")
    zip_ref.extractall(directory)
    zip_ref.close()


def query_gman_for_task(task_id):
    r = requests.get(f"{gman_url}/gman/{task_id}")
    r.raise_for_status()
    return r.json()


def update_task_id_status(
    gman_url=None, task_id=None, status=None, message=None, caller=None
):
    try:
        data = {"message": message, "status": status, "caller": caller}
        r = requests.put(f"{gman_url}/gman/{task_id}", data=json.dumps(data))
    except requests.exceptions.RequestException:
        raise

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        raise
    else:
        id = r.json()["task"]["task_id"]
        return id
