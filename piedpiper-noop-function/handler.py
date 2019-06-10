import tempfile
import os

from .util import (
    unzip_files,
    update_task_id_status,
    upload_artifact,
    download_artifact,
    read_secrets,
)
from .config import Config

gman_url = Config["gman"]["url"]
storage_url = Config["storage"]["url"]


def handle(request):
    """handle a request to the function
    Args:
        req (str): request body
    """
    run_id = request.get_json().get("run_id")
    task_id = request.get_json()["task_id"]
    project = request.get_json()["project"]
    stage = request.get_json()["stage"]

    access_key = read_secrets().get("access_key")
    secret_key = read_secrets().get("secret_key")

    update_task_id_status(
        gman_url=gman_url,
        status="received",
        task_id=task_id,
        message="Received execution task from noop gateway",
        caller="noop_func",
    )

    with tempfile.TemporaryDirectory() as temp_directory:
        download_artifact(
            run_id,
            f"artifacts/{project}.zip",
            f"{temp_directory}/{project}.zip",
            storage_url,
            access_key,
            secret_key,
        )

        unzip_files(f"{temp_directory}/{project}.zip", temp_directory)
        os.chdir(temp_directory)
        log_file = f"{temp_directory}/noop.log"
        with open(log_file, "w") as f:
            f.write(f"Noop performed for stage {stage}!")

        upload_artifact(
            run_id,
            f"artifacts/logs/{stage}/noop.log",
            log_file,
            storage_url,
            access_key,
            secret_key,
        )
        update_task_id_status(
            gman_url=gman_url,
            task_id=task_id,
            status="completed",
            message="Flake8 execution complete",
            caller="noop_func",
        )

    return 200
