import tempfile
import os

from flask import g
from piedpiper.gman import client as gman_client
from piedpiper.artman import artman_client
from piedpiper.storeman.client import storage_client
from piedpiper.sri import generate_sri

from .util import unzip_files, read_secrets, gman_activate
from .config import Config

gman_url = Config["gman"]["url"]
storage_url = Config["storage"]["url"]
function_name = Config["name"]
function_executor = f"{Config['name']}_executor"
executor_url = Config["executor_url"]


@gman_activate(status="received")
def handle(request):
    """
    Noop executor function definition. This handler function controls
    all functionality
    :param request: The request object from Flask
    This object is required to have the following JSON parameters:
    * run_id: The run_id of the task
    * thread_id: The thread_id, from gman, that this execution is forked from.
    * project: The project name of the run
    * configs: A list containing the configuration dictionaries for the run.
    * stage: The stage that is being run.
    * artifacts: A list of dictionaries containing information on the artifacts
    required for this run
    :param request:
    :return:
    """
    run_id = request.get_json().get("run_id")
    stage = request.get_json()["stage"]
    configs = request.get_json()["configs"]
    artifacts = request.get_json()["artifacts"]
    task = g.task

    access_key = read_secrets().get("access_key")
    secret_key = read_secrets().get("secret_key")

    minio_client = storage_client(
        "minio", hostname=storage_url, access_key=access_key, secret_key=secret_key
    )

    with tempfile.TemporaryDirectory() as temp_directory:
        for art_name, art_data in artifacts.items():
            minio_client.download_file(
                art_data["artifact_uri"], os.path.join(temp_directory, art_name)
            )
            unzip_files(f"{temp_directory}/{art_name}", temp_directory)
        os.chdir(temp_directory)
        log_file = f"{temp_directory}/noop.log"
        with open(log_file, "w") as f:
            f.write(
                f"Noop performed for stage {stage} with the following configs {configs}"
            )

        minio_client.upload_file(run_id, f"artifacts/logs/{stage}/noop.log", log_file)
        project_artifact_hash = generate_sri(log_file)
        artifact_uri = f"minio://{storage_url}/{run_id}/logs/{stage}/noop.log"
        artman_client.post_artifact(
            task_id=task["task"]["task_id"],
            artman_url=gman_url,
            uri=artifact_uri,
            caller=function_executor,
            sri=str(project_artifact_hash),
        )

        gman_client.update_task_id(
            gman_url=gman_url,
            task_id=task["task"]["task_id"],
            status="info",
            message="Uploaded log artifact",
        )
