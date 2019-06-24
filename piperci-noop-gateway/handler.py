from .config import Config
from .util import gman_activate, gman_delegate
from flask import g

import json
import requests


@gman_activate(status="started")
def handle(request):
    """
    Entrypoint to the noop FaaS.
    :param request: The request object from Flask
    This object is required to have the following JSON parameters:
    * run_id: The run_id of the task
    * project: The project name of the run
    * configs: A list containing the configuration dictionaries for the run.
    * stage: The stage that is being run.
    * artifacts: A list of dictionaries containing information on the artifacts
    required for this run
    :return:
    """
    executor_url = Config["executor_url"]

    run_id = request.get_json().get("run_id")
    project = request.get_json().get("project")
    configs = request.get_json().get("configs")
    stage = request.get_json().get("stage")
    artifacts = request.get_json().get("artifacts")
    task = g.task

    data = {
        "run_id": run_id,
        "thread_id": task["task"]["thread_id"],
        "project": project,
        "configs": configs,
        "stage": stage,
        "artifacts": artifacts,
    }

    headers = {"Content-Type": "application/json"}

    requests.post(
        executor_url,
        data=json.dumps(data),
        headers=headers,
        hooks={"response": gman_delegate},
    )
