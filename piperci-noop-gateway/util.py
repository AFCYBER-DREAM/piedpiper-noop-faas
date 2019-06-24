from .config import Config

from flask import g, request
import traceback

from piperci.gman import client as gman_client


def gman_activate(status):
    def decorator_func(func):
        def wrapper_func(*args, **kwargs):
            gman_url = Config["gman"]["url"]
            function_name = f"{Config['name']}"
            run_id = request.get_json().get("run_id")
            project = request.get_json().get("project")

            task = gman_client.request_new_task_id(
                run_id=run_id,
                gman_url=gman_url,
                status=status,
                project=project,
                caller=function_name,
            )
            g.task = task
            try:
                func(*args, **kwargs)
                gman_client.update_task_id(
                    gman_url=gman_url,
                    task_id=task["task"]["task_id"],
                    status="completed",
                    message=f"{function_name} completed successfully.",
                )
                return task
            except Exception:
                message = traceback.format_exc()
                gman_client.update_task_id(
                    gman_url=gman_url,
                    status="failed",
                    task_id=task["task"]["task_id"],
                    message=f"Failed to execute {function_name}. Exception: {message}",
                )
                return task

        return wrapper_func

    return decorator_func


def gman_delegate(r, *args, **kwargs):
    gman_url = Config["gman"]["url"]
    if r.status_code == 202:
        gman_client.update_task_id(
            gman_url=gman_url,
            task_id=g.task["task"]["task_id"],
            status="delegated",
            message=f"Delegated execution to {r.url}",
        )
    else:
        gman_client.update_task_id(
            gman_url=gman_url,
            task_id=g.task["task"]["task_id"],
            status="failed",
            message=f"Failed to delegate execution to {r.url}",
        )
    return r
