from CryoCore import API
from CryoCloud.Common.DockerProcess import DockerProcess


def process_task(worker, task):
    """
    worker.status and worker.log are ready here.

    Move files from one place to another
    Needs task["args"]["src"] and "dst"

    """

    gpu = False
    env = {}

    if "gpu" in task["args"] and task["args"]["gpu"]:
        gpu = True

    if "target" not in task["args"]:
        raise Exception("Missing docker target")

    target = task["args"]["target"]
    if target.__class__ != list:
        target = [target]

    if len(target) == 0:
        raise Exception("Require parameter 'target'")

    if "env" in task["args"]:
        env = task["args"]["env"]

    if "dirs" in task["args"]:
        dirs = task["args"]["dirs"]

    dp = DockerProcess(target, worker.status, worker.log, API.api_stop_event, dirs=dirs, env=env, gpu=gpu)
    dp.run()

    worker.log.debug("Docker completed")
    return worker.status["progress"].get_value(), None
