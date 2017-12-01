
from __future__ import print_function
import CryoCloud


description = """
Run docker
"""

supress = ["-o", "-r", "-f", "-t", "--steps", "--module"]


# We have some additional arguments we'd like
def addArguments(parser):
    print("Adding arguments")
    # parser.add_argument('parameters', type=str, nargs='+', help='Parameters to list/modify').completer = completer

    # parser.add_argument("target", type=str, nargs='1', help="Docker")
    parser.add_argument("--args", dest="args", default=None, help="Arguments to send to the docker (use '')")

    parser.add_argument("-d", "--docker",
                        dest="target",
                        default=None,
                        help="Target docker to run")

    parser.add_argument("--node",
                        dest="node",
                        default=None,
                        help="Target node for run")

    parser.add_argument("--gpu", action="store_true", dest="gpu", default=False,
                        help="Use GPU if possible")


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, options):

        print(dir(options))
        self._tasks = []
        self._taskid = 1

        self.options = options

        task = {
            "target": options.target,
            "gpu": options.gpu
        }
        if options.args:
            task["arguments"] = options.args.split(" ")

        self.head.add_job(1, 1, task, module="docker", node=self.options.node)

    def onAllocated(self, task):
        print("Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"])

    def onCompleted(self, task):
        print("Task completed:", task["taskid"], task["step"])
        self._tasks.remove(task["taskid"])
        print("Setting done", task["args"]["src"])
        self.dir_monitor.setDone(task["args"]["src"])

        print(len(self._tasks), "tasks left")
        if len(self._tasks) == 0:
            print("DONE")
            self.head.stop()

    def onTimeout(self, task):
        print("Task timeout out, requeue")
        self.head.requeue(task)

    def onError(self, task):
        print("Error for task", task)
        # Notify someone, log the file as failed, try to requeue it or try to figure out what is wrong and how to fix it
        self._tasks.remove(task["taskid"])
        print(len(self._tasks), "tasks left")
        if len(self._tasks) == 0:
            print("DONE")
            self.head.stop()
