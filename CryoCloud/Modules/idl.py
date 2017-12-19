"""

Module to execute IDL scripts. It basically expecs a task argument with 'cmd'
which is suitable for subprocess.

We also expect status parameters to be printed on stdout in the format:

[progress] 10
[state] running

"""

import subprocess
import os
import os.path
import fcntl
import re
import select


def process_task(worker, task):
    """
    worker.status and worker.log are ready here.

    Please update worker.status["progress"] to a number between 0 and 100 for
    progress for this task

    If an error occurs, just throw an exception, when done return the progress
    that was reached (hopefully 100)

    """
    if "cmd" not in task["args"]:
        raise Exception("Missing 'cmd' in args for IDL module - nothing to run")

    if task["args"]["cmd"].__class__ != list:
        raise Exception("Arguments must be a list")

    cmd = ["idl"]
    cmd.extend(task["args"]["cmd"])

    orig_dir = os.getcwd()
    try:
        if "dir" in task["args"]:
            if not os.path.isdir(task["args"]["dir"]):
                raise Exception("Specified directory '%s' does not exist (or is not a directory)" % task["args"]["dir"])
            os.chdir(task["args"]["dir"])
            print("Change dir to", (task["args"]["dir"]))

        env = os.environ
        if "env" in task["args"]:
            # Specified an additional environment
            for key in task["args"]["env"]:
                env[key] = task["args"]["env"][key]

        worker.log.debug("IDLCommand is: '%s'" % cmd)
        print(" ".join(cmd).join(" "))

        p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # We set the outputs as nonblocking
        fcntl.fcntl(p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

        buf = {p.stdout: "", p.stderr: ""}

        def report(line):
            """
            Report a line if it should be reported as status or log
            """
            m = re.match("\[(.+)\] (.+)", line)
            if m:
                worker.status[m.groups()[0]] = m.groups()[1]

            m = re.match("\<(\w+)\> (.+)", line)
            if m:
                level = m.groups()[0]
                msg = m.groups()[1]
                if level == "debug":
                    worker.log.debug(msg)
                elif level == "info":
                    worker.log.info(msg)
                elif level == "warning":
                    worker.log.warning(msg)
                elif level == "error":
                    worker.log.error(msg)
                else:
                    worker.log.error("Unknown log level '%s'" % level)

        while not worker._stop_event.isSet():
            ready = select.select([p.stdout, p.stderr], [], [], 1.0)[0]
            for fd in ready:
                data = fd.read()
                buf[fd] += str(data, 'UTF-8')

            # Process any stdout data
            while buf[p.stdout].find("\n") > -1:
                line, buf[p.stdout] = buf[p.stdout].split("\n", 1)
                report(line)

            # Check for output on stderr - set error message
            while buf[p.stderr].find("\n") > -1:
                line, buf[p.stderr] = buf[p.stderr].split("\n", 1)
                report(line)

            # See if the process is still running
            if p.poll() is not None:
                # Process exited
                if p.poll() == 0:
                    break
                # Unexpected
                raise Exception("IDL exited with exit value %d" % p.poll())

        return worker.status["progress"].get_value(), None
    finally:
        if "dir" in task["args"]:
            os.chdir(orig_dir)
