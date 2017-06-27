import os
import os.path


def process_task(self, task):
    """
    self.status and self.log are ready here.

    Move files from one place to another
    Needs task["args"]["src"] and "dst"

    """
    if "src" not in task["args"]:
        raise Exception("Missing src")
    if "dst" not in task["args"]:
        raise Exception("Missing dst")

    src = task["args"]["src"]
    dst = task["args"]["dst"]

    if src.__class__ != list:
        src = [src]

    for s in src:
        if not os.path.exists(s):
            raise Exception("Missing source file '%s'" % s)

        if os.path.isdir(dst):
            d = dst + os.path.split(s)[1]
        else:
            d = dst

        self.log.debug("Moving file '%s' to %s" % s, d)
        os.rename(s, d)

    return 100, None
