import os
import zipfile
import json


def process_task(self, task):
    """
    Unzip files

    """
    if "src" not in task["args"]:
        raise Exception("Missing src")
    src = task["args"]["src"]

    if "dst" in task["args"]:
        dst = task["args"]["dst"]
        if not os.path.isdir(dst):
            raise Exception("dst is not a directory")
    else:
        # We use the directory of the source file as default target
        dst = os.path.split(src)[0]

    if src.__class__ != list:
        src = [src]

    done = 0
    errors = ""
    for s in src:
        if not os.path.exists(s):
            raise Exception("Missing zip file '%s'" % s)

        self.log.debug("Unzipping %s to %s" % (s, dst))
        try:
            retval = []
            f = zipfile.ZipFile(s)
            names = f.namelist()
            for name in names:
                if name[-1] == "/" and name.count("/") == 1:
                    retval.append(name[:-1])
            f.extractall(dst)
            done += 1
            errors = json.dumps(retval)
        except Exception as e:
            errors += "%s: %s\n" % (s, e)
            self.log.exception("Unzip of %s failed" % s)

        # Very crude progress - only count completed archives
        self.status["progress"] = 100 * done / float(len(src))

    return self.status["progress"].get_value(), errors
