import time

from CryoCore.Core import Status

class PrintStatusReporter(Status.OnChangeStatusReporter):
    """
    Print all changes to the screen
    """
    def report(self, event):
        """
        print to screen
        """

        def colored(text, color):
            colors = {"green": "\033[92m",
                      "red": "\033[91m",
                      "yellow": "\033[93m",
                      "blue": "\033[94m"}
            return colors[color] + text + "\033[0m "

        if 0: # BW
            print("%s: [%s] %s=%s"%(event.status_holder.get_name(),
                                    time.ctime(event.get_timestamp()),
                                    event.get_name(),
                                    event.get_value()))
        else:
            print("%s: [%s] %s= %s"%(colored(event.status_holder.get_name(), "red"),
                                    colored(time.ctime(event.get_timestamp()), "green"),
                                    colored(event.get_name(), "blue"),
                                    event.get_value()))
