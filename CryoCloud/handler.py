from .Common import jobdb


class DefaultHandler:
    head = None
    PRI_HIGH = jobdb.PRI_HIGH
    PRI_NORMAL = jobdb.PRI_NORMAL
    PRI_LOW = jobdb.PRI_LOW
    PRI_BULK = jobdb.PRI_BULK
    TYPE_NORMAL = jobdb.TYPE_NORMAL
    TYPE_ADMIN = jobdb.TYPE_ADMIN
    TYPE_MANUAL = jobdb.TYPE_MANUAL

    def onReady(self):
        pass

    def onAllocated(self, task):
        pass

    def onCompleted(self, task):
        pass

    def onTimeout(self, task):
        pass

    def onError(self, task):
        pass

    def onStepCompleted(self, step):
        pass
