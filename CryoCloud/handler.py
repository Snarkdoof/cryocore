import random
import copy


class DefaultHandler:
    head = None
    metadata = {}

    def addMeta(self, metadata):
        key = random.randint(0, 9223372036854775806)
        self.metadata[key] = copy.copy(metadata)
        return key

    def getMeta(self, key):
        _key = key
        if key.__class__ == dict:
            if "itemid" in key:
                _key = key["itemid"]
        if _key in self.metadata:
            return self.metadata[_key]
        return {}

    def cleanMeta(self, key):
        _key = key
        if key.__class__ == dict:
            if "itemid" in key:
                _key = key["itemid"]
        if _key in self.metadata:
            del self.metadata[_key]

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
