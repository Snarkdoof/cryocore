

CFG = {}


class Config:
    def __init__(self, root="", version=None):
        self._root = root
        global CFG
        self.cfg = CFG
        self._callbacks = {}

    def set_default(self, name, value):
        if name not in self.cfg:
            self.cfg[name] = value

    def get(self, path, absolute_path=False):
        if not absolute_path:
            p = self._root + "." + path
        else:
            p = path

        if p not in self.cfg:
            raise Exception("Missing parameter")
        return self.cfg[p]

    def add(self, path, value):
        self.cfg[path] = value

    def set(self, path, value, absolute_path=False):
        if not absolute_path:
            p = self._root + "." + path
        else:
            p = path

        self.cfg[p] = value
        if p in self._callbacks:
            for func, args in self._callbacks:
                if args:
                    func(path, *args)
                else:
                    func(path)

    def keys(self, path):
        p = self._root + "." + path
        children = []
        for k in self.cfg:
            if k.startswith(p):
                children.append(k[len(p):])
        return children

    def ___setitem__(self, name, value):
        self.set(name, value)

    def __getitem__(self, name):
        return self.get(name)

    def require(self, items):
        for item in items:
            if self._root + "." + item not in self.cfg:
                raise Exception("Missing parameter '%s.%s'" % (self._root, item))

    def add_callback(self, parameter_list, func, *args):
        for param in parameter_list:
            p = self._root + "." + param
            if p not in self._callbacks:
                self._callbacks[p] = []
            self._callbacks[p].append((func, args))

    def del_callback(self, callback_id):
        # Just ignore for now
        pass
