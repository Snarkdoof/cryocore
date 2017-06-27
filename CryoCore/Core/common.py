import multiprocessing


def randomPort(target, low_limit=1500, up_limit=65000, max_tries=20):
    import socket
    import random

    targetIP = socket.gethostbyname(target)
    tries = 0
    result = 1

    while (tries < max_tries) and result != 111:
        port = random.randint(low_limit, up_limit)
        result = portState(targetIP, port)
        tries += 1

    if tries >= max_tries:
        port = 0

    return port


def portState(targetIP, port):
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((targetIP, port))
        s.close()
    except Exception as e:
        print(e)
        pass
    finally:
        return result


def connect(host, port):
    import sys
    import socket
    import time

    s = None
    for i in range(0, 3):
        for res in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error as msg:
                s = None
                continue
            try:
                s.connect(sa)
            except socket.error as msg:
                s.close()
                s = None
                continue
            break
        if s:
            break
        time.sleep(2)

    return s


def connect_server(host, port):
    import socket
    import sys

    for res in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM, 0, socket.AI_PASSIVE):
        af, socktype, proto, canonname, sa = res
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error as msg:
            s = None
            continue
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(sa)
            s.listen(5)
        except socket.error as msg:
            s.close()
            s = None
            continue
        break
    return s


class SingletonException(Exception):
    pass


_stSingletons = set()
# Ensure only one instance of each Singleton class is created.
# This is not bound to the individual Singleton class since
# we need to ensure that there is only one mutex for each Singleton class,
# which would required having a lock when setting up the Singleton class,
# which is what this is anyway.
# So, when any Singleton is created,
# we lock this lock and then we don't need to lock it again for that class.

# _lockForSingletons = threading.RLock()
# _lockForSingletonCreation = threading.RLock()

_lockForSingletons = multiprocessing.RLock()
_lockForSingletonCreation = multiprocessing.RLock()


def _createSingletonInstance(cls, lstArgs, dctKwArgs):
    with _lockForSingletonCreation:
        if cls._isInstantiated():  # some other thread got here first
            return

        instance = cls.__new__(cls)

        try:
            instance.__init__(*lstArgs, **dctKwArgs)
        except TypeError as e:
            if str(e).find('__init__() takes') != -1:
                raise SingletonException('If the Singleton requires __init__ args, ' +
                    'supply them on first call to getInstance().')
            else:
                raise

        cls.cInstance = instance

        _addSingleton(cls)


def _addSingleton(cls):
    with _lockForSingletons:
        assert cls not in _stSingletons
        _stSingletons.add(cls)


def _removeSingleton(cls):
    with _lockForSingletons:
        if cls in _stSingletons:
            _stSingletons.remove(cls)


class MetaSingleton(type):
    def __new__(mcs, strName, tupBases, dct):
        if '__new__' in dct:
            raise SingletonException('Can not override __new__ in a Singleton')
        return super(MetaSingleton, mcs).\
                    __new__(mcs, strName, tupBases, dct)

    def __call__(mcs, *lstArgs, **dictArgs):
        raise SingletonException('Singletons may only be instantiated through getInstance()')


#class Singleton(object, metaclass=MetaSingleton):  # Python 3
class Singleton(object):
    """
    A Python Singleton mixin class. It makes use of some of the ideas found at U{http://c2.com/cgi/wiki?PythonSingleton}.
    Just inherit from it and you have a singleton. No code is required in subclasses to create singleton behaviour I{inheritance from Singleton is all that is needed}.
    Singleton creation is threadsafe.|
    USAGE
    =====
    Just inherits from Singleton. If you need a constructor, include an C{__init__()} method in your class as you usually would. However, if your class is S, you instantiate the singleton using S.getInstante() instead of S(). Repeated calls to S.getInstance() return the originally-created instance.
    For example:

    >>> class S(Singleton):
    ... def __init__(self, a, b=1):
    ...     pass

    >>> S1 = S.getInstance(1, b=3)

    Most of the time, that's all you need to know. However, there are some other useful behaviours. Read on for a full description:
        1. Getting the singleton

            >>> S.getInstance()

            returns the instance of S. If none exists, it is created.
        2. The usual idiom to construct an instance by calling the class, i.e. S(), is disabled fo the sake of clarity.

            For one thing, the S() syntax means instantiation, but getInstance() usually does not cause instantiation. So the S() syntax would be misleading.
            Because of that, if S() were allowed, a programmer who didn't happen to notice the inheritance from Singleton (or who wasn't fully aware of what a Singleton pattern does) might think he was creating a new instance, which could lead to very unexpected behaviour.
            So, overall, it is felt that it is better to make things clearer by requiring the call of a class method that is defined in Singleton. An attempt to instantiate via S() will result in a L{SingletonException} being raised.
        3. Use C{__S.__init__()} for instantiation processing, since S.getInstance() runs S.__init__(), passing it the args it has received. If no data needs to be passed in at instantiation time, you don't need S.__init__().
        4. If C{S.__init__(.)} requires parameters, include them B{only} in the first call to S.getInstance(). If subsequent calls have arguments, a L{SingletonException} is raised by default.
        If you find it more convenient for subsequent calls to be allowed to have arguments, but for those arguments to be ignored, just include 'ignoreSubsequent = True' in your class definition, i.e.:

        >>> class S(Singleton):
        ... ignoreSubsequent = True
        ... def __init__(self, a, b=1):
        ...     pass

        5. As an implementation detail, classes that inherit from Singleton may not have their own C{__new__} methods. To make sure this requirement is followed, an exception is raised if a Singleton subclass includes C{__new__}. This happens at subclass instantiation time (by means of the MetaSingleton metaclass).

    @author: Gary Robinson, grobinson@flyfi.com
    @organization: Norut
    @date: October 2009
    @version: 1.0
    """
    __metaclass__ = MetaSingleton  # Python 2.7

    def getInstance(cls, *lstArgs, **dctKwArgs):
        """
        Call this to instantiate an instance or retrieve the existing instance. If the singleton requires args to be instantiated, include them the first time you call getInstance.
        @raise SingletonException: the Singleton has been already instantiated, this method has been called with parameters and the attribute C{ignoreSubsequent} has not been defined in the Singleton offspring.
        @note: This method is "static"
        """
        if cls._isInstantiated():
            if (lstArgs or dctKwArgs) and not hasattr(cls, 'ignoreSubsequent'):
                raise SingletonException('Singleton already instantiated, but getInstance() ' +
                     'was called with args.')
        else:
            _createSingletonInstance(cls, lstArgs, dctKwArgs)

        return cls.__dict__['cInstance']
    getInstance = classmethod(getInstance)

    def _isInstantiated(cls):
        """
        Don't use hasattr(cls, 'cInstance'), because that screws things up if there is a singleton that extends another singleton. hasattr looks in the base class if it doesn't find in subclass.
        @note: This method is "static"
        """
        return 'cInstance' in cls.__dict__
    _isInstantiated = classmethod(_isInstantiated)
