import time
import socket
import select
import random
import threading as threading

import queue

from CryoCore.Core import API,Status
from CryoCore.Core.Status import *

# Index
TIME = 0
PARAM = 1
SOCKET = 2
TYPE = 3
INTERVAL = 4

def by_due_time(a, b):
    """
    Sort a list of requests by time, soonest first
    """
    return a[TIME] <= b[TIME]

class NetworkException(Exception):
    pass

class NoSuchHolderException(Exception):
    pass

class RemoteStatusReporter(Status.OnChangeStatusReporter, threading.Thread):

    PROTOCOL = socket.AF_UNSPEC # AF_INET for IPv4 only, AF_UNSPEC for any
    
    def __init__(self, name, stop_event=None):
        threading.Thread.__init__(self)
        
        Status.OnChangeStatusReporter.__init__(self, name)
        
        self.name = name
        
        #self.cfg = API.get_config(self.name)

        self.cfg = API.get_config("System.Status.RemoteStatusReporter")
        self.cfg.require(["port"])

        self.log = API.get_log(self.name)
        #self.status = API.get_status(self.name)
        
        self.port = None

        if not stop_event:
            self.stop_event = threading.Event()
        else:
            self.stop_event = stop_event
        self.lock = threading.Lock()
        
        self._addresses = {}
        self._requests = []
        
        self._setup_network()

        self._register()
        
        self.start()


    def _register(self):
        """
        Register with on-board status service
        """
        pass

    def _unregister(self):
        """
        Unregister with on-board status service
        """
        pass
        
    def _setup_network(self):
        """
        Create a TCP socket that accepts a connection.
        
        If the config parameter 'port' is "auto", it will find one, otherwise it will 
        try to use the provided port

        """
        
        max_retries = 100
        while True:
            max_retries -= 1
            if max_retries == 0:
                self.port = None
                raise Exception("Could not set up network (after 100 iterations)!")

            try:
                if self.cfg["port"] == "auto":
                    self.port = random.randint(1500, 65000)
                else:
                    self.port = int(self.cfg["port"])
                    
                for res in socket.getaddrinfo(None, 
                                              self.port, 
                                              socket.AF_UNSPEC,
                                              socket.SOCK_STREAM, 0,
                                              socket.AI_PASSIVE):
                    
                    af, socktype, proto, canonname, sa = res
                    self.log.info("Remote status reporter listening on %s"% str(sa))
                    self.socket = socket.socket(af, socktype, proto)
                    self.socket.bind(sa)
                    self.socket.listen(1)
                    return
            except:
                if not self.cfg["port"] == "auto":
                    self.port = None
                    raise Exception("Could not set up network!")
                    

    def get_port(self):
        return self.port
    
    def stop(self):
        self.stop_event.set()

    def run(self):
        """
        Thread entrypoint - Accept connections from multiple
        remote status wrappers.  Will accept both SUBCRIBE and
        UNSUBSCRIBE messages from remote nodes and push status
        updates accordingly.

        message is SUBSCRIBE|PARAM|TYPE|INTERVAL or UNSUBSCRIBE|PARAM

        The PARAM must be in holder.name format
        
        """

        self.clients = []
        while not self.stop_event.is_set():

            try:
                # Check for new incoming connections
                (r,w,e) = select.select([self.socket] + self.clients,
                                        [], [], 0.5)
                
                if len(r) > 0:
                    # We got something!
                    for source in r:
                        if source == self.socket:
                            # Accept new connection
                            (new_conn, addr) = self.socket.accept()
                            self.log.info("Accepted connection from %s"% \
                                          str(addr))
                            self._addresses[new_conn] = addr
                            #new_conn.setblocking(False)
                            self.clients.append(new_conn)
                            
                        else:
                            # Remote status request
                            try:
                                self._parse_request(source)
                            except NetworkException:
                                self._lost_connection(source)
                                continue
                            
                # Done listening - should I send any changes to anyone?
                now = time.time()
                updates = []
                self.lock.acquire()
                try:
                    if len(self._requests) == 0:
                        continue

                    # Sort the requests by due time - soonest first
                    self._requests.sort()

                    while float(self._requests[0][TIME]) <= now:
                        req =  self._requests.pop(0)
                        #if req[TYPE] == "periodic": # All are periodic...
                        # Update the req and add it back - the
                        # list will be sorted by time after this loop
                        new_time = float(req[TIME]) + float(req[INTERVAL])
                        self._requests.append((new_time, ) + req[1:])
                        
                        holder, name = self._split_param(req[PARAM])
                        elem = self.status_holders[holder][name]
                        if not elem:
                            self.log.error("Unknown parameter should be reported: %s"%param)
                            continue
                        updates.append((elem, req[SOCKET]))
                finally:
                    self.lock.release()

                # Perform the actual reporting
                for (elem, socket) in updates:
                    self._send_update(elem, socket)
                    
            except:
                self.log.exception("Error status reporting")
                        
        self._unregister()
        
    def _split_param(self, param):
        """
        Split the param into holder,name.  Require the holder
        to actually be present too.

        @return (holder name, param)
        """
        idx = 0
        while True:
            pos = param.find(".", idx)
            if pos == -1:
                raise NoSuchHolderException("Unknown holder for %s"% param)
            
            if param[:pos] in list(self.status_holders.keys()):
                return (param[:pos], param[pos+1:])

            # This was not a holder, try next "."
            idx = pos + 1
            
        raise NoSuchHolderException("Unknown holder for %s"% param)
    

    def _parse_request(self, socket):
        """
        Data is available from a client - read and parse the
        request and set up any requests to be handled

        Format is PARAM|TYPE|ARGUMENT\n where ARGUMENT
        can be blankm but there must be two pipes pr line.
        Type must be one of 'periodic', "once", or "onchange"

        """

        # Assume we get less than 10k of requests
        try:
            data = socket.recv(10240)
        except Exception as e:
            raise NetworkException(e)

        if not data:
            return

        try:
            for line in data.split("\n"):
                if not line.strip(): # Skip any blank lines
                    continue
                
                try:
                    command = line.split("|")
                except:
                    self.log.error("Bad request: '%s'"%line)
                    continue

                if command[0] == "SUBSCRIBE":
                    
                    (cmd, param, type, arg) = command
                    if param.lower() == "all":
                        for holder in list(self.status_holders.keys()):
                            for name in list(self.status_holders[holder].keys()):
                                if type == "onchange":
                                    self.status_holders[holder][name].add_callback(self._send_update, socket)
                                    self._send(self.status_holders[holder][name], socket)
                                elif type == "periodic":
                                    req = (time.time(), holder + "." + name, socket, type, arg)
                        continue

                    holder,name = self._split_param(param)
                    
                    elem = self.status_holders[holder][name]
                    if not elem:
                        self.log.error("Ignoring request for unknown parameter '%s'"%param)
                        continue

                    if type == "once":
                        self._send_update(elem, socket)
                    elif type == "onchange":
                        # TODO: Do this asynchronously? Is a nonblocking socket
                        elem.add_callback(self._send_update, socket)
                        self._send_update(elem, socket)
                    elif type == "periodic":
                        self.lock.acquire()
                        try:
                            req = (time.time(), param, socket, type, arg)
                            # Will be sorted before reporting
                            self._requests.append(req)
                        finally:
                            self.lock.release()
                    else:
                        self.log.error("Bad request type '%s', expected one of 'once', 'onchange' or 'periodic'"%type)
                        continue
                    
                    self.log.debug("Got subscribe '%s' on %s"% (type, param))
                    
                elif command[0] == "UNSUBSCRIBE":

                    (cmd, param) = command
                    holder, name = self._split_param(param)
                    elem = self.status_holders[holder][name]
                    if not elem:
                        self.log.error("Ignoring unsubscribe for unknown parameter '%s'"%param)
                        continue

                    self.log.debug("Got unsubscribe %s"% param)

                    # Remove periodics
                    self.lock.acquire()
                    try:
                        for req in self._requests:
                            if req[PARAM] == param:
                                self._requests.remove(req)
                    finally:
                        self.lock.release()

                    # Remove callbacks
                    try:
                        elem.del_callback(self._send_update(socket))
                    except:
                        pass
                elif command[0] == "LIST":
                    self.log.debug("Got LIST request")
                    for holder in list(self.status_holders.values()):
                        for name in holder.list_status_elements():
                            elem = holder[name]
                            self._send_update(elem, socket)
                    self.log.debug("LIST request OK")
                        
        except:
            self.log.exception("Processing request from " + str(self._addresses[socket]))

    def _lost_connection(self, socket):
        """
        Perform tasks to clean up after a node dissapeared
        """
        # Remove periodics
        self.lock.acquire()
        try:
            for req in self._requests:
                if req[SOCKET] == socket:
                    self._requests.remove(req)

            if socket in self.clients:
                self.clients.remove(socket)
        finally:
            self.lock.release()
            

    def _send_update(self, element, socket):
        """
        Send the element on the given socket
        """
        # Serialize the element thingy
        try:
            socket.send(element.serialize() + "\n")
        except:
            self._lost_connection(socket)
            self.log.exception("Sending update on '%s to %s"% \
                               (element.get_name(),
                                self._addresses[socket]))


    def report(self, event):
        """
        Callback function to when an element has been updated
        """

        # Silently ignore all these
        pass
