import threading
import time

import sys
if sys.version_info.major == 2:
    import Queue as queue
else:
    import queue

# Global Variables to save all the Status_holders object and to access to them through a Lock

status_holders = {}
status_lock = threading.Lock()


# ===============================================================
def get_status_holder(name, stop_event=None):
    """
    It returns the status of the application I{name}. In case of this I{name} was not already included into the L{status_holder<status_holders>} dictionary, it would be included.
    @param name: Name of the application which status will be saved.
    @type name: C{string}
    @return: The L{StatusHolder<StatusHolder>} object which keeps all the related-status data.
    @rtype: L{StatusHolder<StatusHolder>}
    @postcondition: the application status, which is named I{name}, woudl be kept in L{status_holders<status_holders>}, if it was not included before the call.
    @note: this function has been synchronized by means of a C{Lock}, therefore it can be accessed by different threads at the same time safely.
    """
    global status_lock, status_holders
    with status_lock:
        if name not in status_holders:
            status_holders[name] = StatusHolder(name, stop_event=stop_event)

        return status_holders[name]


class StatusException(Exception):
    pass


class NoSuchElementException(StatusException):
    pass


class NoSuchReporterException(StatusException):
    pass

# ===============================================================


class StatusHolder(threading.Thread):
    """
    A class to hold (and report) status information for an application.
    A status holder can have multiple reporters, that will report status information on change or periodically.

    It runs as a thread which asynchronously executes callbacks
    @author: Njaal
    @organization: Norut
    @date: November 2008
    @version: 1.0


    @ivar name: Name of the application whose status is being kept.
    @type name: C{string}
    @ivar elements: parameters which are related to the status to be kept.
    @type elements: C{dictionary} of L{BaseElement<BaseElement>}
    @ivar reporters: objects to be called either periodically or in case of change of the watched status.
    @type reporters: L{StatusReporter<StatusReporter>} object: L{OnChangeStatusReporter<OnChangeStatusReporter>} or L{PeriodicStatusReporter<PeriodicStatusReporter>}
    @ivar events: C{list} of events
    @type events: C{list}
    @ivar lock: lock to guarantee the methods are thread-safe.
    @type lock: C{Lock}
    """

    def __init__(self, name, stop_event):
        """
        This builder should be called just once. Therefore, do not create new StatusHolder    if you don't know what you're doing. Use the L{getStatusHolder<getStatusHolder>} function to retrieve StatusHolder object instead.
        @param name: application name which saves its status inside the object's dictionary.
        @type name: C{string}
        @postcondition: The object has been initialized by setting the application name, setting to void the internal dictionaries and list, and getting the lock to synchronize the access to the already mentioned dictionaries.
        """
        threading.Thread.__init__(self)

        self.name = name
        self.elements = {}
        self.reporters = {}

        self._status_lock = threading.RLock()

        from CryoCore.Core import API
        self.log = API.get_log(self.name)

        self.events = []
        self.stop_event = stop_event
        self._async_stop_event = threading.Event()
        self._callback_queue = queue.Queue()
        self._set_value_queue = queue.Queue()  # For async set_value
        self.start()

    def kill(self):
        """
        Terminate hard
        """
        self._async_stop_event.set()

    def stop(self):
        """
        Perform any outstanding callbacks and set_values before quitting
        """

        print("Stopping")
        while self._set_value_queue.unfinished_tasks > 0:
            time.sleep(0.1)
            print("set_value:", self._set_value_queue.unfinished_tasks)

        while self._callback_queue.unfinished_tasks > 0:
            time.sleep(0.1)
            print("callback:", self._set_value_queue.unfinished_tasks)

        if 0:
            with self._set_value_queue.empty:
                if self._set_value_queue.unfinished_tasks > 0:
                    self._set_value_queue.empty.wait(10.0)

            with self._callback_queue.empty:
                if self._callback_queue.unfinished_tasks > 0:
                    self._callback_queue.empty.wait(10.0)

        self._async_stop_event.set()
        print("Async stopped")

    def run(self):
        self.log.info("Callback %s thread started" % self.name)
        while not self._async_stop_event.is_set():
            try:
                while self._set_value_queue.unfinished_tasks > 0:
                    (elem, value, force_update, timestamp) = self._set_value_queue.get(block=False)
                    elem.set_value(value, force_update, timestamp, async=False)
            except queue.Empty:
                pass
            except:
                self.log.exception("Exception in async set value")

            func = None
            try:
                func = self._callback_queue.get(block=True, timeout=1.0)
            except queue.Empty:
                pass  # Why the h... does this not return None?

            try:
                if func:
                    func()
            except:
                self.log.exception("Exception in callback '%s'" % str(func))

            if self.stop_event.is_set():
                break

        self.log.info("Callback thread %s stopped" % self.name)

    def queue_callback(self, func):
        if not self.stop_event.is_set():
            self._callback_queue.put(func)

    def async_set_value(self, elem, value, force_update, timestamp):
        if not self.stop_event.is_set():
            self._set_value_queue.put((elem, value, force_update, timestamp))

    def reset(self):
        """
        Reset everything to blanks.
        @postcondition: The object has been reseted, so its dictionaries and list are empty.
        """

        self.elements = {}
        self.reporters = {}
        self.events = []

    def deserialize(self, serialized):
        """
        Deserialize a serialized status element. Throws exception if badly
        formatted.

        @return a BaseElement
        """
        try:
            (name, timestamp, value) = serialized.split("|")

            # Try to convert to native types
            if value.isdigit():
                value = int(value)
            elif value.count(".") > 0:
                try:
                    value = float(value)
                except:
                    pass
            elif value.lower() in ["true", "false"]:
                value = bool(value)
            timestamp = float(timestamp)

            return RemoteStatusElement(name, self, timestamp=timestamp,
                                       initial_value=value)
        except:
            self.log.exception("Could not deserialize '%s'" % serialized)

    def has_key(self, key):
        """
        Check if the key is known

        @return: True iff a status object exists with the given key
        """
        with self._status_lock:
            return key in self.elements

    def get_name(self):
        """
        Return the name of this status.
        @return: the application name which was set during the initialization.
        @rtype: C{string}
        """

        return self.name

    def get_reporter(self, name):
        """
        Get a given reporter from the status, using the name of the reporter. It returns the reporter addressed by its L{name} which has been saved by calling L{add_reporter<add_reporter>}.
        @return: The reporter linked to the I{name}.
        @rtype: a C{object} based on L{StatusReporter<StatusReporter>}: L{OnChangeStatusReporter<OnChangeStatusReporter>} or L{PeriodicStatusReporter<PeriodicStatusReporter>}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @param name: name of the searched reporter.
        @type name: C{string}
        @raise AssertionError: the parameter I{name} was C{None}
        @raise Exception: the reporter I{name} was not found
        """
        assert name
        with self._status_lock:
            if name not in self.reporters:
                raise Exception("No such reporter '%s'" % name)
            return self.reporters[name]

    def add_reporter(self, reporter):
        """
        Add a reporter to this status object. This recently added reporter can be accessed by L{get_reporter<get_reporter>}.
        @postcondition: the internal dictionary has got another reporeter to be indexed by its name.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @param reporter: The reporter object to be saved inside.
        @type reporter: L{StatusReporter<StatusReporter>} object: L{OnChangeStatusReporter<OnChangeStatusReporter>} or L{PeriodicStatusReporter<PeriodicStatusReporter>}
        @raise AssertionError: the parameter I{reporter} was C{None}
        @raise Exception: the I{reporter}, which is distinguished by its field name, has been already saved.
        """
        assert reporter
        with self._status_lock:
            if reporter.name in self.reporters:
                return  # Silent ignore
                # raise Exception("Already have reporter '%s' registered"%reporter.name)
            self.reporters[reporter.name] = reporter

            # The reporter must contact me later
            reporter.add_status_holder(self)

            # if we have any other reporters, copy the elements to the new one
            for element in list(self.elements.values()):
                self._add_element(element)

    def _add_element(self, new_element):
        """
        Add the element I{new_element} to the C{dictionary} with the element and it is added as element to every single reporter of the C{dictionary} reporter.
        @param new_element: new element to be added to the dictionaries.
        @type new_element: a element based on L{BaseElement<BaseElement>}
        @postcondition: save the I{new_element} into the L{elements<elements>} besides saving the element into the every single reporter in L{reporters<reporters>}.
        """

        if new_element.name in self.elements:
            if self.elements[new_element.name].__class__ == RemoteStatusElement:
                self.elements[new_element.name]._update(new_element)
            elif self.elements[new_element.name].__class__ == Status2DElement:
                self.elements[new_element.name]._updated()
            else:
                self.elements[new_element.name].set_value(new_element.get_value(),
                                                      timestamp=new_element.get_timestamp())
        else:
            self.elements[new_element.name] = new_element
            for reporter in list(self.reporters.values()):
                reporter.add_element(new_element)

    def create_status_element(self, name, initial_value=None, expire_time=None):
        """
        Create and return, after having saved a new L{StatusElement<StatusElement>} object with the given parameters: I{name} and I{initial_value} into the C{dictionary} L{elements<elements>}.
        @param name: Name of the new element to be included into the L{StatusElement<StatusElement>}.
        @type name: C{string}
        @param initial_value: Initial value which will be refreshed after the first update
        @type initial_value: C{object}
        @return: a new initialized element which keeps a piece of data of an application status.
        @rtype: L{StatusElement<StatusElement>}
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @raise AssertionError: the parameter I{name} was C{None}.
        @raise Exception: the element was already saved into the dictionary L{elements<elements>}.
        @postcondition: the just created status element has been included into the reporters' element list. These reporters are indexed into the status holder dictionary L{reporters<reporters>}.
        """
        assert name
        with self._status_lock:
            if name in self.elements:
                raise Exception("Already have a status element with the name '%s'" % name)
            new_element = StatusElement(name, self, initial_value, expire_time=expire_time)
            self._add_element(new_element)
        return new_element

    def add_status_element(self, element):
        """
        Add the L{StatusElement<StatusElement>} I {status_element} object. This status element can be accessed by L{get_status_element<get_status_element>}.
        @postcondition: the dictionary L{elements<elements>} has added the I{element}, furthermore it has been added to every single reporter's element list of L{reporters<reporters>}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @param element: created element to be kept.
        @type element: L{StatusElement<StatusElement>}
        """
        with self._status_lock:
            self._add_element(element)

    def create_status2d_element(self, name, size, initial_value=None, expire_time=None):
        assert name
        with self._status_lock:
            if name in self.elements:
                raise Exception("Already have a status element with the name '%s'" % name)
            new_element = Status2DElement(name, self, size, initial_value, expire_time=expire_time)
            self._add_element(new_element)

        return new_element

    def get_status_element(self, name):
        """
        Get a status element from the L{StatusHolder<StatusHolder>} by I{name}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @return: a status element which is indexed by its I{name}.
        @rtype: L{StatusElement<StatusElement>}
        @raise AssertionError: the parameter I{name} was C{None}.
        """
        assert name
        with self._status_lock:
            if name not in self.elements:
                raise NoSuchElementException(name)
            return self.elements[name]

    def get_or_create_status_element(self, name, initial_value=None, expire_time=None):
        """
        Return the status element which is named by I{name}, but in case of not existing such element it is created as it goes along.
        @postcondition: The status element whose name is I{name} is created if it did not exist before. Moreover, it has been added to every single reporter's element list of L{reporters<reporters>}.
        @param name: name of the searched status element.
        @type name: C{string}
        @param initial_value: initial value for the status element if its creation is required. Its default values is C{None}.
        @type initial_value: C{object}
        @raise AssertionError: from L{create_status_element<create_status_element>}
        @raise Exception: from L{create_status_element<create_status_element>}
        """
        with self._status_lock:
            if name not in self.elements:
                e = self.create_status_element(name, initial_value, expire_time=expire_time)
                self._add_element(e)
                return e
            elif expire_time:
                self.elements[name].set_expire_time(expire_time)
            return self.elements[name]

    def get_or_create_status2d_element(self, name, size, initial_value=None, expire_time=None):
        """
        Return the status element which is named by I{name}, but in case of not existing such element it is created as it goes along.
        @postcondition: The status element whose name is I{name} is created if it did not exist before. Moreover, it has been added to every single reporter's element list of L{reporters<reporters>}.
        @param name: name of the searched status element.
        @type name: C{string}
        @param initial_value: initial value for the status element if its creation is required. Its default values is C{None}.
        @type initial_value: C{object}
        @raise AssertionError: from L{create_status_element<create_status_element>}
        @raise Exception: from L{create_status_element<create_status_element>}
        """
        with self._status_lock:
            if name not in self.elements:
                e = self.create_status2d_element(name, size, initial_value, expire_time=expire_time)
                self._add_element(e)
                return e
            # Is this a 2d element?
            if self.elements[name].__class__ != Status2DElement:
                raise Exception("Already have a 1d status element called '%s'" % name)

            return self.elements[name]

    def new(self, name, initial_value=None, expire_time=None):
        """
        Create (or get) a status element (shorthand for get_or_create_status_element). Use this if implicit get using [] operators are sufficient
        """
        return self.get_or_create_status_element(name, initial_value, expire_time)

    def new2d(self, name, size, initial_value=None, expire_time=None):
        """
        Create (or get) a 2D status element (shorthand for get_or_create_status2d_element).  Use this for all 2d status elements
        """
        return self.get_or_create_status2d_element(name, size, initial_value, expire_time)

    def __setitem__(self, key, value):
        """
        Change or create a status element and set it to the given I{value}.
        @postcondition: the I{key} associated I{value} has been changed or created if it did not exist before.
        @param key: dictionary-like key to address the status, the name of the status.
        @type key: C{string}
        @param value: new status for the I{key}.
        @type value: L{StatusElement<StatusElement>}
        @raise AssertionError: from L{get_or_create_status_element<get_or_create_status_element>}.
        @raise Exception: from L{get_or_create_status_element<get_or_create_status_element>}.
        """
        from CryoCore.Core.API import cc_default_expire_time
        self.get_or_create_status_element(key, expire_time=cc_default_expire_time).set_value(value)

    def __getitem__(self, key):
        """
        Get or create a status element from I{key}.
        @param key: dictionary-like kye to address the status, the name of the status.
        @type key: C{string}
        @return: the status which is addressed by I{key}, or in case of having been created yet, the recently created status with the name I{key}.
        @rtype: L{StatusElement<StatusElement>}
        @raise AssertionError: from L{get_or_create_status_element<get_or_create_status_element>}.
        @raise Exception: from L{get_or_create_status_element<get_or_create_status_element>}.
        """
        from CryoCore.Core.API import cc_default_expire_time
        return self.get_or_create_status_element(key, expire_time=cc_default_expire_time)

    def remove_status_element(self, element):
        """
        Remove a status element.
        @postcondition: remove the I{element} from L{elements<elements>}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @raise AssertionError: the I{element} is C{None}.
        @param element: element to be removed
        @type element: L{StatusElement<StatusElement>}
        @raise AssertionError: the parameter I{element} was C{None}.
        @raise NoSuchElementException: the I{element}, which is addressed by its name, was not found in the L{elements<elements>}.
        """
        assert element
        with self._status_lock:
            if element.name not in self.elements:
                raise NoSuchElementException(element.name)
            del self.elements[element.name]

            for reporter in list(self.reporters.values()):
                # TODO: More elegant here
                try:
                    reporter.remove_element(element)
                except:
                    pass

    def create_event(self, name, values=[]):
        """
        Create and return a new event.
        @param name: name of the event to be created
        @type name: C{string}
        @param values: list of values for the event. Its default value is '[]'.
        @type values: C{list}
        @return: the recently created event.
        @rtype: L{EventElement<EventElement>}
        """
        return EventElement(name, self, values)

    def add_event(self, event):
        """
        Add the I{event} to the L{events<events>} list of the status holder. Besides, the L{_add_element<_add_element>} is called, so I{event} is saved into the L{elements<elements>} dictionary and into every single reporters' element list of L{reporters<reporters>}.
        @param event: event to be saved.
        @type event: L{EventElement<EventElement>}
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @postcondition: I{event} has been added to the L{events<events>} list and L{elements<elements>} dictionary.
        """
        with self._status_lock:
            self.events.append(event)
            self._add_element(event)

    def remove_range(self, rng):
        """
        Remove the I{range} from the L{events<events>} list by calling L{remove_event<remove_event>}.
        @param rng:range to be removed.
        @type range: L{RangeElement<RangeElement>}
        @postcondition: the I{range} has been deleted from L{events<events>}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        self.remove_event(rng)

    def remove_event(self, event):
        """
        Remove the I{event} from the L{events<events>}. If I{event} is not found in the list, nothing is done.
        @param event: event to be removed.
        @type event: L{EventElement<EventElement>}
        @postcondition: the I{event} has been deleted from L{events<events>}.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            if event in self.events:
                self.events.remove(event)

    def create_and_add_event(self, name, values=[]):
        """
        Create and add the event which is addressed by I{name}, by calling L{create_event<create_event>} and L{add_event<add_event>} afterwards.
        @postcondition: A event called I{name} with the I{values} has been added to the L{events<events>} list, L{elements<elements>} dictionary and every single reporters' element list of L{reporters<reporters>}.
        @param name: name of the event to be created
        @type name: C{string}
        @param values: list of values for the event. Its default value is '[]'.
        @type values: C{list}
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe, therefore it is thread-safe..
        """
        self.add_event(self.create_event(name, values))

    def create_range(self, name, values=[]):
        """
        Create and return a new range.
        @param name: name of the range to be created
        @type name: C{string}
        @param values: list of values for the range. Its default value is '[]'.
        @type values: C{list}
        @return: the recently created event.
        @rtype: L{RangeElement<RangeElement>}
        """
        return RangeElement(name, self, values)

    def add_range(self, rng):
        """
        Add the I{range} to the L{events<events>} list of the status holder by calling L{add_event<add_event>}.
        @param rng: range to be saved.
        @type range: L{RangeElement<RangeElement>}
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @postcondition: I{range} has been added to the L{events<events>} list, L{elements<elements>} dictionary and into every single reporters' element list of L{reporters<reporters>}.
        """
        self.add_event(rng)

    def create_and_add_range(self, name, values=[]):
        """
        Create and add the range which is addressed by I{name}, by calling L{create_range<create_range>} and L{add_range<add_range>} afterwards.
        @postcondition: A range called I{name} with the I{values} has been added to the L{events<events>} list, L{elements<elements>} dictionary and every single reporters' element list of L{reporters<reporters>}.
        @param name: name of the range to be created
        @type name: C{string}
        @param values: list of values for the range. Its default value is '[]'.
        @type values: C{list}
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe, therefore it is thread-safe.
        """
        self.add_range(self.create_range(name, values))

    def get_elements(self):
        """
        Return a copy of the list of all elements in L{elements<elements>}. This method is provided especially for the reporters.
        @return: a list of elements.
        @rtype: C{list} of L{BaseElement<BaseElement>}
        @note: this method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            return list(self.elements.values())[:]

    def get_events(self):
        """
        Return a list of all the collected events until now, reseting the L{events<events>} list afterwards. This method is provided especially for the reporters.
        @postcondition: the L{events<events>} list is set to [].
        @return: a list of events.
        @rtype: C{list} of L{EventElement}
        @note: this method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            events = self.events
            self.events = []
            return events

    def keys(self):
        """
        Same as list_status_elements(), but for compatibility with
        maps
        """
        return self.list_status_elements()

    def list_status_elements(self):
        """
        Return a list of all the names of the L{elements<elements>}.
        @return: a list with the keys of the status holder's elements.
        @rtype: C{list} of C{string}
        @note: this method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            return list(self.elements.keys())

# ===============================================================


class BaseElement:
    """
    Base class to hold the related element data of the status.
    @author: Njaal
    @organization: Norut
    @date: November 2008
    @version: 1.0

    @ivar type: type of element class, in this case it is "BaseElement"
    @type type: C{string}

    @ivar timestamp: time when the object was created. Time is expressed as a floating point number, in seconds, since the epoch, in UTC.
    @type timestamp: C{float}
    @ivar status_holder: reference to the L{status holder<StatusHolder>} which keeps this element.
    @type timestamp: L{statusHolder<StatusHolder>}
    @ivar name: name of the element.
    @type name: C{string}
    @ivar callbacks: list of functions to be called in case of either change or periodically.
    @type callbacks: C{list}
    @ivar lock: lock to synchronize the access to the sensible object variables.
    @type lock: C{Lock}
    @ivar on_change_events: events to be set on change.
    @type on_change_events: C{list} of L{EventElement<EventElement>}
    @ivar on_value_events: events to be set when an element value reaches a fixed value.
    @type on_value_events: C{list} of L{EventElement<EventElement>}
    """
    type = "BaseElement"

    def __init__(self, name, status_holder, timestamp=None, expire_time=None):
        """
        Create a new element. It must be called by L{create_status_element()<StatusHolder.create_status_element>} throught a status holder object.
        @param name: name of the element
        @type name: C{string}
        @param status_holder: reference to the status holder which keeps this element
        @type status_holder: L{statusHolder<StatusHolder>}
        @param timestamp: time when this element was created. Its default value is the actual time when the object was initialized.
        @type timestamp: C{float}
        @postcondition: all the field of the object has been initialized.
        @raise AssertionError: the I{name} was C{None}.
        """
        assert name

        if not timestamp:
            self.timestamp = time.time()
        else:
            self.timestamp = timestamp

        self.status_holder = status_holder
        self.name = name
        self.value = None
        self.callbacks = []
        self._status_lock = threading.Lock()
        self.on_value_events = {}  # Events to set on change
        self.on_change_events = []
        self._db_param_id = None
        self._db_channel_id = None
        self._expire_time = expire_time

        # Downsampling
        self._limit_num_changes = 0
        self._limit_cooldown = 0
        self._changes_left = 0
        self._next_report = 0

    def __str__(self):
        """
        Return a C{string} with the name and the value of the element.
        @return: list of values of the element headed by the element name.
        @rtype: C{string}
        """
        return self.name + "=" + str(self.get_value())

    def __eq__(self, value):
        """
        Return whether the current element value is equal to the I{value}.
        @return: a boolean value:
            - True, if the current element value is equal to I{value}
            - False, if the current element value is B{not} equal to the I{value}
        @rtype: C{boolean}
        """
        return self.get_value() == value

    def __ne__(self, value):
        """
        Return whether the current element value is unequal to the I{value}.
        @return: a boolean value:
            - False, if the current element value is equal to I{value}
            - Trie, if the current element value is B{not} equal to the I{value}
        @rtype: C{boolean}
        """
        return self.get_value() != value

    def __lt__(self, value):
        """
        Return whether the current element value less than the I{value}.
        @return: a boolean value:
            - True, iff the current element value is less than the I{value}
        @rtype: C{boolean}
        """
        return self.get_value() < value

    def __le__(self, value):
        """
        Return whether the current element value less than the I{value}.
        @return: a boolean value:
            - True, iff the current element value is less than or equal tothe I{value}
        @rtype: C{boolean}
        """
        return self.get_value() <= value

    def __gt__(self, value):
        """
        Return whether the current element value greater than the I{value}.
        @return: a boolean value:
            - True, iff the current element value is greater than the I{value}
        @rtype: C{boolean}
        """
        return self.get_value() > value

    def __ge__(self, value):
        """
        Return whether the current element value greater than the I{value}.
        @return: a boolean value:
            - True, iff the current element value is greater than or equal the I{value}
        @rtype: C{boolean}
        """
        return self.get_value() >= value

    def serialize(self):
        """
        Return a serialized version of this element suitable for
        the remote status service.  Can be deserialized with
        deserialize() command :)
        """
        strng = self.status_holder.get_name() + "." + self.name + "|"
        strng += "%f" % self.get_timestamp() + "|"  # Force to keep decimals
        if self.get_value().__class__ == float:
            strng += "%f" % self.get_value()
        else:
            strng += str(self.get_value())
        return strng

    def clear(self):
        """
        Clear all callbacks and events hooked to this element
        """
        with self._status_lock:
            self.on_value_events = {}  # Events to set on change
            self.on_change_events = []
            self.callbacks = []

    def set_expire_time(self, expire_time):
        self._expire_time = expire_time

    def get_expire_time(self):
        if self._expire_time is None:
            return None
        return time.time() + self._expire_time

    def get_timestamp(self):
        """
        Return when the object was created.
        @return: the element field L{timestamp<timestamp>}.
        @rtype: C{float}
        """
        return self.timestamp

    def get_name(self):
        """
        Return the name given during the initialization.
        @return: the element field L{name<name>}.
        @rtype: C{string}
        """
        return self.name

    def get_type(self):
        """
        Return the element type.
        @return: the element field L{type<type>}.
        @rtype: C{string}
        """
        return self.type

    def get_value(self):
        """
        Return the element value
        @return: the element field L{value<value>}.
        @rtype: whatever the value is
        """
        return self.value

    def add_callback(self, callback, *args):
        """
        Add a callback that will be executed when this element is changed. The callback function will be passed the status element itself and any given arguments too. If the I{callback} function already exists in the list with the same arguments I{args}, nothing is done.
        @param callback: name of the function to be called.
        @type callback: C{string}
        @param args: list of arguments of the function I{callback}.
        @type args: C{list} of C{object}
        @postcondition: the L{callbacks<callbacks>} field has got a new element.
        """
        if (callback, args, False) in self.callbacks:
            raise Exception("Already have callback '%s' registered" % str(callback))
            return

        self.callbacks.append((callback, args, False))

    def add_limited_callback(self, callback, *args):
        """
        Add a callback that will be executed when this element is changed according to downsample specification. The callback function will be passed the status element itself and any given arguments too. If the I{callback} function already exists in the list with the same arguments I{args}, nothing is done.
        @param callback: name of the function to be called.
        @type callback: C{string}
        @param args: list of arguments of the function I{callback}.
        @type args: C{list} of C{object}
        @postcondition: the L{callbacks<callbacks>} field has got a new element.
        """
        if (callback, args, True) in self.callbacks:
            raise Exception("Already have callback '%s' registered" % str(callback))
            return

        self.callbacks.append((callback, args, True))

    def remove_callback(self, callback, *args):
        """
        Remove an already registered callback name with I{callback} and with the arguments I{args}.
        @postcondition: the I{callback} has been removed from the L{callbacks<callbacks>} list.
        @param callback: name of the function to be deleted.
        @type callback: C{string}
        @param args: list of arguments of the function I{callback}.
        @type args: C{list} of C{object}
        @raise Exception: the callback which is indexed by (I{callback}, I{args}) was not found in L{callbacks<callbacks>}.
        """
        if (callback, args, False) in self.callbacks:
            self.callbacks.remove((callback, args, False))
            return
        raise Exception("Cannot remove unknown callback")

    def remove_limited_callback(self, callback, *args):
        """
        Remove an already registered callback name with I{callback} and with the arguments I{args}.
        @postcondition: the I{callback} has been removed from the L{callbacks<callbacks>} list.
        @param callback: name of the function to be deleted.
        @type callback: C{string}
        @param args: list of arguments of the function I{callback}.
        @type args: C{list} of C{object}
        @raise Exception: the callback which is indexed by (I{callback}, I{args}) was not found in L{callbacks<callbacks>}.
        """
        if (callback, args, True) in self.callbacks:
            self.callbacks.remove((callback, args, True))
            return
        raise Exception("Cannot remove unknown callback")

    def add_event_on_change(self, event, once=False):
        """
        Add an event to be set when a element value has changed eitheir once or repeatedly.
        @param event: event to be set on change.
        @type event: L{EventElement<EventElement>}
        @param once: it is a boolean parameter which means:
            - True, this event has to be set just once the element values has changed
            - False, this event has to be set, every single time this is done.
        Its default value is False, so the event is set again and again its value changes.
        @type once: C{boolean}
        @postcondition: the I{event} has been included into the L{on_change_events<on_change_event>} dictionary.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            self.on_change_events.append((event, once))

    def remove_event_on_change(self, event):
        """
        Add an event to be set when a element value has changed eitheir once or repeatedly.
        @param event: event to be set on change.
        @type event: L{EventElement<EventElement>}
        @param once: it is a boolean parameter which means:
            - True, this event has to be set just once the element values has changed
            - False, this event has to be set, every single time this is done.
        Its default value is False, so the event is set again and again its value changes.
        @type once: C{boolean}
        @postcondition: the I{event} has been included into the L{on_change_events<on_change_event>} dictionary.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        """
        with self._status_lock:
            if (event, True) in self.on_change_events:
                self.on_change_events.remove((event, True))
            if (event, False) in self.on_change_events:
                self.on_change_events.remove((event, False))

    def add_event_on_value(self, value, event, once=False):
        """
        Add a threading.Event (or similar - needs set() function) that will be set when the value of this status element becomes the given value.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @postcondition: the I{event} and its modifier I{once} have been saved into the L{on_value_events<on_value_events>} dictionary, indexed by the I{value}, so when this value is reached by the element, the event will be called as many time as I{once} points out.
        @param value: the value which the element must reach to set the event.
        @type value: C{object}
        @param event: event to be set if the value is hit.
        @type event: L{EventElement<EventElement>}
        @param once: it is a boolean parameters which means_
            - True, this event has to be set just the first time the element hits the value.
            - False, this event has to be set, every single time the element hits the value.
        Its default value is False, so the event is set again an again tis values reaches I{value}.
        """
        should_set = False
        with self._status_lock:
            if value in self.on_value_events:
                self.on_value_events[value].append((event, once))
            else:
                self.on_value_events[value] = [(event, once)]

            if self.value == value:
                # Do not set the event until the lock has been released
                should_set = True

        if should_set:
            event.set()

    def remove_event_on_value(self, value, event):
        """
        Remove a threading.Event (or similar - needs set() function) that has been registered with this status element.
        @note: This method is synchronized by C{Lock}, therefore it is thread-safe.
        @param value: the value which the element must reach to set the event.
        @type value: C{object}
        @param event: event to be set if the value is hit.
        @type event: L{EventElement<EventElement>}
        """
        with self._status_lock:
            if value in self.on_value_events:
                for (_event, _once) in self.on_value_events[value]:
                    if _event == event:
                        self.on_value_events[value].remove((_event, _once))

    def downsample(self, num_changes=None, cooldown=None):
        """
        Downsample reporting of this element to either each num_changes updates or wait at
        least cooldown seconds before reporting a new update.
        """
        if num_changes is None and cooldown is None:
            raise Exception("Need either numchanges OR cooldown")

        self._limit_num_changes = num_changes
        self._limit_cooldown = cooldown

    def _updated(self, changed=True):
        """
        When a status element is changed, this method must be called to notify reporters.
        """
        # TODO: Lock or make a copy?
        from CryoCore.Core import API

        # Do notifications
        val = None
        if len(self.on_value_events) > 0:
            val = self.get_value()
            if val in self.on_value_events:
                for (event, is_once) in self.on_value_events[val]:
                    try:
                        if is_once:
                            self.on_value_events[val].remove((event, is_once))
                        event.set()
                    except Exception as e:
                        API.get_log("status").exception("Setting value %s=%s" %
                                                        (self.name, val))
                        print("Exception setting on_value_event: %s=%s: %s" % (self.name, val, e))

        # The rest only should happen if a change occurred
        if not changed:
            return

        for (event, is_once) in self.on_change_events:
            try:
                if is_once:
                    self.on_change_events.remove(event)
                event.set()
            except Exception as e:
                print("Exception setting on_change_event: %s on %s: %s" % (e, self.name, e))
                API.get_log("status").exception("Setting value %s=%s" %
                                                (self.name, val))

        # Make a copy of the status element for asynchonous callbacks
        report_downsampled = True
        if self._limit_num_changes:
            if self._changes_left == self._limit_num_changes:
                self._changes_left = self._limit_num_changes
                report_downsampled = True
            else:
                self._changes_left -= 1
                report_downsampled = False

        if self._limit_cooldown:
            now = time.time()
            if self._next_report <= now:
                self._next_report = now + self._limit_cooldown
            else:
                report_downsampled = False

        import copy
        element = copy.copy(self)
        for (callback, args, downsampled) in self.callbacks:
            if downsampled and not report_downsampled:
                continue
            try:
                if args:
                    # callback(element, *args)
                    self.status_holder.queue_callback(callback(element, *args))
                else:
                    self.status_holder.queue_callback(callback(element))
                    # callback(element)
            except Exception as e:
                print("Exception in callback", callback, "for parameter", self.name, ":", e)
                API.get_log("status").exception("Setting value %s=%s" % (self.name, val))
                # raise Exception("JIKES") # Useful to throw an exception here if you wonder
                # where a status update came from. Do something better?


class StatusElement(BaseElement):
    """
    Class to hold status information
    """
    type = "status report"

    def __init__(self, name, status_holder, initial_value=None, timestamp=None, expire_time=None):
        """
        Create a new element.  DO NOT USE THIS - use
        create_status_element() using a Status Holder object
        """
        BaseElement.__init__(self, name, status_holder, timestamp=timestamp, expire_time=expire_time)
        self.value = initial_value
        self.aux = None

    def set_value(self, value, force_update=False, timestamp=None, async=False, aux=None):
        """
        Update the value of this status element
        """
        if not timestamp:
            self.timestamp = time.time()
        else:
            self.timestamp = timestamp
        self.aux = aux
        if async:
            self.status_holder.async_set_value(self, value, force_update, timestamp)
            return

        if value != self.value or force_update:
            self.value = value
            self._updated()
        else:
            self._updated(changed=False)

    def get_value(self):
        return self.value

    def __float__(self):
        return float(self.value)

    def inc(self, value=1, async=False, commit=True, timestamp=None):
        """
        Will only work for numbers!
        """
        if self.value is None:
            self.value = 0

        if timestamp:
            self.timestamp = timestamp
        else:
            self.timestamp = time.time()

        if async:
            with self._status_lock:
                self.value += value
                if commit:
                    self.status_holder.async_set_value(self, self.value,
                                                       True,
                                                       self.timestamp)
            return

        with self._status_lock:
            try:
                self.value += value
                if commit:
                    self._updated()
            except:
                raise Exception("Can only increment numbers (%s had value %s)" % (self.name, self.value))

    def dec(self, value=1):
        """
        Will only work for numbers!
        """
        self.timestamp = time.time()
        with self._status_lock:
            try:
                self.value -= value
                self._updated()
            except:
                raise Exception("Can only increment numbers")


class RemoteStatusElement(StatusElement):
    """
    A status element that represents remote state.  The main
    difference between it and a normal status element is that the
    nameing is slightly different - as the name of the element is the
    full path, including hostname of the remote element

    You should NEVER create one of these elements, but request it from the
    RemoteStatusHolder
    """
    def __init__(self, name, status_holder, initial_value=None, timestamp=None, expire_time=None):
        """
        Create a new remote status element. Do never use this directly,
        but request it from the RemoteStatusHolder
        """
        StatusElement.__init__(self, name, status_holder,
                               initial_value, timestamp, expire_time)

    def _update(self, deserialized):
        """
        Update the state of this element based on another element,
        typically a remote, deserialized element from a remote
        status reporter.
        """
        assert self.get_name() == deserialized.get_name()
        self.timestamp = deserialized.get_timestamp()
        self.value = deserialized.get_value()
        self._updated()

    def set_value(self, value):
        """
        Remote status objects cannot update state. This method just
        throws an exception
        """
        raise Exception("Remote status objects cannot update state")

    def inc(self, value):
        """
        Remote status objects cannot update state. This method just
        throws an exception
        """
        raise Exception("Remote status objects cannot update state")

    def dec(self, value):
        """
        Remote status objects cannot update state. This method just
        throws an exception
        """
        raise Exception("Remote status objects cannot update state")


class EventElement(BaseElement):
    type = "event"

    def __init__(self, name, status_holder, values=[]):
        """
        Create a new element.  DO NOT USE THIS - use
        create_status_element() using a Status Holder object
        """
        self.time = int(time.time())
        BaseElement.__init__(self, name, status_holder)
        self.values = values

    def get_time(self):
        return self.time

    def add_value(self, value):
        with self._status_lock:
            self.values.append(value)

        self._updated()

    def get_values(self):
        """
        Return the values as a copy to ensure that there are no synchronization issues
        """
        with self._status_lock:
            return self.values[:]


class RangeElement(BaseElement):
    type = "range"

    def __init__(self, name, status_holder, values=[]):
        self.start_time = self.end_time = int(time.time())
        BaseElement.__init__(self, name, status_holder)
        self.values = values

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def add_value(self, value):
        with self._status_lock:
            self.end_time = int(time.time())
            self.values.append(value)

        self._updated()

    def get_values(self):
        """
        Return the values as a copy to ensure that there are no synchronization issues
        """
        with self._status_lock:
            return self.values[:]


class Status2DElement(StatusElement):
    """
    """

    type = "2d status report"

    def __init__(self, name, status_holder, size, initial_value=None, timestamp=None, expire_time=None):
        StatusElement.__init__(self, name, status_holder, timestamp=timestamp, expire_time=expire_time)

        if len(size) != 2:
            raise Exception("Bad size, need to numbers")
        if size[0].__class__ != int or size[1].__class__ != int:
            raise Exception("Bad size, need to numbers")
        self._last_pos = None
        self.value = []
        self.size = size
        self.resized = False
        self.initial_value = initial_value
        for x in range(0, size[0]):
            self.value.append([])
            for y in range(0, size[1]):
                self.value[x].append(initial_value)
        self._updated()
        self.aux = None

    def printOut(self):
        print("Size:", self.size, (len(self.value), len(self.value[0])))
        for x in range(0, len(self.value)):
            print("%2d" % x,)
            for y in range(0, len(self.value[0])):
                print("% 5s" % self.value[x][y],)
            print()

    def set_value(self, pos, value, force_update=False, timestamp=None, async=False, aux=None, expand=True):
        """
        Update the value of the x,y component of this status element
        """
        if pos[0] < 0 or pos[1] < 0:
            raise Exception("Negative positions not allowed")

        if pos[0] >= self.size[0] or pos[1] >= self.size[1]:
            if not expand:
                raise Exception("Bad set_value, %s is outside of range %s" % (str(pos), str(self.size)))
            else:
                # Create new max-sizes
                for x in range(self.size[0] - 1, pos[0]):
                    self.value.append([])
                    for y in range(0, max(self.size[1], pos[1])):
                        self.value[x + 1].append(self.initial_value)
                        print(x, y, self.value)

                self.size = [max(pos[0] + 1, self.size[0]), max(pos[1] + 1, self.size[1])]
                print(self.size, len(self.value), len(self.value[-1]))
                # Update size in DB
                self.resized = True
        else:
            self.resized = False

        if not timestamp:
            self.timestamp = time.time()
        else:
            self.timestamp = timestamp
        self.aux = aux

        if async:
            raise Exception("Async updates not supported for 2d staus yet")
            # self.status_holder.async_set_value(self, value, force_update, timestamp)
            # return

        if value != self.value[pos[0]][pos[1]] or force_update:
            self.value[pos[0]][pos[1]] = value
            self._last_pos = pos
            self._updated()
        else:
            self._updated(changed=False)

    def get_value(self, pos=None):
        if pos:
            if pos[0] >= self.size[0] or pos[1] >= self.size[1] or pos[0] < 0 or pos[1] < 0:
                raise Exception("Bad get_value, %s is outside of range %s" % (str(pos), str(self.size)))
            return self.value[pos[0]][pos[1]]  # (pos, self.value[pos[0]][pos[1]])
        return self.value

    def get_last_update(self):
        if self._last_pos is None:
            return None, None
        return (self._last_pos, self.value[self._last_pos[0]][self._last_pos[1]])

    def get_default_value(self):
        return self.initial_value

    def add_event_on_value(self, value, event, once=False):
        raise Exception("Not supported for 2D status elements")

    def remove_event_on_value(self, value, event):
        raise Exception("Not supported for 2D status elements")

    def inc(self, pos, val=1, timestamp=None):
        if timestamp:
            self.timestamp = timestamp
        else:
            self.timestamp = time.time()

        if pos[0] >= self.size[0] or pos[1] >= self.size[1] or pos[0] < 0 or pos[1] < 0:
            raise Exception("Bad set_value, %s is outside of range %s" % (str(pos), str(self.size)))
        self._last_pos = pos
        self.value[pos[0]][pos[1]] += val
        self._updated()


class StatusReporter:
    """
    This is the basic status reporter class.  It cannot be used
    directly, but provides a base for all status reporters.
    The status reporter is threadsafe
    """
    def __init__(self, name):
        self.name = name
        self._status_lock = threading.RLock()
        self.status_holders = {}

    def add_status_holder(self, holder):

        if not holder.get_name() in self.status_holders:
            self.status_holders[holder.get_name()] = holder

    def get_elements(self):
        """
        Return all elements that should be reported
        """
        elements = []
        for holder in list(self.status_holders.values()):
            elements += holder.get_elements()
        return elements

    def get_events(self):
        """
        Return all elements that should be reported
        """
        events = []
        for holder in list(self.status_holders.values()):
            events += holder.get_events()
        return events

    def add_element(self, element):
        """
        Add a status element to this reporter. Must be overloaded
        to actually do something
        """
        pass

    def remove_element(self, element):
        """
        Remove a status element from this reporter. Must be overloaded
        to actually do something
        """
        pass


class OnChangeStatusReporter(StatusReporter):
    """
    A basic status reporter which calls 'report(element)' whenever
    it is changed
    """
    elements = []

    def add_element(self, element):
        """
        Add element to this reporter
        """
        element.add_limited_callback(self.report)

    def remove_element(self, element):
        """
        Remove an element from this reporter
        """
        element.remove_callback(self.report)

    def report(self, element):
        """
        This function must be implemented by and extending class. Does nothing.
        """
        raise Exception("Report function must be overloaded")


class PeriodicStatusReporter(StatusReporter):
    """
    Base class for a periodic status reporter, calling report(self)
    at given times.  To ensure a nice shutdown, execute stop() when
    stopping.
    """
    def __init__(self, name, frequency, error_handler=None, stop_event=None):
        """
        Frequency is a float in seconds
        Error-handler will get an error code and a string as parameters,
        the meaning will be up to the implemenation of the
        PeriodicStatusReporter.
        """

        StatusReporter.__init__(self, name)
        self.frequency = frequency
        self.parameters = []
        self.error_handler = error_handler

        # Set up the timer
        if not stop_event:
            self.stop_event = threading.Event()
        else:
            self.stop_event = stop_event

        self.timer = threading.Timer(self.frequency, self.on_time_event)
        self.timer.start()

    def stop(self, block=False):
        """
        Stop this reporter.  If block=True this function will not return
        until the reporter has actually stopped
        """
        self.timer.cancel()
        self.on_time_event()
        self.stop_event.set()
        self.timer.cancel()

        if block:
            self.timer.join()

    def report(self):
        """
        This function must be overloaded, does nothing
        """
        raise Exception("Not implemented")

    def on_time_event(self):
        """
        Callback function for timers
        """
        if not self.stop_event.is_set():
            self.timer = threading.Timer(self.frequency, self.on_time_event)
            self.timer.start()

            try:
                self.report()
            except Exception as e:
                self.timer.cancel()
                self.timer = None
                if self.error_handler:
                    try:
                        self.error_handler(0, str(e))
                    except:
                        pass
                else:
                    print("Error but no error handler:", e, "[%s]" % e.__class__)
                    import traceback
                    traceback.print_exc()
