/*	CCshm_py.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
*/

#include "CCshm_py.h"
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include "EventBus.h"

#if PY_MAJOR_VERSION >= 3
#define PY3K 1
#else
#define PY3K 0
#endif

#if PY_VERSION_HEX < 0x030900A4 && !defined(Py_SET_SIZE)
static inline void _Py_SET_SIZE(PyVarObject *ob, Py_ssize_t size)
{ ob->ob_size = size; }
#define Py_SET_SIZE(ob, size) _Py_SET_SIZE((PyVarObject*)(ob), size)
#endif

#if CCSHM_VERSION == 3 && !PY3K
    #error Compiling for python3, but we're including python2
#elif CCSM_VERSION == 2 && PY3K
    #error Compiling for python2, but we're including python3
#endif

extern "C" {

static EventBusManager *_bus_manager = 0;

typedef struct {
	PyObject_HEAD
	EventBus	*bus;
} EventBus_Py;

static PyObject *EventBus_Py_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	EventBus_Py *self;
	self = (EventBus_Py *) type->tp_alloc(type, 0);
	if (self != NULL) {
		self->bus = 0;
	}
	return (PyObject *) self;
}

static void	EventBus_Py_dealloc(EventBus_Py *self) {
    if (self->bus) {
        if (_bus_manager)
            _bus_manager->remove_bus(self->bus);
		self->bus->release();
    }
	self->bus = 0;
	Py_TYPE(self)->tp_free((PyObject *) self);
}

static int EventBus_Py_init(EventBus_Py *self, PyObject *args, PyObject *kwds) {
	const char *path = 0;
	Py_ssize_t	items = 0, item_size = 0;
	if (!PyArg_ParseTuple(args, "snn", &path, &items, &item_size)) {
		PyErr_SetString(PyExc_ValueError, "Path to file, items or item_size invalid\n");
		return -1;
	}
	struct stat st_buf = {};
	if (stat(path, &st_buf) != 0) {
		PyErr_SetString(PyExc_ValueError, "Please provide a path to an (empty) file to use for generating shared memory and semaphores.\n");
		return -1;
	}
	bool force_init = false;
	self->bus = new EventBus(path, items, item_size);
	if (!self->bus->open(force_init)) {
		self->bus->release();
		self->bus = 0;
		PyErr_SetString(PyExc_ValueError, "Failed to initialize event bus.\n");
		return -1;
	}
    if (_bus_manager)
        _bus_manager->add_bus(self->bus);
	return 0;
}

static PyMemberDef EventBus_Py_members[] = {
	{NULL}  /* Sentinel */
};

static PyObject*	EventBus_Py_post(EventBus_Py *self, PyObject *args) {
	const char	*data = 0;
	Py_ssize_t	length = 0;
	if (!PyArg_ParseTuple(args, "s#", &data, &length)) {
		PyErr_SetString(PyExc_ValueError, "Unable to post data - wrong type (expect string or bytes)\n");
		return 0;
	}
    if (length > 0) {
        EventBusData	ebd;
        ebd.data = malloc(length);
        memcpy(ebd.data, data, length);
        ebd.length = length;
        ebd.free_after = true;
        _bus_manager->post(self->bus, ebd);
    }
#if 0
	//	TODO: Determine if we should make a copy of the string (python thread safety?)
	//	Don't think it's necessary, but..
	Py_BEGIN_ALLOW_THREADS;
	//	We actually should make this post to a different thread, so we don't block the caller.
	self->bus->post(ebd);
	Py_END_ALLOW_THREADS;
#endif
    
	Py_RETURN_NONE;
}


/*	This is a custom constructor for PyByteArray that takes ownership
	of the passed-in buffer. Some caveats:
	1) Whoever is responsible for creating the buffer, must ensure that
	   its size is 1 larger than the length it represents.
	2) It might be advantageous to use the PyObject_Malloc function to create
	   the buffer in the first place. Not entirely sure what happens if one passes
	   in a buffer made with a different allocator. (malloc might be fine, but...)
*/
static PyObject*	PyByteArray_FromMemoryTransferOwnership(void *data, Py_ssize_t len) {
	PyByteArrayObject *obj = 0;
    
    obj = PyObject_New(PyByteArrayObject, &PyByteArray_Type);
    if (!obj)
        return 0;
	obj->ob_bytes = (char*)data;
	obj->ob_bytes[len] = '\0';
    Py_SET_SIZE(obj, len);
    obj->ob_alloc = len + 1;
    #if PY3K
    obj->ob_start = obj->ob_bytes;
    #endif
    obj->ob_exports = 0;
	return (PyObject *)obj;
}

static PyObject*	EventBus_Py_get(EventBus_Py *self, PyObject *args) {
	EventBusData	ebd = {};
	bool			got_data = false;	
	Py_BEGIN_ALLOW_THREADS;
	got_data = self->bus->get(ebd);
	Py_END_ALLOW_THREADS;
	
	if (got_data && ebd.length > 0)
		return PyByteArray_FromMemoryTransferOwnership(ebd.data, ebd.length);
	
	Py_RETURN_NONE;
}


static PyObject*	EventBus_Py_get_head(EventBus_Py *self, PyObject *args) {
	EventBusData	ebd = {};
	bool			got_data = false;	
	Py_BEGIN_ALLOW_THREADS;
	got_data = self->bus->get(ebd, true);
	Py_END_ALLOW_THREADS;
	
	if (got_data && ebd.length > 0)
		return PyByteArray_FromMemoryTransferOwnership(ebd.data, ebd.length);
	
	Py_RETURN_NONE;
}


static EventBusData	EventBusDataFromBuffer(const char *ptr, ssize_t len) {
	EventBusData	d = { };
	d.length = len;
	d.data = malloc(d.length);
	d.free_after = true;
	memcpy(d.data, ptr, len);
	return d;
}


static PyObject*	EventBus_Py_post_many(EventBus_Py *self, PyObject *args) {
	PyObject	*list = 0;
	bool		success = true;
	
	if (!PyArg_ParseTuple(args, "O", &list)) {
		PyErr_SetString(PyExc_ValueError, "Excpected list of strings or bytes objects\n");
		return 0;
	}
	
	std::vector<EventBusData>	items;
	for (Py_ssize_t i=0;i<PyList_Size(list);i++) {
		PyObject	*item = PyList_GetItem(list, i);
		
		#if PY3K
		if (PyUnicode_Check(item)) {
			Py_ssize_t		sz = 0;
			const char *c = PyUnicode_AsUTF8AndSize(item, &sz);
			if (c && sz > 0) {
				items.push_back(EventBusDataFromBuffer(c, sz));
			}
			else {
				PyErr_SetString(PyExc_ValueError, "Failed to get utf-8 representation of string\n");
				printf("Failed to get utf-8 representation of item %zu\n", i);
				success = false;
			}
		}
		#else
		if (PyString_Check(item)) {
			Py_ssize_t		sz = 0;
			char			*c = 0;
			if (PyString_AsStringAndSize(item, &c, &sz) == 0 && c && sz > 0) {
				items.push_back(EventBusDataFromBuffer(c, sz));
			}
			else {
				PyErr_SetString(PyExc_ValueError, "Failed to get item as string and size\n");
				printf("Failed to get item as string and size: %zu\n", i);
				success = false;
			}
		}
		#endif
		else {
			Py_buffer	view = {};
			if (PyObject_GetBuffer(item, &view, PyBUF_SIMPLE) == 0) {
				if (view.len > 0) {
					items.push_back(EventBusDataFromBuffer((const char*)view.buf, view.len));
				}
				PyBuffer_Release(&view);
			}
			else {
				PyErr_SetString(PyExc_ValueError, "Item failed to convert to Py_buffer\n");
				success = false;
				printf("Item %zu failed to convert to Py_buffer\n", i);
			}
		}
	}
	if (items.size() > 0) {
		Py_BEGIN_ALLOW_THREADS;
		self->bus->post_many(items);
		Py_END_ALLOW_THREADS;
	}
	if (success)
		Py_RETURN_NONE;
	return 0;
}


static PyObject*	EventBus_Py_get_many(EventBus_Py *self, PyObject *args) {
	std::vector<EventBusData>	items;
	
	Py_BEGIN_ALLOW_THREADS;
	self->bus->get_many(items);
	Py_END_ALLOW_THREADS;
	
	PyObject	*list = 0;
	if (items.size() > 0) {
		list = PyList_New(items.size());
		for (size_t i=0;i<items.size();i++) {
			PyObject	*bytes = PyByteArray_FromMemoryTransferOwnership(items[i].data, items[i].length);
			PyList_SetItem(list, i, bytes);
		}
	}
	else
		list = PyList_New(0);
	
	return list;
}


static PyMethodDef EventBus_Py_methods[] = {
	{"post", (PyCFunction) EventBus_Py_post, METH_VARARGS, "Post a buffer to the event bus" },
	{"get", (PyCFunction) EventBus_Py_get, METH_NOARGS, "Get message from bus. Blocks until a message is available." },
	{"get_head", (PyCFunction) EventBus_Py_get_head, METH_NOARGS, "Get the latest message from bus. Blocks until a message is available and skips old messages." },
	{"get_many", (PyCFunction) EventBus_Py_get_many, METH_NOARGS, "Get as many messages from bus as possible. Blocks until at least one message is available." },
	{"post_many", (PyCFunction) EventBus_Py_post_many, METH_VARARGS, "Posts a list of buffers to the event bus." },
	{NULL}  /* Sentinel */
};

static PyTypeObject EventBus_Py_type = {
	PyVarObject_HEAD_INIT(NULL, 0)
};

static void CCshm_destroy(void *ptr) {
    if (_bus_manager) {
        _bus_manager->release();
        _bus_manager = 0;
    }
}

#if PY3K
static PyModuleDef CCshm_py3_module = {
	PyModuleDef_HEAD_INIT,
	.m_name = "CCshm_py3",
	.m_doc = "CryoCore Shared Memory.",
	.m_size = -1,
    .m_methods = NULL,
    .m_slots = NULL,
    .m_traverse = NULL,
    .m_clear = NULL,
    .m_free = CCshm_destroy
};
#endif


static void init_eventbus_type() {
	//	This is a workaround for older versions of GCC that complain about non-trivial designated initializer lists.
	#if PY3K
	EventBus_Py_type.tp_name = "CCshm_py3.EventBus";
	#else
	EventBus_Py_type.tp_name = "CCshm_py2.EventBus";
	#endif
	EventBus_Py_type.tp_doc = "EventBus";
	EventBus_Py_type.tp_basicsize = sizeof(EventBus_Py);
	EventBus_Py_type.tp_itemsize = 0;
	EventBus_Py_type.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
	EventBus_Py_type.tp_new = EventBus_Py_new;
	EventBus_Py_type.tp_init = (initproc) EventBus_Py_init;
	EventBus_Py_type.tp_dealloc = (destructor) EventBus_Py_dealloc;
	EventBus_Py_type.tp_members = EventBus_Py_members;
	EventBus_Py_type.tp_methods = EventBus_Py_methods;
}

static PyObject *CCshm_generic_init(void) {
	PyObject *m;
	
    _bus_manager = new EventBusManager();
	init_eventbus_type();
	
	if (PyType_Ready(&EventBus_Py_type) < 0)
		return NULL;
	
	#if PY3K
	m = PyModule_Create(&CCshm_py3_module);
	if (m == NULL)
		return NULL;
	#else
	m = Py_InitModule3("CCshm_py2", 0, "CryoCore Shared Memory.");
	#endif

	Py_INCREF(&EventBus_Py_type);
	if (PyModule_AddObject(m, "EventBus", (PyObject *) &EventBus_Py_type) < 0) {
		Py_DECREF(&EventBus_Py_type);
		Py_DECREF(m);
		return NULL;
	}
	PyModule_AddIntConstant(m, "version", 3);
	return m;
}

#if PY3K
    PyMODINIT_FUNC PyInit_CCshm_py3(void) { return CCshm_generic_init(); }
#else
	PyMODINIT_FUNC initCCshm_py2(void) { CCshm_generic_init(); }
#endif

};

