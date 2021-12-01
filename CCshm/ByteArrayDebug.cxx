/*	ByteArrayDebug.cxx
	(c) 2019 Daniel Stodle, dsto@norceresearch.no
	
	Code to figure out why python is crashing when zero-copy transfering data from C to Python.
*/

#define PY_SSIZE_T_CLEAN
#ifdef __APPLE__
#include <Python/Python.h>
#include <Python/structmember.h>
#else
#include <Python.h>
#include <structmember.h>
#endif
#include <vector>


#if PY_MAJOR_VERSION >= 3
#define PY3K 1
#else
#define PY3K 0
#endif

extern "C" {

typedef struct {
	PyObject_HEAD
	int field;
} DBG_Py;

static PyObject *DBG_Py_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	DBG_Py *self;
	self = (DBG_Py *) type->tp_alloc(type, 0);
	if (self != NULL) {
		
	}
	return (PyObject *) self;
}

static void	DBG_Py_dealloc(DBG_Py *self) {
	Py_TYPE(self)->tp_free((PyObject *) self);
}

static int DBG_Py_init(DBG_Py *self, PyObject *args, PyObject *kwds) {
	return 0;
}

static PyMemberDef DBG_Py_members[] = {
	{NULL}  /* Sentinel */
};

static PyObject*	PyByteArray_FromMemoryTransferOwnership(void *data, Py_ssize_t len) {
	#if 0
	PyObject *obj2 = PyByteArray_FromStringAndSize((const char*)data, len);
	free(data);
	return obj2;
	#endif
	
	PyByteArrayObject *obj;
    Py_ssize_t alloc;

    obj = PyObject_New(PyByteArrayObject, &PyByteArray_Type);
    if (obj == NULL)
        return NULL;

	alloc = len + 1;
	printf("Byte array object of length %zd has ptr %p\n", len, data);
	obj->ob_bytes = (char*)data;
	obj->ob_bytes[len] = '\0';
    Py_SIZE(obj) = len;
    obj->ob_alloc = alloc;
    #if PY3K
    obj->ob_start = obj->ob_bytes;
    #endif
    obj->ob_exports = 0;
	return (PyObject *)obj;
}

struct DBGData {
	uint8_t	*data;
	Py_ssize_t len;
};

static PyObject*	DBG_Py_get_many(DBG_Py *self, PyObject *args) {
	std::vector<DBGData>	items;
	
	Py_BEGIN_ALLOW_THREADS;
	
	for (Py_ssize_t i=0;i<5;i++) {
		void 	*ptr = malloc((i+1)*23 + 1);
		DBGData d = { (uint8_t*)ptr, (i+1)*23 };
		printf("Allocated: %p [%zd bytes]\n", d.data, d.len);
		//for (size_t j=0;j<d.len;j++)
		// 	d.data[j] = j&0xff;
		items.push_back(d);
	}
	Py_END_ALLOW_THREADS;
	
	PyObject	*list = 0;
	if (items.size() > 0) {
		list = PyList_New(items.size());
		for (size_t i=0;i<items.size();i++) {
			PyObject	*bytes = PyByteArray_FromMemoryTransferOwnership(items[i].data, items[i].len);
			PyList_SetItem(list, i, bytes);
		}
	}
	else
		list = PyList_New(0);
	
	return list;
}


static PyMethodDef DBG_Py_methods[] = {
	{"get_many", (PyCFunction) DBG_Py_get_many, METH_NOARGS, "Get as many messages from bus as possible. Blocks until at least one message is available." },
	{NULL}  /* Sentinel */
};

static PyTypeObject DBG_Py_type = {
	PyVarObject_HEAD_INIT(NULL, 0)
};

#if PY3K
static PyModuleDef CCshm_py3_module = {
	PyModuleDef_HEAD_INIT,
	.m_name = "ByteArrayDebug_py3",
	.m_doc = "CryoCore Shared Memory.",
	.m_size = -1,
};
#endif


static void init_DBG_type() {
	//	This is a workaround for older versions of GCC that complain about non-trivial designated initializer lists.
	#if PY3K
	DBG_Py_type.tp_name = "ByteArrayDebug_py3.dbg";
	#else
	DBG_Py_type.tp_name = "ByteArrayDebug.dbg";
	#endif
	DBG_Py_type.tp_doc = "ByteArrayDebug";
	DBG_Py_type.tp_basicsize = sizeof(DBG_Py);
	DBG_Py_type.tp_itemsize = 0;
	DBG_Py_type.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
	DBG_Py_type.tp_new = DBG_Py_new;
	DBG_Py_type.tp_init = (initproc) DBG_Py_init;
	DBG_Py_type.tp_dealloc = (destructor) DBG_Py_dealloc;
	DBG_Py_type.tp_members = DBG_Py_members;
	DBG_Py_type.tp_methods = DBG_Py_methods;
}

static PyObject *CCshm_generic_init(void) {
	PyObject *m;
	
	init_DBG_type();
	
	if (PyType_Ready(&DBG_Py_type) < 0)
		return NULL;
	
	#if PY3K
	m = PyModule_Create(&CCshm_py3_module);
	if (m == NULL)
		return NULL;
	#else
	m = Py_InitModule3("ByteArrayDebug", 0, "CryoCore Shared Memory.");
	#endif

	Py_INCREF(&DBG_Py_type);
	if (PyModule_AddObject(m, "dbg", (PyObject *) &DBG_Py_type) < 0) {
		Py_DECREF(&DBG_Py_type);
		Py_DECREF(m);
		return NULL;
	}
	PyModule_AddIntConstant(m, "version", 2);
	return m;
}

#if PY3K
    PyMODINIT_FUNC PyInit_CCshm_py3(void) { return CCshm_generic_init(); }
#else
	PyMODINIT_FUNC initByteArrayDebug(void) { CCshm_generic_init(); }
#endif

};

