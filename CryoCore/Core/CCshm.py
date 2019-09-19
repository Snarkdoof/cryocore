import sys

EventBus = None

if sys.version_info[0] >= 3:
	try:
		import CCshm_py3
		EventBus = CCshm_py3.EventBus
	except:
		import traceback
		print("Shared memory module for py3 not found!")
		traceback.print_exc()
else:
	try:
		import CCshm_py2
		EventBus = CCshm_py2.EventBus
	except:
		import traceback
		print("Shared memory module for py2 not found!")
		traceback.print_exc()
