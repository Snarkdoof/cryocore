import CCshm_py3
import time
import threading
import traceback

bus = CCshm_py3.EventBus(".ccshm-shared-mem-id")

class test:
	def __init__(self):
		self.x = "hei"
		print("Test construct")
		def mythread():
			print("wtf")
			try:
				print(self.x)
			except:
				traceback.print_exc()
		print("Starting thread")
		t = threading.Thread(target=mythread, args=(), daemon=True)
		t.start()
		print("done")

x = test()
print("Now what")

def getter():
	while True:
		print("Waiting")
		result = bus.get()
		print(result.decode("utf-8"))
		del result
	
t = threading.Thread(target=getter, daemon=True)
t.start()

try:
	while True:
		bus.post("The time is %.2f\n" % (time.time()))
		time.sleep(0.5)
		print("Posted..")
except:
	print("Exiting")
	raise SystemExit

	
