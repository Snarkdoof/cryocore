import ByteArrayDebug
import time
import threading
import queue

try:
	from pymavlink import mavutil
except:
	print("No pymavlink!")

class mavshm(mavutil.mavfile):
	def __init__(self,
				 device,
				 autoreconnect=False,
				 source_system=255,
				 source_component=0,
				 retries=6,
				 use_native=False, stop_event=None):
		self.dbg = ByteArrayDebug.dbg()
		self.queue = queue.Queue()
		self.stop_event = stop_event
		mavutil.mavfile.__init__(self, None, "shm:" + device, source_system=source_system, source_component=source_component, use_native=use_native)
		try:
			self.thread = threading.Thread(target=self.shm_read, daemon=True)
		except:
			self.thread = threading.Thread(target=self.shm_read)
			self.thread.daemon = True
		self.thread.start()
		
	def close(self):
		print("Not closing")
	
	def _shm_read(self):
		data = self.dbg.get_many()
		if data and len(data) > 0:
			first = None
			for item in data:
				if not first:
					first = item
				else:
					first += item
			return first
		return None
	
	def shm_read(self):
		print("Reader thread up and running %s", self)
		while True:
			data = self._shm_read()
			if data:
				self.queue.put(data)
		print("Reader thread exiting")
	
	def recv(self,n=None):
		while True:
			try:
				data = self.queue.get(True, 0.1)
				return data
			except queue.Empty:
				pass

	def write(self, buf):
		# We assume that we always get a COMPLETE PACKET to write here. Otherwise,
		# bugs will occur if there are multiple writers, since the streams may be crossed
		# at the receiving end.
		pass

def dummy():
	def do_it_many_times(i):
		x = d.get_many()
		if i % 3:
			d.get_many()
		if i % 2:
			d.get_many()
		if i % 7:
			d.get_many()
		y = x[0]
		for i in range(1, len(x)):
			y += x[i]
		return y

	i=0
	while True:
		result = do_it_many_times(i)
		result[0] = b'0'
		time.sleep(0.1)
		print(i)
		i += 1

connection = mavshm("shm")
while True:
	msg = connection.recv_msg()

