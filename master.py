import threading
try:
	import cPickle as pickle
except:
	import pickle
import zmq

class MasterThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		
		self.storage = {}
		self.new_uid = 1
		
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REP)
		self.socket.bind("ipc://master")
	
	def run(self):
		while True:
			req, object_id, uid = self.socket.recv_multipart()
			
			print("Received %s %s %s" % (req, object_id, uid))
			
			message = "404"
			
			if req == "UID":
				message = str(self.new_uid)
				self.new_uid = self.new_uid + 1
			elif req == "GET":
				if object_id not in self.storage:
					message = pickle.dumps([])
				else:
					message = pickle.dumps(self.storage[object_id])
			elif req == "PUT":
				if object_id not in self.storage:
					self.storage[object_id] = []
				self.storage[object_id].append(int(uid))
				message = "OK"
			elif req == "DEL":
				del self.storage[object_id]
				message = "OK"
			
			print("Replying %s" % message)
			
			self.socket.send(message)
		
if __name__ == "__main__":
	thread = MasterThread()
	thread.daemon = True
	thread.start()
	try:
		while True:
			pass
	except KeyboardInterrupt:
		pass