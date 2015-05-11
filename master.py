import threading
try:
	import cPickle as pickle
except:
	import pickle
import zmq

class MasterThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		
		self.execute = True
		self.storage = {}
		self.new_uid = 1
		
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.REP)
		self.socket.bind("tcp://*:50000")
	
	def run(self):
		while self.execute:
			req, object_id, uid = self.socket.recv_multipart()
			
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
			
			self.socket.send(message)
	
	def kill(self):
		self.execute = False
		
if __name__ == "__main__":
	thread = MasterThread()
	try:
		thread.start()
	except KeyboardInterrupt:
		thread.kill()
	finally:
		thread.join()