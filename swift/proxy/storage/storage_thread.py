import threading
import zmq
try:
	import cPickle as pickle
except:
	import pickle

class StorageThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.storage = {}
		self.context = zmq.Context()
		
		self.master_socket = self.context.socket(zmq.REQ)
		self.master_socket.connect("tcp://localhost:50000")
		self.master_socket.send_multipart(["UID"])
		self.uid = int(self.master_socket.recv())
	
		self.socket = self.context.socket(zmq.REP)
		self.socket.bind("tcp://*:%s" % str(50000 + self.uid))
	
	def run(self):
		while self.execute:
			req, object_id = self.socket.recv()
			message = "404"
			
			if req == "GET":
				message = pickle.dumps(self.get(object_id))
			
			self.socket.send(message)
	
	def put(self, object_id, chunk, data):
		if not object_id in self.storage:
			self.storage[object_id] = {}
			self.master_socket.send_multipart(["PUT", object_id, str(self.uid]))
		self.storage[object_id][chunk] = data
		
	def get(self, object_id):
		if not object_id in self.storage:
			return {}
		else:
			obj = self.storage[object_id]
			del self.storage[object_id]
			return obj
	
	def get_all(self, object_id):
		self.master_socket.send_multipart(["GET", object_id])
		message = self.master_socket.recv()
		server_list = pickle.loads(message)
		
		all_pieces = get(object_id)
		
		for uid in server_list:
			if self.uid == uid:
				continue
			stor_socket = self.context.socket(zmq.REQ)
			stor_socket.connect("tcp://localhost:%s" % str(50000 + uid))
			stor_socket.send_multipart(["GET", object_id])
			pieces = pickle.loads(stor_socket.recv())
			all_pieces.update(pieces)
		
		self.master_socket.send_multipart(["DEL", object_id])
		
		return all_pieces
		