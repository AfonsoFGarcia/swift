# Copyright (c) 2015 Afonso Falardo Garcia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from swift.common.swob import Request, Response
from swift.common.utils import get_logger
from string import Template
from tempfile import TemporaryFile
from multiprocessing import Process
import zlib
import sqlite3
import httplib
import tarfile
import itertools

# Get offset of char in string
def find_offsets(string, char):
	offs = -1
	while True:
		offs = string.find(char, offs+1)
		if offs == -1:
			break
		else:
			yield offs

# Processes a bundle for saving all the elements
def process_tar(f, request_url, auth_token):
	# Read the tar bundle
	t = tarfile.open(mode='r', fileobj=f)
	
	# For each element
	for tarinfo in t:
		# Read the file
		ef = t.extractfile(tarinfo)
		content = ef.read()
		
		# Generate file path
		offs = find_offsets(request_url, "/")
		path = "%s/%s" % (request_url[next(itertools.islice(offs,2,3)):], tarinfo.name)
		
		# Send file to backend (will be compressed)
		send_uncompressed_file(path, auth_token, content, tarinfo.size)
	t.close()

# Fetches all chunks from the database
def get_all(path, conn, adaptstamp):
	count_rows = 0
	file_data = bytearray()
	file_length = 0
	
	with conn:
		cur = conn.cursor()
		query = (path, adaptstamp)
		
		# Get all chunks in order and rebuild them to memory
		for row in cur.execute('SELECT * FROM Data WHERE ID=? AND Adaptstamp=? ORDER BY Chunk', query):
			count_rows = count_rows + 1
			chunk = row[3]
			for b in chunk:
				file_data.append(b)
			file_length = file_length + len(chunk)
		
		# Delete chunks from the database
		cur.execute('DELETE FROM DATA WHERE ID=? AND Adaptstamp=?', query)
	
	# Assure no incorrect data is sent (if there are no chunks available)
	if count_rows == 0:
		return None
	else:
		return (file_data, file_length)

# Auxiliary function to get file/bundle and compress the file to store in memory
def write_aux(path, conn, is_bundle, adaptstamp):
	
	# Rebuild file
	all_chunks = get_all(path, conn, adaptstamp)
	
	# Assure no incorrect data is sent (if there are no chunks available)
	if all_chunks is None:
		return None
	
	file_data, file_length = all_chunks
	tmp_file = TemporaryFile()
	
	# Compress the data if it isn't a bundle
	if is_bundle:
		tmp_file.write(file_data)
		tmp_file.seek(0)
		return (tmp_file, file_length)
	else:
		file_data_cmp = zlib.compress(buffer(file_data, 0, len(file_data)), 9)
		file_length_cmp = len(file_data_cmp)
		tmp_file.write(file_data_cmp)
		tmp_file.seek(0)
		return (tmp_file, file_length_cmp)

# Sends a file that was previously compressed to the backend in a new HTTP request
def send_file(path, auth_token, file_data, file_length):
	headers = {"X-Auth-Token": auth_token, "Content-Length": file_length, "X-No-Compress": "1", "User-Agent": "AdaptiveMiddleware"}
	send_file_aux(path, file_data, headers)

# Sends a file for compression to the backend in a new HTTP request
def send_uncompressed_file(path, auth_token, file_data, file_length):
	headers = {"X-Auth-Token": auth_token, "Content-Length": file_length, "User-Agent": "AdaptiveMiddleware"}
	send_file_aux(path, file_data, headers)

# Sends a file to the backend in a new HTTP request
def send_file_aux(path, file_data, headers):
	processed_path = path.replace(" ", "%20")
	conn = httplib.HTTPConnection("127.0.0.1:8080")
	conn.request("PUT", processed_path, file_data, headers)
	r = conn.getresponse()
	data = r.read()
	conn.close()

# Asynchronous process to write files to the backend (when using adaptive compression)
def write_async_proc(path, auth_token, is_bundle, request_url, adaptstamp):
	# Connect to SQLite database
	conn = sqlite3.connect('/dev/shm/adapt.db')
	conn.execute('PRAGMA synchronous=OFF')
	conn.execute('PRAGMA journal_mode=MEMORY')
	conn.text_factory = str
	
	# Get data for the request
	data = write_aux(path, conn, is_bundle, adaptstamp)
	
	# Assure no incorrect data is sent (if there are no chunks available)
	if data is None:
		return
	
	file_data, file_length = data
	
	# Process tar if the data is a bundle or send if it is a file
	if is_bundle:
		process_tar(file_data, request_url, auth_token)
	else:
		send_file(path, auth_token, file_data, file_length)

# Asynchronous process to write files to the backend (when using the normal method)
def compress_async_proc(path, auth_token, body, is_bundle, request_url):
	# Process tar if the data is a bundle or compress and send it if it is a file
	if is_bundle:
		file_data = TemporaryFile()
		file_data.write(body)
		file_data.seek(0)
		
		process_tar(file_data, request_url, auth_token)
	else:
		compressed = zlib.compress(buffer(body, 0, len(body)), 9)
		
		file_length = len(compressed)
		file_data = TemporaryFile()
		file_data.write(compressed)
		file_data.seek(0)
		
		send_file(path, auth_token, file_data, file_length)

# Middleware class (process upload requests)
class AdaptiveDecompressionMiddleware(object):
	
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
		
		# Connect to SQLite database
		self.conn = sqlite3.connect('/dev/shm/adapt.db')
		self.conn.execute('PRAGMA synchronous=OFF')
		self.conn.execute('PRAGMA journal_mode=MEMORY')
		self.conn.text_factory = str
		
		# Create database table if it doesn't exist
		with self.conn:
			cur = self.conn.cursor()
			cur.execute('CREATE TABLE IF NOT EXISTS Data(ID TEXT, Adaptstamp TEXT, Chunk INT, Data TEXT)')
	
	# STORE method: stores a chunk using synchronous methods (used with adaptive compression)
	def STORE(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Bundles are not processed synchronously
		if is_bundle:
			return Response(request=req, status=500, body="No bundles through STORE", content_type="text/plain")
		
		# Get chunk information
		path = req.path_qs
		chunk_index = int(req.headers.get('X-Chunk-Index'))
		
		info = Template('Detected STORE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		# Read chunk
		body = bytearray(env['wsgi.input'].read(req.message_length))
		
		# Inflage the chunk
		chunk = zlib.decompress(buffer(body, 0, len(body)))
		
		info = Template('$nchunk : $length')
		self.logger.debug(info.substitute(nchunk=chunk_index, length=len(chunk)))
		
		# Store the chunk in memory
		with self.conn:
			cur = self.conn.cursor()
			store = (path, adaptstamp, chunk_index, chunk)
			cur.execute('INSERT INTO Data VALUES(?,?,?,?)', store)
		
		return Response(request=req, status=201)
	
	# WRITE method: writes a file/bundle to the backend asynchronously (used with adaptive compression)
	def WRITE_ASYNC(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Get file/bundle information
		path = req.path_qs
		
		# Get authentication token
		auth_token = req.headers.get('X-Auth-Token')
		if auth_token is None:
			auth_token = req.headers.get('X-Storage-Token')
		
		info = Template('Detected WRITE ASYNC request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		# Start process
		p = Process(target=write_async_proc, args=(path, auth_token, is_bundle, request_url, adaptstamp))
		p.start()
		
		# Stop pipeline
		return Response(request=req, status=201)
	
	# WRITE method: writes a file/bundle to the backend (used with adaptive compression)
	def WRITE(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Bundles are not processed synchronously
		if is_bundle:
			return Response(request=req, status=500, body="No bundles through SYNC methods", content_type="text/plain")
		
		# Get file information
		path = req.path_qs
		
		info = Template('Detected WRITE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		# Rebuild file
		result = write_aux(path, self.conn, is_bundle, adaptstamp)
		
		# No chunks available
		if result is None:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		file_data, file_length = result
		
		# Modify enviroment to include new file (requires modified proxy server)
		env['rebuilt_file'] = file_data
		env['rebuilt_file_size'] = file_length
		
		# Continue pipeline
		return self.app
	
	# COMPRESS method: compresses a file and stores it compressed in the backend
	def COMPRESS(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Bundles are not processed asynchronously
		if is_bundle:
			return Response(request=req, status=500, body="No bundles through SYNC methods", content_type="text/plain")
		
		# Read file and ocmpress
		body = bytearray(env['wsgi.input'].read(req.message_length))
		compressed = zlib.compress(buffer(body, 0, len(body)), 9)
		
		# Write compressed data to file (needed for the rest of the pipeline)
		tmp_file = TemporaryFile()
		tmp_file.write(compressed)
		tmp_file.seek(0)
		
		# Modify enviroment to include new file (requires modified proxy server)
		env['rebuilt_file'] = tmp_file
		env['rebuilt_file_size'] = len(compressed)
		
		# Continue pipeline
		return self.app
	
	# COMPRESS method: compresses a file and stores it compressed in the backend asynchronously
	def COMPRESS_ASYNC(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Get file/bundle information
		path = req.path_qs
		
		# Get authentication token
		auth_token = req.headers.get('X-Auth-Token')
		if auth_token is None:
			auth_token = req.headers.get('X-Storage-Token')
		
		info = Template('Detected ASYNC request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		# Read file/bundle
		body = bytearray(env['wsgi.input'].read(req.message_length))
		
		# Start process
		p = Process(target=compress_async_proc, args=(path, auth_token, body, is_bundle, request_url))
		p.start()
		
		# Stop pipeline
		return Response(request=req, status=201)
	
	# VOID method: do not process data
	def VOID(self, env, is_bundle, request_url, adaptstamp):
		req = Request(env)
		
		# Bundles are not processed asynchronously
		if is_bundle:
			return Response(request=req, status=500, body="No bundles through VOID", content_type="text/plain")
		
		self.logger.debug('Detected VOID request')
		
		# Continue pipeline
		return self.app
	
	# Execution of middleware code
	def __call__(self, env, start_response):
		# Only process PUT requests
		if env['REQUEST_METHOD'] != 'PUT':
			return self.app(env, start_response)
		
		req = Request(env)
		
		# Get request information for adaptive compression, bundling and asynchronous behaviour
		chunk_index = req.headers.get('X-Chunk-Index')
		to_write = req.headers.get('X-Write-To-Core')
		to_write_async = req.headers.get('X-Write-Async')
		no_compress = req.headers.get('X-No-Compress')
		is_bundle = req.headers.get('X-Bundle')
		request_url = req.headers.get('X-Request-URL')
		adaptstamp = req.headers.get('X-Adaptstamp')		
		
		# Only operate over objects
		version, account, container, obj = req.split_path(1, 4, True)
		if not obj:
			return self.app(env, start_response)
		
		# Default behaviour
		handler = self.COMPRESS
		
		# Synchronous write (store chunks in backend)
		if to_write:
			handler = self.WRITE
		
		# Asynchronous requests
		if to_write_async:
			handler = self.COMPRESS_ASYNC
			
			# Asynchronous write (store chunks in backend)
			if to_write:
				handler = self.WRITE_ASYNC
		
		# Store chunks in the database
		if chunk_index:
			handler = self.STORE
		
		# Previously compressed file
		if no_compress:
			handler = self.VOID
		
		return handler(env, is_bundle, request_url, adaptstamp)(env, start_response)

# Factory (for egg)
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def adaptive_decompression_filter(app):
		return AdaptiveDecompressionMiddleware(app, conf)
	return adaptive_decompression_filter
