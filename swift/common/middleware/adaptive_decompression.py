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

def get_all(path, conn):
	count_rows = 0
	file_data = bytearray()
	file_length = 0
	
	with conn:
		cur = conn.cursor()
		query = (path,)
		
		for row in cur.execute('SELECT * FROM Data WHERE ID=? ORDER BY Chunk', query):
			count_rows = count_rows + 1
			chunk = row[2]
			for b in chunk:
				file_data.append(b)
			file_length = file_length + len(chunk)
		
		cur.execute('DELETE FROM DATA WHERE ID=?', query)
	
	if count_rows == 0:
		return None
	else:
		return (file_data, file_length)

def write_aux(path, conn):
	
	all_chunks = get_all(path, conn)
	
	if all_chunks is None:
		return None
	
	# Get the chunks from memory and rebuild file
	file_data, file_length = all_chunks
	
	body = bytearray(file_data)
	file_data_cmp = zlib.compress(buffer(body, 0, len(body)), 9)
	file_length_cmp = len(file_data_cmp)
	
	tmp_file = TemporaryFile()
	tmp_file.write(file_data_cmp)
	tmp_file.seek(0)
	
	return (tmp_file, file_length_cmp)

def write_async_proc(path, auth_token):
	conn = sqlite3.connect('/dev/shm/adapt.db')
	conn.execute('PRAGMA synchronous=OFF')
	conn.execute('PRAGMA journal_mode=MEMORY')
	conn.text_factory = str
	
	data = write_aux(path, conn)
	
	if data is None:
		return
	
	file_data, file_length = data
	
	headers = {"X-Auth-Token": auth_token, "Content-Length": file_length, "X-No-Compress": "1", "User-Agent": "AdaptiveMiddleware"}
	conn = httplib.HTTPConnection("127.0.0.1:8080")
	conn.request("PUT", path, file_data, headers)

class AdaptiveDecompressionMiddleware(object):
	
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
		
		self.conn = sqlite3.connect('/dev/shm/adapt.db')
		self.conn.execute('PRAGMA synchronous=OFF')
		self.conn.execute('PRAGMA journal_mode=MEMORY')
		self.conn.text_factory = str

		with self.conn:
			cur = self.conn.cursor()
			cur.execute('CREATE TABLE IF NOT EXISTS Data(ID TEXT, Chunk INT, Data TEXT)')
	
	def STORE(self, env):
		req = Request(env)
		path = req.path_qs
		chunk_index = int(req.headers.get('X-Chunk-Index'))
		
		info = Template('Detected STORE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		body = bytearray(env['wsgi.input'].read(req.message_length))
		
		# Inflage the chunk
		chunk = zlib.decompress(buffer(body, 0, len(body)))
		
		# Debug Info
		info = Template('$nchunk : $length')
		self.logger.debug(info.substitute(nchunk=chunk_index, length=len(chunk)))
		
		# Store the chunk in memory
		with self.conn:
			
			cur = self.conn.cursor()
			store = (path, chunk_index, chunk)
			cur.execute('INSERT INTO Data VALUES(?,?,?)', store)
		
		return Response(request=req, status=201)
	
	def WRITE_ASYNC(self, env):
		req = Request(env)
		path = req.path_qs
		auth_token = req.headers.get('X-Auth-Token')
		if auth_token is None:
			auth_token = req.headers.get('X-Storage-Token')
		
		info = Template('Detected WRITE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		p = Process(target=write_async_proc, args=(path, auth_token))
		p.start()
		return Response(request=req, status=201)
	
	def WRITE(self, env):
		req = Request(env)
		path = req.path_qs
		
		info = Template('Detected WRITE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		result = write_aux(path, self.conn)
		
		if result is None:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		# Get the chunks from memory and rebuild file
		file_data, file_length = result
		
		# Modify enviroment to include new file
		env['rebuilt_file'] = file_data
		env['rebuilt_file_size'] = file_length
		
		return self.app
	
	def COMPRESS(self, env):
		req = Request(env)
		body = bytearray(env['wsgi.input'].read(req.message_length))
		compressed = zlib.compress(buffer(body, 0, len(body)), 9)
		
		tmp_file = TemporaryFile()
		tmp_file.write(compressed)
		tmp_file.seek(0)
		
		env['rebuilt_file'] = tmp_file
		env['rebuilt_file_size'] = len(compressed)
		
		return self.app
	
	def VOID(self, env):
		self.logger.debug('Detected VOID request')
		return self.app
	
	def __call__(self, env, start_response):
		if env['REQUEST_METHOD'] != 'PUT':
			return self.app(env, start_response)
		
		req = Request(env)
		
		chunk_index = req.headers.get('X-Chunk-Index')
		to_write = req.headers.get('X-Write-To-Core')
		to_write_async = req.headers.get('X-Write-Async')
		no_compress = req.headers.get('X-No-Compress')
		
		version, account, container, obj = req.split_path(1, 4, True)
		if not obj:
			return self.app(env, start_response)
		
		handler = self.COMPRESS
		
		if to_write:
			handler = self.WRITE
		
		if to_write_async:
			handler = self.WRITE_ASYNC
		
		if chunk_index:
			handler = self.STORE
		
		if no_compress:
			handler = self.VOID
		
		return handler(env)(env, start_response)
		
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def adaptive_decompression_filter(app):
		return AdaptiveDecompressionMiddleware(app, conf)
	return adaptive_decompression_filter
