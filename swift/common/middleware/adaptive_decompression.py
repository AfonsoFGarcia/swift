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
import zlib
import sqlite3

class AdaptiveDecompressionMiddleware(object):
	
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
		
		conn = sqlite3.connect('/dev/shm/adapt.db')
		
		with conn:
			cur = conn.cursor()
			cur.execute('CREATE TABLE IF NOT EXISTS Data(ID TEXT, Chunk INT, Data TEXT)')
			cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS DataIndex ON Data(ID, Chunk)')
			conn.commit()
		
		conn.close()
	
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
		conn = sqlite3.connect('/dev/shm/adapt.db')
		
		with conn:
			conn.text_factory = str
			cur = conn.cursor()
			store = (path, chunk_index, chunk)
			cur.execute('INSERT INTO Data VALUES(?,?,?)', store)
			conn.commit()
		
		conn.close()
		
		return Response(request=req, status=201)
	
	def WRITE(self, env):
		req = Request(env)
		path = req.path_qs
		
		info = Template('Detected WRITE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		all_chunks = self.get_all(path)
		
		if not all_chunks:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		# Get the chunks from memory and rebuild file
		file_data, file_length = all_chunks
		
		self.logger.debug(file_length)
		
		tmp_file = TemporaryFile()
		tmp_file.write(file_data)
		tmp_file.seek(0)
		
		# Modify enviroment to include new file
		env['rebuilt_file'] = tmp_file
		env['rebuilt_file_size'] = file_length
		
		return self.app
	
	def get_all(self, path):
		conn = sqlite3.connect('/dev/shm/adapt.db')
		count_rows = 0
		file_data = bytearray()
		file_length = 0
		
		with conn:
			conn.text_factory = str
			cur = conn.cursor()
			query = (path,)
			
			for row in cur.execute('SELECT * FROM Data WHERE ID=? ORDER BY Chunk', query):
				chunk = row[2]
				for b in chunk:
					file_data.append(b)
				file_length = file_length + len(chunk)
			count_rows = cur.rowcount
			
			cur.execute('DELETE FROM DATA WHERE ID=?', query)
			conn.commit()
		
		conn.close()
		
		if count_rows <= 0:
			return None
		else:
			return (file_data, file_length)
	
	def __call__(self, env, start_response):
		if env['REQUEST_METHOD'] != 'PUT':
			return self.app(env, start_response)
		
		req = Request(env)
		
		chunk_index = req.headers.get('X-Chunk-Index')
		to_write = req.headers.get('X-Write-To-Core')
		
		if not chunk_index and not to_write:
			return self.app(env, start_response)
		
		version, account, container, obj = req.split_path(1, 4, True)
		if not obj:
			return self.app(env, start_response)
		
		handler = None
		
		if to_write:
			handler = self.WRITE
		
		if chunk_index:
			handler = self.STORE
		
		return handler(env)(env, start_response)
		
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def adaptive_decompression_filter(app):
		return AdaptiveDecompressionMiddleware(app, conf)
	return adaptive_decompression_filter