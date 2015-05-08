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
from tempfile import TemporaryFile
from string import Template
import zlib

global storage = {}

class AdaptiveDecompressionMiddleware(object):
		
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf, log_route="adaptdecomp")
	
	def STORE(self, env):
		req = Request(env)
		path = req.path_qs
		chunk_index = int(req.headers.get('X-Chunk-Index'))
		
		info = Template('Detected STORE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		if not path in storage:
			storage[path] = {}
		
		body = bytearray(env['wsgi.input'].read(req.message_length))
		
		# Inflage the chunk
		chunk = bytearray(zlib.decompress(buffer(body, 0, len(body))))
		
		# Debug Info
		info = Template('$nchunk : $length')
		self.logger.debug(info.substitute(nchunk=chunk_index, length=len(chunk)))
		
		# Store the chunk in memory
		storage[path][chunk_index] = chunk
		
		return Response(request=req, status=201)
	
	def WRITE(self, env):
		req = Request(env)
		path = req.path_qs
		
		info = Template('Detected WRITE request: $rpath')
		self.logger.debug(info.substitute(rpath=path))
		
		if not path in storage:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		n_chunks = int(env['wsgi.input'].read(req.message_length))
		
		# Get the chunks from memory and rebuild file
		
		file_data = TemporaryFile()
		file_length = 0
		
		for x in range(0, n_chunks):
			info = Template('Is key $key in dict? $val')
			self.logger.debug(info.substitute(key=x, val=(x in storage[path])))
			file_data.write(storage[path][x])
			file_length = file_length + len(storage[path][x])
		
		self.logger.debug(file_length)
		
		# Modify request to contain rebuilt file
		
		env['wsgi.input'] = file_data
		req.headers['Content-Length'] = file_length
		del storage[path]
		
		return self.app
	
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