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
import zlib

class AdaptiveDecompressionMiddleware(object):
	storage = {}
	
	def __init__(self, app):
		self.app = app
	
	def STORE(self, env):
		req = Request(env)
		path = req.path_qs
		
		if not path in self.__class__.storage:
			self.__class__.storage[path] = {}
		
		# Inflage the chunk
		chunk = zlib.decompress(env['wsgi.input'])
		
		# Store the chunk in memory
		chunk_index = req.headers.get('X-Chunk-Index')
		self.__class__.storage[path][chunk_index] = chunk
		
		return Response(request=req, status=202, body="OK", content_type="text/plain")
	
	def WRITE(self, env):
		req = Request(env)
		path = req.path_qs
		
		if not path in self.__class__.storage:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		# Do we really need this? Let's test.
		# Get the chunks from memory
		# Rebuild file
		
		# Modify request to contain rebuilt file
		env['wsgi.input'] = self.__class__.storage[path]
		del self.__class__.storage[path]
		
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
    def adaptive_decompression_filter(app):
        return AdaptiveDecompressionMiddleware(app)
    return adaptive_decompression_filter