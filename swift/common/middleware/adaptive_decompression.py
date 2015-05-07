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

class AdaptiveDecompressionMiddleware(object):
	__storage = {}
	
	def __init__(self, app, conf):
		self.app = app
	
	def STORE(self, req, path):
		if not path in __storage:
			__storage[path] = {}
		
		# Get the chunk
		# Inflage the chunk
		# Store the chunk in memory
		
		return Response(request=req, body="OK", content_type="text/plain")
	
	def WRITE(self, req, path):
		if not path __storage:
			return Response(request=req, status=404, body="No chunks found", content_type="text/plain")
		
		# Get the chunks from memory
		# Rebuild file
		# Modify request to contain rebuilt file
		
		return self.app
	
	def __call__(self, env, start_response):
		if env['REQUEST_METHOD'] != 'PUT':
			return self.app(env, start_response)
		
		req = Request(env)
		
		chunk_index = req.headers.get('X-Chunk-Index')
		to_write = req.headers.get('X-Write-To-Core')
		
		if not chunk_index or not to_write:
			return self.app(env, start_response)
		
		version, account, container, obj = req.split_path(1, 4, True)
		if not obj:
			return self.app(env, start_response)
			
		path = req.path_qs
		
		handler = None
		
		if to_write:
			handler = self.WRITE
		
		if chunk_index:
			handler = self.STORE
		
		return handler(req, path)(env, start_response)