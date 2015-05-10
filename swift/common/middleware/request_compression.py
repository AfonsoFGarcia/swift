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
import zlib

class CompressionMiddleware(object):
	storage = {}
	
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
	
	def STORE(self, env):
		body = bytearray(env['wsgi.input'].read(req.message_length))
	
	def __call__(self, env, start_response):
		#if env['REQUEST_METHOD'] != 'GET':
		#	return self.app(env, start_response)
		
		#req = Request(env)
		
		#get_compressed = req.headers.get('X-Get-Compressed')
		
		#if not get_compressed:
		#	return self.app(env, start_response)
		
		#version, account, container, obj = req.split_path(1, 4, True)
		#if not obj:
		#	return self.app(env, start_response)
		
		def compress_response(status, headers):
			self.logger.debug(status);
			self.logger.debug(headers);
			return start_response(status, headers)
		
		return self.app(env, compress_response)
		
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def request_compression_filter(app):
		return CompressionMiddleware(app, conf)
	return request_compression_filter