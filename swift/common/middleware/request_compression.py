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

from swift.common.swob import Request, wsgify
from swift.common.utils import get_logger, split_path
from swift.common.http import is_success
import zlib

# Middleware class (compress download requests)
class CompressionMiddleware(object):
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
	
	@wsgify
	def __call__(self, req):
		obj = None
		
		# Only process if object
		try:
			(version, account, container, obj) = split_path(req.path_info, 4, 4, True)
		except ValueError:
			pass
		
		# Send request down the pipeline and get response
		resp = req.get_response(self.app)
		
		# Get request headers for compression
		get_compressed = req.headers.get('X-Get-Compressed')
		
		# If object was requested uncompressed, uncompress the object and modify the response
		if not get_compressed and obj and req.method == 'GET' and is_success(resp.status_int):
			body = bytearray(resp.body)
			def_body = zlib.decompress(buffer(body, 0, len(body)))
			resp.body = def_body
			resp.headers['Content-Length'] = len(def_body)
		
		# Continue pipeline
		return resp

# Factory (for egg)
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def request_compression_filter(app):
		return CompressionMiddleware(app, conf)
	return request_compression_filter