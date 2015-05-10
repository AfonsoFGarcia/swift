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

class CompressionMiddleware(object):
	storage = {}
	
	def __init__(self, app, conf):
		self.app = app
		self.logger = get_logger(conf)
	
	@wsgify
	def __call__(self, req):
		obj = None
		
		try:
			(version, account, container, obj) = split_path(req.path_info, 4, 4, True)
		except ValueError:
			pass
		
		resp = req.get_response(self.app)
		get_compressed = req.headers.get('X-Get-Compressed')
		
		if get_compressed and obj and req.method == 'GET' and is_success(resp.status_int):
			body = bytearray(resp.body);
			def_body = zlib.compress(buffer(body, 0, len(body)))
			resp.body = def_body
			resp.headers['Content-Length'] = len(def_body)
			
		return resp
		
def filter_factory(global_conf, **local_conf):
	conf = global_conf.copy()
	conf.update(local_conf)
	
	def request_compression_filter(app):
		return CompressionMiddleware(app, conf)
	return request_compression_filter