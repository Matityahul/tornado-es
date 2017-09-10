# -*- coding: utf-8 -*-
from tornado import gen
from tornado.gen import coroutine, Return

from tornadoes_ext.models import BulkList

from six.moves.urllib.parse import urlencode, urlparse
from tornado.escape import json_decode
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.concurrent import return_future

from tornadoes_ext.serializer import json_dumps

SLEEP_TIME_BETWEEN_RETRIES = 0.02
ATTEMPTS_NUMBER = 3


class ESConnection(object):
    _MATCH_ALL_QUERY = {"query": {"match_all": {}}}

    def __init__(self, host='localhost', port='9200', io_loop=None, protocol='http', custom_client=None,
                 max_attempts=ATTEMPTS_NUMBER, retry_on_timeout=False, http_request_kwargs=None):
        self.io_loop = io_loop or IOLoop.instance()
        self.url = "%(protocol)s://%(host)s:%(port)s" % {"protocol": protocol, "host": host, "port": port}
        self.bulk = BulkList()
        self.client = custom_client or AsyncHTTPClient(self.io_loop)
        self.max_attempts = max_attempts
        self.retry_on_timeout = retry_on_timeout

        if max_attempts < 1:
            raise ValueError("max_attempts must be 1 or higher")

        # extra kwargs passed to tornado's HTTPRequest class e.g. request_timeout
        self.http_request_kwargs = http_request_kwargs or {}

    @staticmethod
    def _create_query_string(params):
        """
        Support Elasticsearch 5.X
        """
        parameters = params or {}

        for param, value in parameters.items():
            param_value = str(value).lower() if isinstance(value, bool) else value
            parameters[param] = param_value

        return urlencode(parameters)

    @coroutine
    def _fetch(self, request, **kwargs):
        for i in range(self.max_attempts):
            response = yield self.client.fetch(request, raise_error=False, **kwargs)

            if response.error:
                retry = response.code >= 500 or (self.retry_on_timeout and response.code in (408, 429,))

                # Number of retries was exceeded, or error is a server error,
                # but not 408-Request Timeout nor 429-Too Many Requests
                if not retry or i + 1 == self.max_attempts:
                    raise Return(response)

                yield gen.sleep(SLEEP_TIME_BETWEEN_RETRIES)
            else:
                raise Return(response)

    @classmethod
    def from_uri(cls, uri, io_loop=None, custom_client=None, http_request_kwargs=None):
        parsed = urlparse(uri)

        if not parsed.hostname or not parsed.scheme:
            raise ValueError('Invalid URI')

        return cls(host=parsed.hostname, protocol=parsed.scheme, port=parsed.port, io_loop=io_loop,
                   custom_client=custom_client, http_request_kwargs=http_request_kwargs)

    @staticmethod
    def create_path(method, index='_all', type='', **kwargs):
        query_string = ESConnection._create_query_string(kwargs)

        path = "/%(index)s/%(type)s/_%(method)s" % {"method": method, "index": index, "type": type}

        if query_string:
            path += '?' + query_string

        return path

    @return_future
    def search(self, callback, index='_all', type='', source=None, **kwargs):
        source = json_dumps(source or self._MATCH_ALL_QUERY)
        path = self.create_path("search", index=index, type=type, **kwargs)

        self.post_by_path(path, callback, source)

    def multi_search(self, index, source):
        self.bulk.add(index, source)

    @return_future
    def apply_search(self, callback, params=None):
        params = params or {}
        path = "/_msearch"
        if params:
            path = "%s?%s" % (path, urlencode(params))
        source = self.bulk.prepare_search()
        self.post_by_path(path, callback, source=source)

    @return_future
    def post_by_path(self, path, callback, source):
        url = '%(url)s%(path)s' % {"url": self.url, "path": path}
        request_http = HTTPRequest(url, method="POST", body=source, **self.http_request_kwargs)
        self._fetch(request=request_http, callback=callback)

    @return_future
    def get_by_path(self, path, callback):
        url = '%(url)s%(path)s' % {"url": self.url, "path": path}
        self._fetch(url, callback=callback, **self.http_request_kwargs)

    @return_future
    def get(self, index, type, uid, callback, parameters=None):
        def to_dict_callback(response):
            source = json_decode(response.body)
            callback(source)
        self.request_document(index, type, uid, callback=to_dict_callback, parameters=parameters)

    @return_future
    def put(self, index, type, uid, contents, parameters=None, callback=None):
        self.request_document(
            index, type, uid, "PUT", body=json_dumps(contents),
            parameters=parameters, callback=callback)

    @return_future
    def update(self, index, type, uid, contents, callback=None):
        path = "/%(index)s/%(type)s/%(uid)s/_update" % {
            "index": index,
            "type": type,
            "uid": uid
        }

        partial = { "doc": contents }

        self.post_by_path(path, callback, source=json_dumps(partial))

    @return_future
    def delete(self, index, type, uid, parameters=None, callback=None):
        self.request_document(index, type, uid, "DELETE", parameters=parameters, callback=callback)

    @return_future
    def count(self, index="_all", type='', source='', parameters=None, callback=None):
        """
        The query can either be provided using a simple query string as a parameter 'q',
        or using the Query DSL defined within the request body (source).
        Notice there are additional query string parameters that could be added only with the first option.
        """
        parameters = parameters or {}

        path = self.create_path('count', index=index, type=type, **parameters)

        if source:
            source = json_dumps(source)

        self.post_by_path(path=path, callback=callback, source=source)

    @return_future
    def request_document(self, index, type, uid, method="GET", body=None, parameters=None, callback=None):
        query_string = ESConnection._create_query_string(parameters)

        path = '/{index}/{type}/{uid}'.format(**locals())
        url = '%(url)s%(path)s?%(querystring)s' % {
            "url": self.url,
            "path": path,
            "querystring": query_string
        }
        request_arguments = dict(self.http_request_kwargs)
        request_arguments['method'] = method

        if body is not None:
            request_arguments['body'] = body

        request = HTTPRequest(url, **request_arguments)
        self._fetch(request, callback=callback)
