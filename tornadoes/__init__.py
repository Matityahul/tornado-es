# -*- coding: utf-8 -*-
from tornado.gen import coroutine, Return

from tornadoes.models import BulkList

from six.moves.urllib.parse import urlencode, urlparse
from tornado.escape import json_encode, json_decode
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest


class ESConnection(object):
    __MATCH_ALL_QUERY = {"query": {"match_all": {}}}

    # TODO : timeout, max_retries, retry_on_timeout
    def __init__(self, host='localhost', port='9200', io_loop=None, protocol='http', custom_client=None,
                 http_request_kwargs=None):
        self.io_loop = io_loop or IOLoop.instance()
        self.url = "%(protocol)s://%(host)s:%(port)s" % {"protocol": protocol, "host": host, "port": port}
        self.bulk = BulkList()
        self.client = custom_client or AsyncHTTPClient(self.io_loop)

        # extra kwargs passed to tornado's HTTPRequest class e.g. request_timeout
        self.http_request_kwargs = http_request_kwargs or {}

    @classmethod
    def from_uri(cls, uri, io_loop=None, custom_client=None, http_request_kwargs=None):
        parsed = urlparse(uri)

        if not parsed.hostname or not parsed.scheme:
            raise ValueError('Invalid URI')

        return cls(host=parsed.hostname, protocol=parsed.scheme, port=parsed.port, io_loop=io_loop,
                   custom_client=custom_client, http_request_kwargs=http_request_kwargs)

    @staticmethod
    def create_path(method, index='_all', doc_type='', **kwargs):
        parameters = {}

        for param, value in kwargs.items():
            parameters[param] = value

        path = "/%(index)s/%(type)s/_%(method)s" % {"method": method, "index": index, "type": doc_type}

        if parameters:
            path += '?' + urlencode(parameters)

        return path

    @coroutine
    def search(self, index='_all', doc_type='', body=None, **kwargs):
        path = self.create_path('search', index, doc_type, **kwargs)

        body = body or self.__MATCH_ALL_QUERY
        response = yield self.post_by_path(path, body)

        raise Return(response)

    def multi_search(self, index, body):
        self.bulk.add(index, body)

    @coroutine
    def apply_search(self, params=None):
        params = params or {}
        path = '/_msearch'

        if params:
            path = "%s?%s" % (path, urlencode(params))

        body = self.bulk.prepare_search()

        response = yield self.post_by_path(path, body)
        raise Return(response)

    @coroutine
    def post_by_path(self, path, body):
        url = '%(url)s%(path)s' % {"url": self.url, "path": path}
        request_http = HTTPRequest(url, method="POST", body=body, **self.http_request_kwargs)

        # TODO : retry if needed
        response = yield self.client.fetch(request=request_http)
        raise Return(response)

    @coroutine
    def get_by_path(self, path):
        url = '%(url)s%(path)s' % {"url": self.url, "path": path}

        # TODO : retry if needed
        response = yield self.client.fetch(url, **self.http_request_kwargs)
        raise Return(response)

    @coroutine
    def get(self, index, doc_type, doc_id, parameters=None):
        response = yield self.request_document(index, doc_type, doc_id, parameters=parameters)
        source = json_decode(response.body)
        raise Return(source)

    @coroutine
    def put(self, index, doc_type, doc_id, contents, parameters=None):
        response = yield self.request_document(index, doc_type, doc_id, "PUT", body=json_encode(contents),
                                               parameters=parameters)
        raise Return(response)

    @coroutine
    def update(self, index, doc_type, doc_id, contents):
        path = "/%(index)s/%(type)s/%(id)s/_update" % {"index": index, "type": doc_type, "id": doc_id}

        partial = {"doc": contents}

        response = yield self.post_by_path(path, json_encode(partial))
        raise Return(response)

    @coroutine
    def delete(self, index, doc_type, doc_id, parameters=None):
        response = yield self.request_document(index, doc_type, doc_id, "DELETE", parameters=parameters)
        raise Return(response)

    @coroutine
    def count(self, index="_all", doc_type='', body=None, **kwargs):
        path = self.create_path('count', index=index, doc_type=doc_type, **kwargs)

        body = body or self.__MATCH_ALL_QUERY
        response = yield self.post_by_path(path, body)

        raise Return(response)

    @coroutine
    def request_document(self, index='_all', doc_type='', doc_id='', method="GET", body=None, parameters=None):
        path = "/%(index)s/%(type)s/%(doc_id)s" % {"index": index, "type": doc_type, "doc_id": doc_id}

        url = '%(url)s%(path)s?%(querystring)s' % \
              {"url": self.url, "path": path, "querystring": urlencode(parameters or {})}
        request_arguments = dict(self.http_request_kwargs)
        request_arguments['method'] = method

        if body is not None:
            request_arguments['body'] = body

        request = HTTPRequest(url, **request_arguments)

        response = yield self.client.fetch(request)
        raise Return(response)