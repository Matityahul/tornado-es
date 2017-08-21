[![Build Status](https://secure.travis-ci.org/Matityahul/tornado-es-lm.svg?branch=master)](https://travis-ci.org/Matityahul/tornado-es-lm)


Tornado-es-lm
=============

A tornado-powered python library that provides asynchronous access to elasticsearch. Extends [tornado-es](https://github.com/globocom/tornado-es).


Install
=======

Via pip:

    pip install tornadoes-lm


Usage
=====

Indexing a dummy document:

```bash
$ curl -XPUT 'http://localhost:9200/index_test/typo_test/1' -d '{
    "typo_test" : {
        "name" : "scooby doo"
    }
}'
```

Tornado program used to search the document previously indexed:

```python
# -*- coding: utf-8 -*-

import json

import tornado.ioloop
import tornado.web

from tornadoes_lm import ESConnection


class SearchHandler(tornado.web.RequestHandler):

    es_connection = ESConnection("localhost", 9200)

    @tornado.web.asynchronous
    def get(self, indice="index_test", tipo="typo_test"):
        query = {"query": {"match_all": {}}}
        self.es_connection.search(callback=self.callback,
                                  index=indice,
                                  type=tipo,
                                  source=query)

    def callback(self, response):
        self.content_type = 'application/json'
        self.write(json.loads(response.body))
        self.finish()

application = tornado.web.Application([
    (r"/", SearchHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
```


Development
===========

Setup your development environment:

    make setup

> *Note: Don't forget to create a virtualenv first*

Run tests:

    make test

> *Note: Make sure ElasticSearch is running on port 9200*


Contributing
============

Fork, patch, test, and send a pull request.


License
=======

[MIT](http://opensource.org/licenses/MIT)
