import json
import uuid
from datetime import date, datetime
from decimal import Decimal

from six import string_types

JSON_MIME_TYPE = 'application/json'
TEXT_MIME_TYPE = 'text/plain'


def _default(data):
    if isinstance(data, (date, datetime)):
        return data.isoformat()
    elif isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError("Unable to serialize %r (type: %s)" % (data, type(data)))


def json_dumps(data):
    # don't serialize strings
    if isinstance(data, string_types):
        return data

    return json.dumps(data, default=_default, ensure_ascii=False)


def deserialize_es_response(http_response):
    if http_response.error:
        http_response.rethrow()

    content_type = http_response.headers.get('Content-Type', JSON_MIME_TYPE)

    # split out charset
    mime_type = content_type.split(';', 1)[0]

    if mime_type == JSON_MIME_TYPE:
        return json.loads(http_response.body)
    elif mime_type == TEXT_MIME_TYPE:
        return http_response.body
    else:
        raise TypeError('Unknown MIME type, unable to deserialize: %s' % mime_type)