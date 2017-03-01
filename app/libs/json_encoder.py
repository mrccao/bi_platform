import numpy
import decimal
import uuid
import arrow
from flask.json import JSONEncoder
from datetime import datetime

class FlaskJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, numpy.int64):
                return int(obj)
            elif isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, decimal.Decimal):
                return float(obj)
            elif isinstance(obj, uuid.UUID):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, arrow.Arrow):
                return obj.format()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)
