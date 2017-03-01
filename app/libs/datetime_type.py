import arrow
from datetime import datetime
from sqlalchemy import types
from sqlalchemy_utils import ArrowType

from flask import current_app as app
from flask_login import current_user


class NaiveDateTime(types.TypeDecorator):
    impl = types.DateTime

class AwareDateTime(types.TypeDecorator):
    impl = ArrowType

    def process_result_value(self, value, _):
        if value is not None:
            return value.to(app.config['APP_TIMEZONE'])

class OGInsertableAwareDateTime(types.TypeDecorator):
    impl = ArrowType

    # def process_bind_param(self, value, _):
    #     if value is not None:
    #         if isinstance(value, datetime):
    #             return arrow.get(value).replace(hours=-8).to(app.config['APP_TIMEZONE'])
    #         return arrow.get(value).replace(hours=-8).to(app.config['APP_TIMEZONE'])
    def process_bind_param(self, value, _):
        if value is not None:
            if isinstance(value, datetime):
                return arrow.get(value).replace(hours=-8)
            return arrow.get(value).replace(hours=-8)

    def process_result_value(self, value, _):
        if value is not None:
            return value.to(app.config['APP_TIMEZONE'])

class OGReadableAwareDateTime(types.TypeDecorator):
    impl = ArrowType

    def process_result_value(self, value, _):
        if value is not None:
            return value.replace(hours=-8).to(app.config['APP_TIMEZONE'])
