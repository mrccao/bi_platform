import calendar
import logging
import signal

import arrow
import importlib
from flask import current_app as app

from app.exceptions import TimeoutException


def get_day_range_of_month(now):
    current_year = now.year
    current_month = now.month
    previous_month = current_month - 1
    previous_year = current_year
    _, last_day_of_current_month = calendar.monthrange(current_year, current_month)

    if current_month == 1:
        previous_year = current_year - 1
        previous_month = 12

    _, last_day_of_current_month = calendar.monthrange(current_year, current_month)
    _, last_day_of_previous_month = calendar.monthrange(previous_year, previous_month)

    first_day_of_current_month = str(current_year) + '-' + str(current_month) + '-' + '01'
    last_day_of_current_month = str(current_year) + '-' + str(current_month) + '-' + str(last_day_of_current_month)
    first_day_of_previous_month = str(previous_year) + '-' + str(previous_month) + '-' + '01'
    last_day_of_previous_month = str(previous_year) + '-' + str(previous_month) + '-' + str(last_day_of_previous_month)


    first_day_of_current_month = arrow.Arrow.strptime(first_day_of_current_month, '%Y-%m-%d').format('YYYY-MM-DD')
    last_day_of_current_month = arrow.Arrow.strptime(last_day_of_current_month, '%Y-%m-%d').format('YYYY-MM-DD')


    first_day_of_previous_month = arrow.Arrow.strptime(first_day_of_previous_month, '%Y-%m-%d').format('YYYY-MM-DD')
    last_day_of_previous_month = arrow.Arrow.strptime(last_day_of_previous_month, '%Y-%m-%d').format('YYYY-MM-DD')

    return first_day_of_current_month, last_day_of_current_month, first_day_of_previous_month, last_day_of_previous_month


def generate_sql_date(target):
    now = current_time(app.config['APP_TIMEZONE'])
    yesterday = now.replace(days=-1).format('YYYY-MM-DD')
    today = now.format('YYYY-MM-DD')

    # timezone_offset = app.config['APP_TIMEZONE']
    timezone_offset = '-04:00'
    day = {'today': today, 'yesterday': yesterday}
    someday = day.get(target, target)

    if target == 'lifetime':
        someday = None
        index_date = None
        sql_date = today, someday, index_date, timezone_offset
        return sql_date

    elif target in ['today', 'yesterday']:
        index_date = now.replace(days=-3).format('YYYY-MM-DD')
        sql_date = today, someday, index_date, timezone_offset
        return sql_date

    else:
        target_date = arrow.Arrow.strptime(target, '%Y-%m-%d', app.config['APP_TIMEZONE'])
        index_date = target_date.replace(days=-3).format('YYYY-MM-DD')
        sql_date = today, someday, index_date, timezone_offset
        return sql_date


def current_time(timezone=None):
    """ get current time """
    now = arrow.utcnow()
    if timezone is None:
        return now
    return now.to(timezone)

#
# def get_week_order_of_date_range(start_time, end_time):
#     if end_time == start_time:
#         return
#
#     end_year, end_week = end_time.isocalendar()[0:2]
#     start_year, start_week = start_time.isocalendar()[0:2]
#
#     if end_week < 10:
#         end_week = '0' + str(end_week)
#     if start_week < 10:
#         start_week = '0' + str(start_week)
#
#     end_week = str(end_year) + '-' + str(end_week)
#     start_week = str(start_year) + '-' + str(start_week)
#
#     return start_week, end_week

#
# def generate_date_range_group_by_daily_or_weekly_or_monthly(start_time, end_time, group_type):
#     start_day = arrow.Arrow.strptime(start_time, "%Y-%m-%d")
#     end_day = arrow.Arrow.strptime(end_time, "%Y-%m-%d")
#
#     if group_type == 'Monthly':
#         start_month = start_day.format("YYYY-MM")
#         end_month = end_day.format("YYYY-MM")
#
#         return start_month, end_month
#
#     if group_type == 'Weekly':
#         start_week, end_week = get_week_order_of_date_range(start_day, end_day)
#
#         return start_week, end_week
#
#     return start_time, end_time


def current_time_as_float(timezone=None):
    return current_time(timezone).float_timestamp * 1000


def get_last_day_of_prev_month(timezone=None):
    prev_month = current_time(timezone).replace(months=-1)
    return arrow.get(prev_month.year, prev_month.month, calendar.monthrange(prev_month.year, prev_month.month)[1])


class timeout(object):
    """
    To be used in a ``with`` block and timeout its content.
    """

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        logging.error("Process timed out")
        raise TimeoutException(self.error_message)

    def __enter__(self):
        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            signal.alarm(0)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)


def error_msg_from_exception(e):
    """Translate exception into error message

    Database have different ways to handle exception. This function attempts
    to make sense of the exception object and construct a human readable
    sentence.

    TODO(bkyryliuk): parse the Presto error message from the connection
                     created via create_engine.
    engine = create_engine('presto://localhost:3506/silver') -
      gives an e.message as the str(dict)
    presto.connect("localhost", port=3506, catalog='silver') - as a dict.
    The latter version is parsed correctly by this function.
    """
    msg = ''
    if hasattr(e, 'message'):
        if type(e.message) is dict:
            msg = e.message.get('message')
        elif e.message:
            msg = "{}".format(e.message)
    return msg or '{}'.format(e)


def dedup(l, suffix='__'):
    """De-duplicates a list of string by suffixing a counter

    Always returns the same number of entries as provided, and always returns
    unique values.

    # >>> dedup(['foo', 'bar', 'bar', 'bar'])
    ['foo', 'bar', 'bar__1', 'bar__2']
    """
    new_l = []
    seen = {}
    for s in l:
        if s in seen:
            seen[s] += 1
            s += suffix + str(seen[s])
        else:
            seen[s] = 0
        new_l.append(s)
    return new_l


def str_to_class(module_name, class_name):
    mdl = importlib.import_module(module_name)
    kls = getattr(mdl, class_name)
    return kls

# def base_json_conv(obj):

#     if isinstance(obj, numpy.int64):
#         return int(obj)
#     elif isinstance(obj, set):
#         return list(obj)
#     elif isinstance(obj, decimal.Decimal):
#         return float(obj)
#     elif isinstance(obj, uuid.UUID):
#         return str(obj)

# def json_iso_dttm_ser(obj):
#     """
#     json serializer that deals with dates

#     >>> dttm = datetime(1970, 1, 1)
#     >>> json.dumps({'dttm': dttm}, default=json_iso_dttm_ser)
#     '{"dttm": "1970-01-01T00:00:00"}'
#     """
#     val = base_json_conv(obj)
#     if val is not None:
#         return val
#     if isinstance(obj, datetime):
#         obj = obj.isoformat()
#     elif isinstance(obj, date):
#         obj = obj.isoformat()
#     else:
#         raise TypeError(
#             "Unserializable object {} of type {}".format(obj, type(obj))
#         )
#     return obj
