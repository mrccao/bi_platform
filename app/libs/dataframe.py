""" 
Wrapper around pandas.DataFrame.
"""

import arrow
import pandas as pd

from datetime import datetime

from flask import current_app as app


def convert_datetime(obj):
    if pd.isnull(obj):
        return ''

    if isinstance(obj, datetime):
        return arrow.get(obj).to(app.config['APP_TIMEZONE']).format()
    else:
        return obj


def convert_integer(obj):
    if pd.isnull(obj):
        return None

    if obj == 0:
        return None

    return str(obj)


class DataFrame(object):
    def __init__(self, df):
        self.__df = df.where((pd.notnull(df)), None)

        for column in self.__df.select_dtypes(include=['object']).columns:
            self.__df[column] = self.__df[column].apply(convert_datetime)

        for column in self.__df.select_dtypes(include=['datetime', 'datetime64']).columns:
            self.__df[column] = self.__df[column].apply(convert_datetime)
            self.__df[column].astype(str)

        for column in [col for col in self.__df.dtypes.keys() if
                       col.endswith('_amount') or
                       col.endswith('_count') or
                       col.startswith('count_of_') or
                       col.startswith('amount_of_')]:
            self.__df[column] = self.__df[column].apply(convert_integer)
            self.__df[column].astype(str)

    @property
    def dateframe(self):
        return self.__df

    @property
    def size(self):
        return len(self.__df.index)

    @property
    def columns(self):
        return [{'title': col} for col in self.__df.dtypes.keys()]

    @property
    def data(self):
        columns = self.__df.dtypes.keys()
        return [[row[column] for column in columns] for _, row in self.__df.iterrows()]
