""" 
Wrapper around pandas.DataFrame.
"""

import arrow
import pandas as pd

from flask import current_app as app


class DataFrame(object):
    def __init__(self, df):
        self.__df = df.where((pd.notnull(df)), None)

    @property
    def dateframe(self):
        for column in self.__df.select_dtypes(include=['datetime', 'datetime64']).columns:
            self.__df[column] = self.__df[column].apply(lambda x: None if pd.isnull(x) else arrow.get(x).to(app.config['APP_TIMEZONE']).format())
            self.__df[column].astype(str)

        for column in [col for col in self.__df.dtypes.keys() if
                       col.endswith('_amount') or
                       col.endswith('_count') or
                       col.startswith('count_of_') or
                       col.startswith('amount_of_')]:
            self.__df[column] = self.__df[column].apply(lambda x: None if (x == None or x == 0) else str(x))
            self.__df[column].astype(str)

        return self.__df

    @property
    def size(self):
        return len(self.__df.index)

    @property
    def columns(self):
        return [{'title': col} for col in self.__df.dtypes.keys()]

    @property
    def data(self):
        for column in self.__df.select_dtypes(include=['datetime', 'datetime64']).columns:
            self.__df[column] = self.__df[column].apply(lambda x: None if pd.isnull(x) else arrow.get(x).to(app.config['APP_TIMEZONE']).format())
            self.__df[column].astype(str)

        for column in [col for col in self.__df.dtypes.keys() if
                       col.endswith('_amount') or
                       col.endswith('_count') or
                       col.startswith('count_of_') or
                       col.startswith('amount_of_')]:
            self.__df[column] = self.__df[column].apply(lambda x: None if (x == None or x == 0) else str(x))
            self.__df[column].astype(str)

        # def format_row_value(column, value):
        #     if value == 0 and (
        #             column.endswith('_amount') or
        #             column.endswith('_count') or
        #             column.startswith('count_of_') or
        #             column.startswith('amount_of_')
        #         ):
        #         return None
        #     return value

        columns = self.__df.dtypes.keys()
        return [[row[column] for column in columns] for _, row in self.__df.iterrows()]
