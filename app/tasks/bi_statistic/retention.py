from datetime import date

import pandas as pd
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_retention(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_retention(connection, transaction, day, timedelta):

        return connection.execute(text("""
                                        SELECT COUNT(DISTINCT user_id) AS sum
                                        FROM   bi_user_currency
                                        WHERE  transaction_type NOT IN :free_transaction_types
                                               AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) =
                                                   DATE_ADD(:on_day, INTERVAL + :timedelta DAY)
                                               AND user_id IN (SELECT user_id
                                                               FROM   bi_user
                                                               WHERE  DATE(CONVERT_TZ(reg_time, '+00:00',
                                                                           :timezone_offset)) =
                                                                      :on_day);  
                                      """), timezone_offset=timezone_offset, on_day=day, timedelta=timedelta,
                                  free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

    def get_retention():

        result_proxy = []

        if target == 'lifetime':
            date_range_reversed =sorted(pd.date_range(date(2016, 6, 1), today),reverse=True)

            for timedelta, timedelta_str in [(1, 'one'), (7, 'seven'), (30, 'thirty')]:

                for day in date_range_reversed:
                    day = day.strftime("%Y-%m-%d")

                    print(str(timedelta) + ' retention on ' + str(day))

                    specific_day_retention = with_db_context(db, collection_retention, day=day, timedelta=timedelta)

                    specific_day_retention_rows = [{'_on_day': str(day), 'sum': row['sum']} for row in
                                                   specific_day_retention]

                    all_day_retention_rows = dict([(timedelta_str, specific_day_retention_rows)])
                    result_proxy.append(all_day_retention_rows)

        else:

            for timedelta, timedelta_str in [(1, 'one'), (7, 'seven'), (30, 'thirty')]:
                print(str(timedelta) + ' retention on ' + str(someday))

                specific_day_retention = with_db_context(db, collection_retention, day=someday, timedelta=timedelta)

                specific_day_retention_rows = [{'_on_day': str(someday), 'sum': row['sum']} for row in
                                               specific_day_retention]

                all_day_retention_rows = dict([(timedelta_str, specific_day_retention_rows)])
                result_proxy.append(all_day_retention_rows)

        return result_proxy

    result_proxy_for_retention = get_retention()

    for all_day_retention_result in result_proxy_for_retention:

        for timedelta_str, rows in all_day_retention_result.items():

            if rows:

                def sync_collection_retention(connection, transaction):

                    where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                                 BIStatistic.__table__.c.game == 'All Game',
                                 BIStatistic.__table__.c.platform == 'All Platform')

                    values = {'{}_day_retention'.format(timedelta_str): bindparam('sum')}

                    try:
                        connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print(target + ' ' + timedelta_str + '_retention for all games transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        transaction.commit()
                        print(target + ' ' + timedelta_str + '_retention for all games transaction.commit()')

                with_db_context(db, sync_collection_retention)
