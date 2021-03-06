from datetime import date

import pandas as pd
from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_retention(target):
    now = current_time(app.config['APP_TIMEZONE'])
    today = now.format('YYYY-MM-DD')
    timezone_offset = '-04:00'

    def collection_retention_all_platform(connection, transaction, day, timedelta):

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

    def get_retention_all_platform():

        result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

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
                if timedelta == 1:
                    day = {'today': now.replace(days=-1).format('YYYY-MM-DD'),
                           'yesterday': now.replace(days=-2).format('YYYY-MM-DD')}

                    someday = day.get(target, target)

                    print(str(timedelta) + ' retention on ' + str(someday))

                if timedelta == 7:
                    day = {'today': now.replace(days=-7).format('YYYY-MM-DD'),
                           'yesterday': now.replace(days=-8).format('YYYY-MM-DD')}

                    someday = day.get(target, target)

                    print(str(timedelta) + ' retention on ' + str(someday))

                if timedelta == 30:
                    day = {'today': now.replace(days=-30).format('YYYY-MM-DD'),
                           'yesterday': now.replace(days=-31).format('YYYY-MM-DD')}

                    someday = day.get(target, target)

                    print(str(timedelta) + ' retention on ' + str(someday))

                specific_day_retention = with_db_context(db, collection_retention_all_platform, day=someday,
                                                         timedelta=timedelta)

                specific_day_retention_rows = [{'_on_day': str(someday), 'sum': row['sum']} for row in
                                               specific_day_retention]

                all_day_retention_rows = dict([(timedelta_str, specific_day_retention_rows)])
                result_proxy.append(all_day_retention_rows)

        return result_proxy

    result_proxy_for_retention = get_retention_all_platform()

    for all_day_retention_result in result_proxy_for_retention:

        for timedelta_str, rows in all_day_retention_result.items():

            if rows:

                def sync_collection_retention_all_platform(connection, transaction):

                    where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                                 BIStatistic.__table__.c.game == 'All Game',
                                 BIStatistic.__table__.c.platform == 'All Platform')

                    values = {'{}_day_retention'.format(timedelta_str): bindparam('sum')}

                    try:
                        connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print(target + ' ' + timedelta_str + '_retention for all platform transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        transaction.commit()
                        print(target + ' ' + timedelta_str + '_retention for all platform transaction.commit()')

                with_db_context(db, sync_collection_retention_all_platform)

    # def collection_retention(connection, transaction, day, timedelta):
    #     return connection.execute(text("""
    #                                     SELECT COUNT(DISTINCT c.user_id) AS sum,
    #                                            CASE
    #                                              WHEN LEFT(b.reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
    #                                              WHEN LEFT(b.reg_source, 3) = 'Web' THEN 'Web'
    #                                              WHEN LEFT(b.reg_source, 3) = 'iOS' THEN 'iOS'
    #                                              WHEN LEFT(b.reg_source, 8) = 'Facebook' THEN 'Facebook Game'
    #                                              WHEN LEFT(b.reg_source, 7) = 'Android' THEN 'Android'
    #                                            END                                                    AS platform
    #                                     FROM   bi_user_currency c
    #                                            INNER JOIN
    #                                            bi_user  b ON c.user_id=b.user_id
    #                                     WHERE  transaction_type NOT IN :free_transaction_types
    #                                            AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) =
    #                                                DATE_ADD(:on_day, INTERVAL + :timedelta DAY)
    #                                            AND c.user_id IN (SELECT user_id
    #                                                            FROM   bi_user
    #                                                            WHERE  DATE(CONVERT_TZ(reg_time, '+00:00',
    #                                                                        :timezone_offset)) =
    #                                                                   :on_day);
    #                                   """), timezone_offset=timezone_offset, on_day=day, timedelta=timedelta,
    #                               free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)
    #
    # def get_retention():
    #
    #     result_proxy = []
    #
    #     if target == 'lifetime':
    #         date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)
    #
    #         for timedelta, timedelta_str in [(1, 'one'), (7, 'seven'), (30, 'thirty')]:
    #
    #             for day in date_range_reversed:
    #                 day = day.strftime("%Y-%m-%d")
    #
    #                 print(str(timedelta) + ' retention on ' + str(day))
    #
    #                 specific_day_retention = with_db_context(db, collection_retention, day=day, timedelta=timedelta)
    #
    #                 specific_day_retention_rows = [{'_on_day': str(day), 'sum': row['sum']} for row in
    #                                                specific_day_retention]
    #
    #                 all_day_retention_rows = dict([(timedelta_str, specific_day_retention_rows)])
    #                 result_proxy.append(all_day_retention_rows)
    #
    #     else:
    #
    #         for timedelta, timedelta_str in [(1, 'one'), (7, 'seven'), (30, 'thirty')]:
    #             if timedelta == 1:
    #                 day = {'today': now.replace(days=-1).format('YYYY-MM-DD'),
    #                        'yesterday': now.replace(days=-2).format('YYYY-MM-DD')}
    #
    #                 someday = day.get(target, target)
    #
    #                 print(str(timedelta) + ' retention on ' + str(someday))
    #
    #             if timedelta == 7:
    #                 day = {'today': now.replace(days=-7).format('YYYY-MM-DD'),
    #                        'yesterday': now.replace(days=-8).format('YYYY-MM-DD')}
    #
    #                 someday = day.get(target, target)
    #
    #                 print(str(timedelta) + ' retention on ' + str(someday))
    #
    #             if timedelta == 30:
    #                 day = {'today': now.replace(days=-30).format('YYYY-MM-DD'),
    #                        'yesterday': now.replace(days=-31).format('YYYY-MM-DD')}
    #
    #                 someday = day.get(target, target)
    #
    #                 print(str(timedelta) + ' retention on ' + str(someday))
    #
    #             specific_day_retention = with_db_context(db, collection_retention_all_platform, day=someday,
    #                                                      timedelta=timedelta)
    #
    #             specific_day_retention_rows = [
    #                 {'_on_day': str(someday), 'sum': row['sum'], '_platform': row['platform']} for row in
    #                 specific_day_retention]
    #
    #             all_day_retention_rows = dict([(timedelta_str, specific_day_retention_rows)])
    #             result_proxy.append(all_day_retention_rows)
    #
    #     return result_proxy
    #
    # result_proxy_for_retention = get_retention()
    #
    # for all_day_retention_result in result_proxy_for_retention:
    #
    #     for timedelta_str, rows in all_day_retention_result.items():
    #
    #         if rows:
    #
    #             def sync_collection_retention(connection, transaction):
    #
    #                 where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
    #                              BIStatistic.__table__.c.game == 'All Game',
    #                              BIStatistic.__table__.c.platform == bindparam('_platform'))
    #
    #                 values = {'{}_day_retention'.format(timedelta_str): bindparam('sum')}
    #
    #                 try:
    #                     connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
    #                 except:
    #                     print(target + ' ' + timedelta_str + '_retention for every platform transaction.rollback()')
    #                     transaction.rollback()
    #                     raise
    #                 else:
    #                     transaction.commit()
    #                     print(target + ' ' + timedelta_str + '_retention for every platform transaction.commit()')
    #
    #             with_db_context(db, sync_collection_retention)
