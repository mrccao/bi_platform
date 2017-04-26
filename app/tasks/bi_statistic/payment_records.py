from datetime import date

import pandas as pd
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_payment_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_payment_records(connection, transaction, day):

        return connection.execute(text(""" 
                                       SELECT Count(DISTINCT user_id) AS paid_user_count,
                                              Count(*)                AS paid_count
                                       FROM   bi_user_bill
                                       WHERE  currency_type = 'Dollar'
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :on_day 
                                      """), on_day=day, timezone_offset=timezone_offset)

    def get_payment_records():

        result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")
                every_day_result = with_db_context(db, collection_payment_records, day=day)
                every_day_result_rows = [{'_on_day': str(day),
                                          'paid_user_count': row['paid_user_count'],
                                          'paid_count': row['paid_count']} for row in every_day_result]

                result_proxy.append(every_day_result_rows)

            return result_proxy

        else:

            someday_result = with_db_context(db, collection_payment_records, day=someday)
            someday_result_rows = [{'_on_day': str(someday),
                                    'paid_user_count': row['paid_user_count'],
                                    'paid_count': row['paid_count']} for row in someday_result]

            result_proxy.append(someday_result_rows)
            return result_proxy

    result_proxy_for_payments = get_payment_records()

    for rows in result_proxy_for_payments:

        if rows:
            def sync_collection_payments(connection, transaction):

                where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                             BIStatistic.__table__.c.game == 'All Game',
                             BIStatistic.__table__.c.platform == 'All Platform')
                values = {'paid_user_count': bindparam('paid_user_count'),
                          'paid_count': bindparam('paid_count')}

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), list(rows))
                except:
                    print(target + ' payments transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print(target + ' payments transaction.commit()')
                    transaction.commit()

            with_db_context(db, sync_collection_payments)
