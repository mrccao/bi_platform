from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_payment_records(target):
    _, someday, _, timezone_offset = generate_sql_date(target)

    def collection_payment_records(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text(""" 
                                           SELECT Date(Convert_tz(createtime, '+00:00', :timezone_offset)) AS on_day, 
                                                  COUNT(DISTINCT u_id)                                     AS paid_user_count, 
                                                  SUM(CASE 
                                                        WHEN user_paylog_status_id = 3 THEN 1 
                                                        ELSE 0 
                                                      END)                                                 AS paid_count, 
                                                  ROUND(SUM(CASE 
                                                              WHEN user_paylog_status_id = 3 THEN order_price 
                                                              ELSE 0 
                                                            END) / 100, 2)                                 AS paid_amount 
                                           FROM   user_paylog 
                                           GROUP  BY on_day 
                                           """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text(""" 
                                           SELECT COUNT(DISTINCT u_id)                                     AS paid_user_count, 
                                                  SUM(CASE 
                                                        WHEN user_paylog_status_id = 3 THEN 1 
                                                        ELSE 0 
                                                      END)                                                 AS paid_count, 
                                                  ROUND(SUM(CASE 
                                                              WHEN user_paylog_status_id = 3 THEN order_price 
                                                              ELSE 0 
                                                            END) / 100, 2)                                 AS paid_amount 
                                           FROM   user_paylog 
                                           WHERE Date(Convert_tz(createtime, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_payment_records, 'orig_wpt_payment')

    if target == 'lifetime':

        rows = map(lambda row: {'_on_day': row['on_day'], 'paid_user_count': row['paid_user_count'],
                                'paid_count': row['paid_count'] if row['paid_count'] else 0,
                                'paid_amount': row['paid_amount'] if row['paid_amount'] else 0}, result_proxy)

    else:

        rows = map(lambda row: {'_on_day': someday, 'paid_user_count': row['paid_user_count'],
                                'paid_count': row['paid_count'] if row['paid_count'] else 0,
                                'paid_amount': row['paid_amount'] if row['paid_amount'] else 0}, result_proxy)

    if rows:
        def sync_collection_revenue(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'paid_user_count': bindparam('paid_user_count'),
                      'paid_count': bindparam('paid_count'),
                      'paid_amount': bindparam('paid_amount')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), list(rows))
            except:
                print(target + ' Revenue transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print(target + 'Revenue transaction.commit()')
                transaction.commit()

        with_db_context(db, sync_collection_revenue)
