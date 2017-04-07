from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_revenue(target, timezone_offset):
    yesterday = current_time(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_revenue(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text(""" 
                                           SELECT Date(Convert_tz(createtime, '+00:00', :timezone_offset)) AS on_day, 
                                                  COUNT(DISTINCT u_id)                                     AS dollar_paid_user_count, 
                                                  SUM(CASE 
                                                        WHEN user_paylog_status_id = 3 THEN 1 
                                                        ELSE 0 
                                                      END)                                                 AS dollar_paid_count, 
                                                  ROUND(SUM(CASE 
                                                              WHEN user_paylog_status_id = 3 THEN order_price 
                                                              ELSE 0 
                                                            END) / 100, 2)                                 AS dollar_paid_amount 
                                           FROM   user_paylog 
                                           GROUP  BY on_day 
                                           """), timezone_offset=timezone_offset)

        if target == 'yesterday':
            return connection.execute(text(""" 
                                           SELECT Date(Convert_tz(createtime, '+00:00', :timezone_offset)) AS on_day, 
                                                  COUNT(DISTINCT u_id)                                     AS dollar_paid_user_count, 
                                                  SUM(CASE 
                                                        WHEN user_paylog_status_id = 3 THEN 1 
                                                        ELSE 0 
                                                      END)                                                 AS dollar_paid_count, 
                                                  ROUND(SUM(CASE 
                                                              WHEN user_paylog_status_id = 3 THEN order_price 
                                                              ELSE 0 
                                                            END) / 100, 2)                                 AS dollar_paid_amount 
                                           FROM   user_paylog 
                                           GROUP  BY on_day 
                                           HAVING on_day = :on_day 
                                           """), on_day=yesterday, timezone_offset=timezone_offset)

        if target == 'today':
            return connection.execute(text(""" 
                                           SELECT Date(Convert_tz(createtime, '+00:00', :timezone_offset)) AS on_day, 
                                                  COUNT(DISTINCT u_id)                                     AS dollar_paid_user_count, 
                                                  SUM(CASE 
                                                        WHEN user_paylog_status_id = 3 THEN 1 
                                                        ELSE 0 
                                                      END)                                                 AS dollar_paid_count, 
                                                  ROUND(SUM(CASE 
                                                              WHEN user_paylog_status_id = 3 THEN order_price 
                                                              ELSE 0 
                                                            END) / 100, 2)                                AS dollar_paid_amount 
                                           FROM   user_paylog 
                                           GROUP  BY on_day 
                                           HAVING on_day = :on_day 
                                           """), on_day=today, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_revenue, 'orig_wpt_payment')

    rows = [{'_on_day': row['on_day'],
             'dollar_paid_user_count': row['dollar_paid_user_count'],
             'dollar_paid_count': row['dollar_paid_count'],
             'dollar_paid_amount': row['dollar_paid_amount'],
             } for row in result_proxy]

    if rows:
        def sync_collection_revenue(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'dollar_paid_user_count': bindparam('dollar_paid_user_count'),
                'dollar_paid_count': bindparam('dollar_paid_count'),
                'dollar_paid_amount': bindparam('dollar_paid_amount')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('Revenue transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('Revenue transaction.commit()')
                transaction.commit()

        with_db_context(db, sync_collection_revenue)