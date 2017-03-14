from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context, celery
from app.utils import current_time


@celery.task
def process_bi_statistic_revenue(target):
    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    #
    # process_bi_statistic_for_lifetime revenue
    #

    def collection_revenue(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT Date(Convert_tz(createtime, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT u_id)                             AS dollar_paid_user_count,
                                                  SUM(CASE
                                                        WHEN user_paylog_status_id = 3 THEN 1
                                                        ELSE 0
                                                      END)                                         AS dollar_paid_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            END) / 100, 2)                         AS dollar_paid_amount
                                           FROM   user_paylog
                                           GROUP  BY on_day
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT Date(Convert_tz(createtime, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT u_id)                             AS dollar_paid_user_count,
                                                  SUM(CASE
                                                        WHEN user_paylog_status_id = 3 THEN 1
                                                        ELSE 0
                                                      END)                                         AS dollar_paid_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            END) / 100, 2)                         AS dollar_paid_amount
                                           FROM   user_paylog
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                       """), on_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT Date(Convert_tz(createtime, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT u_id)                             AS dollar_paid_user_count,
                                                  SUM(CASE
                                                        WHEN user_paylog_status_id = 3 THEN 1
                                                        ELSE 0
                                                      END)                                         AS dollar_paid_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            END) / 100, 2)                         AS dollar_paid_amount
                                           FROM   user_paylog
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_revenue, 'orig_wpt_payment')

    rows = [{'_on_day': row['on_day'],
             'dollar_paid_user_count': row['dollar_paid_user_count'],
             'dollar_paid_count': row['dollar_paid_count'],
             'dollar_paid_amount': row['dollar_paid_amount'],
             } for row in result_proxy]

    if rows:
        def sync_collection_revenue(connection, transaction):
            where = and_(
                BIStatistic.__table__.c._day== bindparam('_on_day'),
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
                print('process_bi_statistic_for_lifetime revenue transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime revenue transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_revenue)

    return
