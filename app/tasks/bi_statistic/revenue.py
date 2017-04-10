from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_revenue(target):
    someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_revenue(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  ROUND(SUM(currency_amount), 2)                           AS sum
                                           FROM   bi_user_bill
                                           WHERE  currency_type = 'Dollar'
                                           GROUP  BY on_day
                                            """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                           SELECT 
                                                  ROUND(SUM(currency_amount), 2)                           AS sum
                                           FROM   bi_user_bill
                                           WHERE  currency_type = 'Dollar'
                                           AND  DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = :on_day
                                            """), timezone_offset=timezone_offset, on_day=someday)

    result_proxy = with_db_context(db, collection_revenue)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_revenue(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'revenue': bindparam('sum')
            }
            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' revenue_transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' revenue transaction.commit()')
            return

        with_db_context(db, sync_collection_revenue)
