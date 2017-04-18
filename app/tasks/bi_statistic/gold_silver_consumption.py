from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


# TODO : to be confirmed sql

# we can't calculate Gold consumption because we don't have all consumption data yet.
# Gold consumption is the total Gold removed from the economy:
# rake, SNG fee, MTT fee, virtual gift purchase (table gift, avatars, charms, emojis),
#  and conversion of Gold to Silver. We need to bring in rake, SNG fee, and MTT fee (including rebuys and add-ons).
# Also need to add insurance but I'm not exactly sure how that data looks like.

def process_bi_statistic_gold_silver_consumption(target):
    _, someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_gold_silver_consumption(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                   SUM(transaction_amount)                          AS sum
                                            FROM   bi_user_currency
                                            WHERE  currency_type = 'Gold'
                                            GROUP  BY on_day
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT SUM(transaction_amount)                          AS sum
                                            FROM   bi_user_currency
                                            WHERE  currency_type = 'Gold'
                                            AND    created_at > :index_time
                                            AND    DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :on_day
                                            """), timezone_offset=timezone_offset, on_day=someday,
                                      index_time=index_time)

    result_proxy = with_db_context(db, collection_gold_silver_consumption)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_gold_consumption(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'gold_consumption': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' gold_consumption transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' gold_consumption transaction.commit()')

        with_db_context(db, sync_collection_gold_consumption)

    def collection_silver_consumption(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                   SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  currency_type = 'Silver'
                                            GROUP  BY on_day
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  currency_type = 'silver'
                                            AND    created_at > :index_time
                                            AND    DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :on_day
                                            """), timezone_offset=timezone_offset, on_day=someday,
                                      index_time=index_time)

    result_proxy = with_db_context(db, collection_silver_consumption)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_silver_consumption(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'gold_consumption': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' silver_consumption transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' silver_consumption transaction.commit()')

        with_db_context(db, sync_collection_silver_consumption)
