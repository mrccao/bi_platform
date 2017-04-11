from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import GOLD_FREE_TRANSACTION_TYPES_TUPLE, SILVER_FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_free_transaction(target):
    _, someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_gold_free_transaction(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                   SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  transaction_type IN  :gold_free_transaction_types
                                            GROUP  BY on_day
                                           """), timezone_offset=timezone_offset,
                                      gold_free_transaction_types=GOLD_FREE_TRANSACTION_TYPES_TUPLE)

        else:

            return connection.execute(text("""
                                            SELECT SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  transaction_type IN  :gold_free_transaction_types
                                            AND    created_at > :index_time
                                            AND    DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = : on_day
                                           """), timezone_offset=timezone_offset, on_day=someday,
                                      gold_free_transaction_types=GOLD_FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_gold_free_transaction)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_gold_free_transaction(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'free_gold': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' free gold transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' free gold transaction.commit()')

        with_db_context(db, sync_collection_gold_free_transaction)

    def collection_silver_free_transaction(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                                SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                       SUM(transaction_amount)                        AS sum
                                                FROM   bi_user_currency
                                                WHERE  transaction_type IN  :silver_free_transaction_types
                                                GROUP  BY on_day
                                               """), timezone_offset=timezone_offset,
                                      silver_free_transaction_types=SILVER_FREE_TRANSACTION_TYPES_TUPLE)

        else:

            return connection.execute(text("""
                                                SELECT SUM(transaction_amount)                        AS sum
                                                FROM   bi_user_currency
                                                WHERE  transaction_type IN  :silver_free_transaction_types
                                                AND    created_at > :index_time
                                                AND    DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = : on_day
                                               """), timezone_offset=timezone_offset, on_day=someday,
                                      silver_free_transaction_types=SILVER_FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_silver_free_transaction)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_silver_free_transaction(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'free_silver': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' free silver transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' free silver transaction.commit()')

        with_db_context(db, sync_collection_silver_free_transaction)
