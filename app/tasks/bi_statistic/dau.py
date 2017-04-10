from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_dau(target):
    someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_dau_all_games(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day
                                           """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)
        else:
            return connection.execute(text("""
                                           SELECT COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  created_at > :index_time
                                                  AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = :on_day
                                                  AND transaction_type NOT IN :free_transaction_types
                                           """), on_day=someday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE, index_time=index_time)

    result_proxy = with_db_context(db, collection_dau_all_games)

    if target == 'lifetime':
        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]
    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]
    if rows:
        def sync_collection_dau_all_games(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {'dau': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' DAU for all games transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' DAU for all games transaction.commit()')

        with_db_context(db, sync_collection_dau_all_games)

    def collection_dau_every_game(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  CASE
                                                    WHEN game_id = 39990 THEN 'TexasPoker'
                                                    WHEN game_id = 23118 THEN 'TimeSlots'
                                                  END                                                      AS game,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     game
                                          """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        else:
            return connection.execute(text("""
                                           SELECT CASE
                                                     WHEN game_id = 39990 THEN 'TexasPoker'
                                                     WHEN game_id = 23118 THEN 'TimeSlots'
                                                  END                                                      AS game,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  created_at > :index_time
                                                  AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = :on_day
                                                  AND transaction_type NOT IN :free_transaction_types
                                           GROUP  BY game
                                          """), on_day=someday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE, index_time=index_time)

    result_proxy = with_db_context(db, collection_dau_every_game)

    if target == 'lifetime':
        rows = [{'_on_day': row['on_day'], '_game': row['game'], 'sum': row['sum']} for row in result_proxy]
    else:
        rows = [{'_on_day': someday, '_game': row['game'], 'sum': row['sum']} for row in result_proxy]
    if rows:
        def sync_collection_dau_every_game(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == bindparam('_game'),
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {'dau': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' DAU for every game transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' DAU  for every game transaction.commit()')

        with_db_context(db, sync_collection_dau_every_game)
