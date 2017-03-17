from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_dau(target, timezone_offset):
    yesterday = current_time(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

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

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                           """), on_day=yesterday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                           """), on_day=today, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_dau_all_games)

    if result_proxy is None:
        return

    rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_dau_all_games(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'dau': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime dau all games transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime dau all games transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_dau_all_games)

    def collection_dau_every_game(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                  ELSE 'Unknown'
                                                  END                                                      AS game,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     game
                                          """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  CASE
                                                     WHEN game_id = 25011 THEN 'Texas Poker'
                                                     WHEN game_id = 35011 THEN 'TimeSlots'
                                                  ELSE 'Unknown'
                                                  END                                                      AS game,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     game
                                           HAVING on_day = : on_day
                                          """), on_day=yesterday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                  CASE
                                                     WHEN game_id = 25011 THEN 'Texas Poker'
                                                     WHEN game_id = 35011 THEN 'TimeSlots'
                                                  ELSE 'Unknown'
                                                  END                                                      AS game,
                                                  COUNT(DISTINCT user_id)                                  AS sum
                                           FROM   bi_user_currency
                                           WHERE  transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     game
                                           HAVING on_day = :on_day
                                           """), on_day=today, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_dau_every_game)

    if result_proxy is None:
        return

    rows = [{'_on_day': row['on_day'], '_game': row['game'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_dau_every_game(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == bindparam('_game'),
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'dau': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime dau  for every game transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime dau  for every game transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_dau_every_game)
