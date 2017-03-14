from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context, celery
from app.utils import current_time


@celery.task
def process_bi_statistic_dau(target):
    # process_bi_statistic_for_lifetime dau_all_game

    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_dau_all_games(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                       """), on_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_dau_all_games)

    if result_proxy is None:
        return

    rows = [{'_on_day': row['on_day'], '_game': 'All Game', 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_dau_all_games(connection, transaction):
            where = and_(
                BIStatistic.__table__.c._day == bindparam('_on_day'),
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
                print('process_bi_statistic_for_lifetime dau  all games transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_dau_all_games)

    # process_bi_statistic_for_lifetime dau_every_game

    def collection_dau_every_game(connection, transaction):
        if target == 'lifetime':
            connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                    ELSE 'unknow'
                                                  END                                              AS game,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day,game
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                                   SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                          CASE
                                                            WHEN game_id = 25011 THEN 'Texas Poker'
                                                            WHEN game_id = 35011 THEN 'TimeSlots'
                                                          ELSE 'unknow'
                                                          END                                              AS game,
                                                          COUNT(DISTINCT user_id)                          AS sum
                                                   FROM   bi_user_currency
                                                   GROUP  BY on_day, game
                                                   HAVING on_day = :on_day
                                               """), on_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                                   SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                          CASE
                                                            WHEN game_id = 25011 THEN 'Texas Poker'
                                                            WHEN game_id = 35011 THEN 'TimeSlots'
                                                          ELSE 'unknow'
                                                          END                                              AS game,
                                                          COUNT(DISTINCT user_id)                          AS sum
                                                   FROM   bi_user_currency
                                                   GROUP  BY on_day, game
                                                   HAVING on_day = :on_day
                                               """), on_day=today)

    result_proxy = with_db_context(db, collection_dau_every_game)

    if result_proxy is None:
        return
    rows = [{
                '_on_day': row['on_day'],
                '_game': row['game'],
                'sum': row['sum']
            } for row in result_proxy]

    if rows:
        def sync_collection_dau_every_game(connection, transaction):
            where = and_(
                BIStatistic.__table__.c._day == bindparam('_on_day'),
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
