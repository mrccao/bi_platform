import arrow
from datetime import date, timedelta, datetime

import pandas as pd
from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_wau(target):
    now = current_time(app.config['APP_TIMEZONE'])
    index_time = now.replace(days=-(7 + 3)).format('YYYY-MM-DD')
    yesterday = now.replace(days=-1).format('YYYY-MM-DD')
    today = now.format('YYYY-MM-DD')
    timezone_offset = app.config['APP_TIMEZONE']

    # process sync_bi_statistic_for_someday
    if target not in ['lifetime', 'today', 'yesterday']:
        index_time = arrow.get(target).replace(days=-(7 + 3)).format('YYYY-MM-DD')
        today = target
        target = 'today'

    def collection_wau_every_game(connection, transaction, day):

        return connection.execute(text("""
                                       SELECT COUNT(DISTINCT user_id) AS sum,
                                              CASE
                                                WHEN game_id = 39990 THEN 'TexasPoker'
                                                WHEN game_id = 23118 THEN 'TimeSlots'
                                              END                     AS game
                                       FROM   bi_user_currency
                                       WHERE  created_at > :index_time
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) <= :on_day
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) >
                                                  DATE_ADD(:on_day, INTERVAL - 7 DAY)
                                              AND transaction_type NOT IN :free_transaction_types
                                       GROUP  BY game
                                       """), on_day=day, timezone_offset=timezone_offset,
                                  free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE, index_time=index_time)

    def get_wau_every_game():
        if target == 'lifetime':
            result_proxy = []
            for day in pd.date_range(date(2016, 6, 1), today):
                day = day.strftime("%Y-%m-%d")
                every_week_result = with_db_context(db, collection_wau_every_game, day=day)
                every_week_result_rows = [{'_on_day': str(day), '_game': row['game'], 'sum': row['sum']} for row in
                                          every_week_result]
                result_proxy.append(every_week_result_rows)

            return result_proxy

        if target == 'yesterday':
            result_proxy = []
            every_week_result = with_db_context(db, collection_wau_every_game, day=yesterday)
            every_week_result_rows = [{'_on_day': str(yesterday), '_game': row['game'], 'sum': row['sum']} for row in
                                      every_week_result]

            result_proxy.append(every_week_result_rows)
            return result_proxy

        if target == 'today':
            result_proxy = []
            every_week_result = with_db_context(db, collection_wau_every_game, day=today)
            every_week_result_rows = [{'_on_day': str(today), '_game': row['game'], 'sum': row['sum']} for row in
                                      every_week_result]

            result_proxy.append(every_week_result_rows)
            return result_proxy

    result_proxy_for_every_game = get_wau_every_game()

    for rows in result_proxy_for_every_game:

        if rows:

            def sync_collection_wau_every_game(connection, transaction):

                where = and_(
                    BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                    BIStatistic.__table__.c.game == bindparam('_game'),
                    BIStatistic.__table__.c.platform == 'All Platform'
                )
                values = {
                    'wau': bindparam('sum')
                }

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print(target + ' WAU for every game transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    print(target + ' WAU for every game transaction.commit()')
                return

            with_db_context(db, sync_collection_wau_every_game)

    def collection_wau_all_games(connection, transaction, day):

        return connection.execute(text("""
                                       SELECT COUNT(DISTINCT user_id) AS sum
                                       FROM   bi_user_currency
                                       WHERE  created_at > :index_time
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) <= :on_day
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) >
                                                  DATE_ADD(:on_day, INTERVAL - 7 DAY)
                                              AND transaction_type NOT IN :free_transaction_types
                                       """), on_day=day, timezone_offset=timezone_offset,
                                  free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE, index_time=index_time)

    def get_wau_all_games():
        if target == 'lifetime':

            result_proxy = []
            for day in pd.date_range(date(2016, 6, 1), today):
                day = day.strftime("%Y-%m-%d")
                every_week_result = with_db_context(db, collection_wau_all_games, day=day)
                every_week_result_rows = [{'_on_day': str(day), 'sum': row['sum']} for row in every_week_result]
                result_proxy.append(every_week_result_rows)
            return result_proxy

        if target == 'yesterday':
            result_proxy = []
            every_week_result = with_db_context(db, collection_wau_all_games, day=yesterday)
            every_week_result_rows = [{'_on_day': str(yesterday), 'sum': row['sum']} for row in every_week_result]

            result_proxy.append(every_week_result_rows)
            return result_proxy

        if target == 'today':
            result_proxy = []
            every_week_result = with_db_context(db, collection_wau_all_games, day=today)
            every_week_result_rows = [{'_on_day': str(today), 'sum': row['sum']} for row in every_week_result]

            result_proxy.append(every_week_result_rows)
            return result_proxy

    result_proxy_for_all_game = get_wau_all_games()

    for rows in result_proxy_for_all_game:

        if rows:
            def sync_collection_wau_all_games(connection, transaction):

                where = and_(
                    BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                    BIStatistic.__table__.c.game == 'All Game',
                    BIStatistic.__table__.c.platform == 'All Platform'
                )
                values = {
                    'wau': bindparam('sum')
                }

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print(target + ' WAU for all games transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    print(target + ' WAU for all games transaction.commit()')
                return

            with_db_context(db, sync_collection_wau_all_games)
