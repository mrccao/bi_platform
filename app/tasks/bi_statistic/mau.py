from datetime import date

import arrow
import pandas as pd
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_mau(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_mau_all_games(connection, transaction, day):

        start_index_time = arrow.get(day).replace(days=-(30 + 3)).format('YYYY-MM-DD')
        end_index_time = arrow.get(day).replace(days=+(30 + 3)).format('YYYY-MM-DD')

        return connection.execute(text("""
                                       SELECT COUNT(DISTINCT user_id) AS sum
                                       FROM   bi_user_currency
                                       WHERE  created_at > :start_index_time AND created_at < :end_index_time
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) <= :on_day
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) >
                                                  DATE_ADD(:on_day, INTERVAL - 30 DAY)
                                              AND transaction_type NOT IN :free_transaction_types
                                       """), on_day=day, timezone_offset=timezone_offset,
                                  free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE,
                                  start_index_time=start_index_time, end_index_time=end_index_time)

    def get_mau_all_games():

        result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")
                every_month_result = with_db_context(db, collection_mau_all_games, day=day)
                every_month_result_rows = [{'_on_day': str(day), 'sum': row['sum']} for row in every_month_result]
                result_proxy.append(every_month_result_rows)

            return result_proxy

        else:

            every_month_result = with_db_context(db, collection_mau_all_games, day=someday)
            every_month_result_rows = [{'_on_day': str(someday), 'sum': row['sum']} for row in every_month_result]

            result_proxy.append(every_month_result_rows)
            return result_proxy

    result_proxy_for_all_game = get_mau_all_games()

    for rows in result_proxy_for_all_game:

        if rows:

            def sync_collection_mau_all_games(connection, transaction):

                where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                             BIStatistic.__table__.c.game == 'All Game',
                             BIStatistic.__table__.c.platform == 'All Platform')
                values = {'mau': bindparam('sum')}

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print(target + ' MAU for all games transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    print(target + ' MAU for all games transaction.commit()')

            with_db_context(db, sync_collection_mau_all_games)

    def collection_mau_every_game(connection, transaction, day):

        start_index_time = arrow.get(day).replace(days=-(30 + 3)).format('YYYY-MM-DD')
        end_index_time = arrow.get(day).replace(days=+(30 + 3)).format('YYYY-MM-DD')

        return connection.execute(text("""
                                       SELECT COUNT(DISTINCT user_id) AS sum,
                                              CASE
                                                WHEN game_id = 39990 THEN 'TexasPoker'
                                                WHEN game_id = 23118 THEN 'TimeSlots'
                                              END                     AS game
                                       FROM   bi_user_currency
                                       WHERE  created_at > :start_index_time AND created_at < :end_index_time
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) <= :on_day
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) >
                                                  DATE_ADD(:on_day, INTERVAL - 30 DAY)
                                              AND transaction_type NOT IN :free_transaction_types
                                       GROUP  BY game
                                       """), on_day=day, timezone_offset=timezone_offset,
                                  free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE,
                                  start_index_time=start_index_time, end_index_time=end_index_time)

    def get_mau_every_game():

        result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")
                every_month_result = with_db_context(db, collection_mau_every_game, day=day)
                every_month_result_rows = [{'_on_day': str(day), '_game': row['game'], 'sum': row['sum']} for row in
                                           every_month_result]
                result_proxy.append(every_month_result_rows)
            return result_proxy

        else:

            every_month_result = with_db_context(db, collection_mau_every_game, day=someday)
            every_month_result_rows = [{'_on_day': str(someday), '_game': row['game'], 'sum': row['sum']} for row in
                                       every_month_result]
            result_proxy.append(every_month_result_rows)
            return result_proxy

    result_proxy_for_every_game = get_mau_every_game()

    for rows in result_proxy_for_every_game:

        if rows:

            def sync_collection_mau_every_game(connection, transaction):

                where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                             BIStatistic.__table__.c.game == bindparam('_game'),
                             BIStatistic.__table__.c.platform == 'All Platform')
                values = {'mau': bindparam('sum')}

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print(target + ' MAU for every game transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    print(target + ' MAU for every game transaction.commit()')

            with_db_context(db, sync_collection_mau_every_game)
