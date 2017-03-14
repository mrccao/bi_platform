from datetime import date

import pandas as pd
from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context, celery
from app.utils import current_time


@celery.task
def process_bi_statistic_wau(target):
    #
    # process_bi_statistic_for_lifetime mau
    #
    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_wau_every_game(connection, transaction):

        if target == 'lifetime':
            tmp_proxy = []
            for month_day in pd.date_range(date(2016, 6, 1), date(2017, 12, 31)):
                every_month_result = connection.execute(text("""
                                               SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                               CASE
                                                    WHEN game_id =25011 THEN 'Texas Poker'
                                                    WHEN game_id =35011 THEN 'TimeSlots'
                                               END                           AS game,
                                                    COUNT(DISTINCT user_id)  AS sum
                                               FROM bi_user_currency
                                               WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                      AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                               GROUP BY on_month,game
                                               HAVING on_month =:month_day
                                               """), month_day=month_day.strftime("%Y-%m-%d"))
                tmp_proxy.append(every_month_result)
            return tmp_proxy

        if target == 'yesterday':
            return connection.execute(text("""
                                               SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                               CASE
                                                    WHEN game_id =25011 THEN 'Texas Poker'
                                                    WHEN game_id =35011 THEN 'TimeSlots'
                                               END                           AS game,
                                                    COUNT(DISTINCT user_id)  AS sum
                                               FROM bi_user_currency
                                               WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                      AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                               GROUP BY on_month,game_id
                                               HAVING on_month =:month_day
                                               """), month_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                               SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                               CASE
                                                    WHEN game_id =25011 THEN 'Texas Poker'
                                                    WHEN game_id =35011 THEN 'TimeSlots'
                                               END                           AS game,
                                                    COUNT(DISTINCT user_id)  AS sum
                                               FROM bi_user_currency
                                               WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                      AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                               GROUP BY on_month,game
                                               HAVING on_month =:month_day
                                               """), month_day=today)

    def collection_wau_all_games(connection, transaction):

        if target == 'lifetime':
            tmp_proxy = []
            for month_day in pd.date_range(date(2016, 6, 1), date(2017, 12, 31)):
                every_week_result = connection.execute(text("""
                                               SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                                       COUNT(DISTINCT user_id)  AS sum
                                               FROM bi_user_currency
                                               WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                      AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                               GROUP BY on_month
                                               HAVING on_month =:month_day
                                               """), month_day=month_day.strftime("%Y-%m-%d"))
                tmp_proxy.append(every_week_result)

        if target == 'yesterday':
            return connection.execute(text("""
                                                   SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                                           COUNT(DISTINCT user_id)  AS sum
                                                   FROM bi_user_currency
                                                   WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                          AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                                   GROUP BY on_month
                                                   HAVING on_month =:month_day
                                                   """), month_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                                   SELECT  DATE (convert_tz(created_at, '+00:00', '-05:00')) AS on_month,
                                                           COUNT(DISTINCT user_id)  AS sum
                                                   FROM bi_user_currency
                                                   WHERE created_at < date(convert_tz(:month_day, '+00:00', '-05:00'))
                                                          AND created_at >=date_add(date(convert_tz(:month_day, '+00:00', '-05:00')),INTERVAL -7 DAY)
                                                   GROUP BY on_month
                                                   HAVING on_month =:month_day
                                                   """), month_day=today)

    def sync_collection_wau_every_game(connection, transaction):

        where = and_(
            BIStatistic.__table__.c.created_day == bindparam('_month'),
            BIStatistic.__table__.c.game == bindparam('_game'),
            BIStatistic.__table__.c.platform == 'All Platform'
        )
        values = {
            'wau': bindparam('sum')
        }

        if target == 'lifetime':

            result_total_proxy = with_db_context(db, collection_wau_every_game)

            if result_total_proxy is None:
                return

            for result_proxy in result_total_proxy:

                rows = [{'_month': row['on_month'], '_game': row['game'], 'sum': row['sum']} for row in result_proxy]

                if rows:

                    try:
                        connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print('process_bi_statistic_for_lifetime wau for every game transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        print('process_bi_statistic_for_lifetime wau for every game transaction.commit()')
                        transaction.commit()

        else:

            result_proxy = with_db_context(db, collection_wau_every_game)

            if result_proxy is None:

                return

            rows = [{'_month': row['on_month'], '_game': row['game'], 'sum': row['sum']} for row in result_proxy]

            if rows:

                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print('process_bi_statistic_for_lifetime wau for every game transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print('process_bi_statistic_for_lifetime wau for every game transaction.commit()')
                    transaction.commit()

    def sync_collection_wau_all_games(connection, transaction):

        where = and_(
            BIStatistic.__table__.c.created_day == bindparam('_month'),
            BIStatistic.__table__.c.game == 'All Game',
            BIStatistic.__table__.c.platform == 'All Platform'
        )
        values = {
            'wau': bindparam('sum')
        }

        if target == 'lifetime':

            result_total_proxy = with_db_context(db, collection_wau_all_games)

            if result_total_proxy is None:
                return

            for result_proxy in result_total_proxy:

                rows = [{'_month': row['on_month'], '_game': row['game'], 'sum': row['sum']} for row in result_proxy]

                if rows:
                    try:
                        connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print('process_bi_statistic_for_lifetime wau for all games transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        print('process_bi_statistic_for_lifetime wau for all games transaction.commit()')
                        transaction.commit()

        else:

            result_proxy = with_db_context(db, collection_wau_all_games)

            if result_proxy is None :
                return

            rows = [{'_month': row['on_month'], '_game': 'All Game', 'sum': row['sum']} for row in result_proxy]

            if rows:
                try:
                    connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print('process_bi_statistic_for_lifetime wau for all games transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print('process_bi_statistic_for_lifetime wau for all games transaction.commit()')
                    transaction.commit()

    with_db_context(db, sync_collection_wau_every_game)
    with_db_context(db, sync_collection_wau_all_games)
