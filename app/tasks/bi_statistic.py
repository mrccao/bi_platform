from flask import current_app as app
from app.tasks import celery, get_config_value, set_config_value, with_db_context, get_wpt_og_user_mapping
from app.extensions import db
from app.utils import current_time
from app.models.bi import BIUser, BIStatistic, BIUserCurrency, BIUserBill
from sqlalchemy import text, and_, or_
from sqlalchemy.sql.expression import bindparam


@celery.task
def process_bi_statistic(target):

    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    #
    # process_bi_statistic_for_lifetime new_registration
    #

    def collection_new_registration(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  end                                            AS platform,
                                                  COUNT(*)                                       AS sum
                                           FROM   bi_user
                                           GROUP  BY on_day, reg_source
                                       """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  end                                            AS platform,
                                                  COUNT(*)                                       AS sum
                                           FROM   bi_user
                                           GROUP  BY on_day, reg_source
                                           HAVING on_day = :on_day
                                       """), on_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  end                                            AS platform,
                                                  COUNT(*)                                       AS sum
                                           FROM   bi_user
                                           GROUP  BY on_day, reg_source
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_new_registration)

    rows = [{
        '_on_day': row['on_day'],
        '_platform': row['platform'],
        'sum': row['sum']
    } for row in result_proxy]

    if rows:
        def sync_collection_new_registration(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.day == bindparam('_on_day'),
                BIStatistic.__table__.c.platform == bindparam('_platform'),
                BIStatistic.__table__.c.game == 'All Game'
                )
            values = {
                'new_registration': bindparam('sum')
                }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime new_registration transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime new_registration transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_new_registration)

    #
    # process_bi_statistic_for_lifetime dau
    #

    def collection_dau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day, game
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
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
                                                  end                                              AS game,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day, game
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_dau)

    rows = [{
        '_on_day': row['on_day'],
        '_game': row['game'],
        'sum': row['sum']
    } for row in result_proxy]

    if rows:
        def sync_collection_dau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == bindparam('_game'),
                BIStatistic.__table__.c.platform == 'All Platform'
                )
            values = {
                'dau': bindparam('sum')
                }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime dau transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime dau transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_dau)

    #
    # process_bi_statistic_for_lifetime mau
    #

    def collection_mau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day, game
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN game_id = 25011 THEN 'Texas Poker'
                                                    WHEN game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
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
                                                  end                                              AS game,
                                                  COUNT(DISTINCT user_id)                          AS sum
                                           FROM   bi_user_currency
                                           GROUP  BY on_day, game
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_mau)

    rows = [{
        '_on_day': row['on_day'],
        '_game': row['game'],
        'sum': row['sum']
    } for row in result_proxy]

    if rows:
        def sync_collection_mau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == bindparam('_game'),
                BIStatistic.__table__.c.platform == 'All Platform'
                )
            values = {
                'mau': bindparam('sum')
                }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime mau transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime mau transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_dau)

    #
    # process_bi_statistic_for_lifetime new_registration_dau
    #

    def collection_new_registration_dau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN uc.game_id = 25011 THEN 'Texas Poker'
                                                    WHEN uc.game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
                                                  COUNT(DISTINCT uc.user_id)                       AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           GROUP  BY on_day, uc.game_id
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN uc.game_id = 25011 THEN 'Texas Poker'
                                                    WHEN uc.game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
                                                  COUNT(DISTINCT uc.user_id)                       AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           GROUP  BY on_day, uc.game_id
                                           HAVING on_day = :on_day
                                       """), on_day=yesterday)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN uc.game_id = 25011 THEN 'Texas Poker'
                                                    WHEN uc.game_id = 35011 THEN 'TimeSlots'
                                                  end                                              AS game,
                                                  COUNT(DISTINCT uc.user_id)                       AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           GROUP  BY on_day, uc.game_id
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_new_registration_dau)

    rows = [{
        '_on_day': row['on_day'],
        '_game': row['game'],
        'sum': row['sum']
    } for row in result_proxy]

    if rows:
        def sync_collection_new_registration_dau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == bindparam('_game'),
                BIStatistic.__table__.c.platform == 'All Platform'
                )
            values = {
                'new_registration_dau': bindparam('sum')
                }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime new_registration_dau transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime new_registration_dau transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_new_registration_dau)


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
                                                      end)                                         AS dollar_paid_succeeded_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_succeeded_amount,
                                                  SUM(CASE
                                                        WHEN ( user_paylog_status_id = 4
                                                                OR user_paylog_status_id = 5 ) THEN 1
                                                        ELSE 0
                                                      end)                                         AS dollar_paid_failed_count,
                                                  ROUND(SUM(CASE
                                                              WHEN ( user_paylog_status_id = 4
                                                                      OR user_paylog_status_id = 5 ) THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_failed_amount
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
                                                      end)                                         AS dollar_paid_succeeded_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_succeeded_amount,
                                                  SUM(CASE
                                                        WHEN ( user_paylog_status_id = 4
                                                                OR user_paylog_status_id = 5 ) THEN 1
                                                        ELSE 0
                                                      end)                                         AS dollar_paid_failed_count,
                                                  ROUND(SUM(CASE
                                                              WHEN ( user_paylog_status_id = 4
                                                                      OR user_paylog_status_id = 5 ) THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_failed_amount
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
                                                      end)                                         AS dollar_paid_succeeded_count,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_succeeded_amount,
                                                  SUM(CASE
                                                        WHEN ( user_paylog_status_id = 4
                                                                OR user_paylog_status_id = 5 ) THEN 1
                                                        ELSE 0
                                                      end)                                         AS dollar_paid_failed_count,
                                                  ROUND(SUM(CASE
                                                              WHEN ( user_paylog_status_id = 4
                                                                      OR user_paylog_status_id = 5 ) THEN order_price
                                                              ELSE 0
                                                            end) / 100, 2)                         AS dollar_paid_failed_amount
                                           FROM   user_paylog
                                           GROUP  BY on_day
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_revenue, 'orig_wpt_payment')

    rows = [{
        '_on_day': row['on_day'],
        'dollar_paid_user_count': row['dollar_paid_user_count'],
        'dollar_paid_succeeded_count': row['dollar_paid_succeeded_count'],
        'dollar_paid_succeeded_amount': row['dollar_paid_succeeded_amount'],
        'dollar_paid_failed_count': row['dollar_paid_failed_count'],
        'dollar_paid_failed_amount': row['dollar_paid_failed_amount']
    } for row in result_proxy]

    if rows:
        def sync_collection_revenue(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
                )
            values = {
                'dollar_paid_user_count': bindparam('dollar_paid_user_count'),
                'dollar_paid_succeeded_count': bindparam('dollar_paid_succeeded_count'),
                'dollar_paid_succeeded_amount': bindparam('dollar_paid_succeeded_amount'),
                'dollar_paid_failed_count': bindparam('dollar_paid_failed_count'),
                'dollar_paid_failed_amount': bindparam('dollar_paid_failed_amount')
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
