from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_new_reg(target):
    yesterday = current_time(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')
    timezone_offset = app.config['APP_TIMEZONE']

    def collection_new_registration(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                  CASE
                                                    WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  END                                                  AS platform,
                                                  COUNT(*)                                             AS sum
                                           FROM   bi_user
                                           GROUP  BY on_day,
                                                     platform
                                            """), timezone_offset=timezone_offset)

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT CASE
                                                    WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  END                                                   AS platform,
                                                  COUNT(*)                                              AS sum
                                           FROM   bi_user
                                           WHERE DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           GROUP  BY platform
                                           """), on_day=yesterday, timezone_offset=timezone_offset)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT CASE
                                                    WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                  END                                                   AS platform,
                                                  COUNT(*)                                              AS sum
                                           FROM   bi_user
                                           WHERE DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           GROUP  BY platform
                                           """), on_day=today, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_new_registration)

    if target == 'yesterday':

        rows = [{'_on_day': yesterday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    elif target == 'today':

        rows = [{'_on_day': today, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_registration(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.platform == bindparam('_platform'),
                BIStatistic.__table__.c.game == 'All Game'
            )
            values = {
                'new_registration': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' New_registration transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' New_registration for every platform transaction.commit()')
            return

        with_db_context(db, sync_collection_new_registration)

    def collection_new_registration_all_platforms(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            GROUP  BY on_day
                                            """), timezone_offset=timezone_offset)

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT COUNT(*)                                               AS sum
                                           FROM   bi_user
                                           WHERE DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=yesterday, timezone_offset=timezone_offset)

        if target == 'today':
            return connection.execute(text("""
                                           SELECT COUNT(*)                                               AS sum
                                           FROM   bi_user
                                           WHERE DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=today, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_new_registration_all_platforms)

    if target == 'yesterday':

        rows = [{'_on_day': yesterday, 'sum': row['sum']} for row in result_proxy]

    elif target == 'today':

        rows = [{'_on_day': today, 'sum': row['sum']} for row in result_proxy]

    else:
        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_registration(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.platform == 'All Platform',
                BIStatistic.__table__.c.game == 'All Game'
            )
            values = {
                'new_registration': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' New_registration transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' New_registration for all platforms transaction.commit()')
            return

        with_db_context(db, sync_collection_new_registration)
