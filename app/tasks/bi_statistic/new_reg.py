from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context, celery
from app.utils import current_time


@celery.task
def process_bi_statistic_new_reg(target):
    #
    # process_bi_statistic_for_lifetime new_registration
    #
    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_new_registration(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) AS on_day,
                                                  CASE
                                                    WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                    WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                    WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                    WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                    ELSE 'unkonw'
                                                  END                                            AS platform,
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
                                                    ELSE 'unkonw'
                                                  END                                            AS platform,
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
                                                    ELSE 'unkonw'
                                                  END                                            AS platform,
                                                  COUNT(*)                                       AS sum
                                           FROM   bi_user
                                           GROUP  BY on_day, reg_source
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_new_registration)

    rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_registration(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.created_day== bindparam('_on_day'),
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
