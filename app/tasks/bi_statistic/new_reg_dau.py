from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context, celery
from app.utils import current_time


@celery.task
def process_bi_statistic_new_reg_dau(target):
    yesterday = current_time().to(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_new_registration_dau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                                  COUNT(DISTINCT uc.user_id)                       AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           GROUP  BY on_day, uc.game_id
                                           """))

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                                  # CASE
                                                  #   WHEN uc.game_id = 25011 THEN 'Texas Poker'
                                                  #   WHEN uc.game_id = 35011 THEN 'TimeSlots'
                                                  # END                                              AS game,
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
                                                  END                                              AS game,
                                                  COUNT(DISTINCT uc.user_id)                       AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           GROUP  BY on_day, uc.game_id
                                           HAVING on_day = :on_day
                                       """), on_day=today)

    result_proxy = with_db_context(db, collection_new_registration_dau)

    rows = [{'_on_day': row['on_day'], '_game': 'All Game', 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_registration_dau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c._day== bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'new_registration_game_dau': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print('process_bi_statistic_for_lifetime new_registration_game_dautransaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_bi_statistic_for_lifetime new_registration_game_dautransaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection_new_registration_dau)
