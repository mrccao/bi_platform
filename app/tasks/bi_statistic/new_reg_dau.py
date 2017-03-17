from flask import current_app as app
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_statistic_new_reg_dau(target, timezone_offset):
    yesterday = current_time()(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    today = current_time()(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')

    def collection_new_registration_dau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(DISTINCT uc.user_id)                               AS sum
                                            FROM   bi_user u
                                                   LEFT JOIN bi_user_currency uc
                                                          ON u.user_id = uc.user_id
                                            WHERE  uc.transaction_type NOT IN :free_transaction_types
                                            GROUP  BY on_day,
                                                      uc.game_id
                                            """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        if target == 'yesterday':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                  COUNT(DISTINCT uc.user_id)                               AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           WHERE  uc.transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     uc.game_id
                                           HAVING on_day = :on_day
                                       """), on_day=yesterday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)
        if target == 'today':
            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                  COUNT(DISTINCT uc.user_id)                               AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           WHERE  uc.transaction_type NOT IN :free_transaction_types
                                           GROUP  BY on_day,
                                                     uc.game_id
                                           HAVING on_day = :on_day
                                       """), on_day=today, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_new_registration_dau)

    rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_registration_dau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
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
