from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_new_reg_dau(target):
    someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_new_reg_dau(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(DISTINCT uc.user_id)                               AS sum
                                            FROM   bi_user u
                                                   LEFT JOIN bi_user_currency uc
                                                          ON u.user_id = uc.user_id
                                            WHERE  uc.transaction_type NOT IN :free_transaction_types
                                            GROUP  BY on_day
                                            """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        else:
            return connection.execute(text("""
                                           SELECT COUNT(DISTINCT uc.user_id)                               AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           WHERE  uc.created_at > :index_time
                                                  AND DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND uc.transaction_type NOT IN :free_transaction_types
                                           """), on_day=someday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE, index_time=index_time)

    result_proxy = with_db_context(db, collection_new_reg_dau)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_new_reg_dau(connection, transaction):
            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == 'All Platform'
            )
            values = {
                'new_reg_game_dau': bindparam('sum')
            }

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg_game_dau_transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' new_reg_game_dau_transaction.commit()')
            return

        with_db_context(db, sync_collection_new_reg_dau)
