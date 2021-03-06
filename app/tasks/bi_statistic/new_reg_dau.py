import arrow
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.constants import FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_new_reg_dau(target):
    _, someday, _, timezone_offset = generate_sql_date(target)

    def collection_new_reg_dau(connection, transaction):

        start_index_time = arrow.get(someday).replace(days=-3).format('YYYY-MM-DD')
        end_index_time = arrow.get(someday).replace(days=+3).format('YYYY-MM-DD')

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
                                           WHERE  uc.created_at > :start_index_time AND uc.created_at < :end_index_time
                                                  AND DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND uc.transaction_type NOT IN :free_transaction_types
                                           """), on_day=someday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE,
                                      start_index_time=start_index_time, end_index_time=end_index_time)

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
            values = {'new_reg_game_dau': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg_game_dau  for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' new_reg_game_dau  for all platforms transaction.commit()')

        with_db_context(db, sync_collection_new_reg_dau)

    def collection_new_reg_dau_every_platform(connection, transaction):

        start_index_time = arrow.get(someday).replace(days=-3).format('YYYY-MM-DD')
        end_index_time = arrow.get(someday).replace(days=+3).format('YYYY-MM-DD')

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   u.reg_platform                                           AS platform,
                                                   COUNT(DISTINCT uc.user_id)                               AS sum
                                            FROM   bi_user u
                                                   LEFT JOIN bi_user_currency uc
                                                          ON u.user_id = uc.user_id
                                            WHERE  uc.transaction_type NOT IN :free_transaction_types
                                            GROUP  BY on_day, u.reg_platform
                                            """), timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE)

        else:

            return connection.execute(text("""
                                           SELECT u.reg_platform                                           AS platform,
                                                  COUNT(DISTINCT uc.user_id)                               AS sum
                                           FROM   bi_user u
                                                  LEFT JOIN bi_user_currency uc
                                                         ON u.user_id = uc.user_id
                                           WHERE  uc.created_at > :start_index_time AND uc.created_at < :end_index_time
                                                  AND DATE(CONVERT_TZ(u.reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND uc.transaction_type NOT IN :free_transaction_types
                                           GROUP BY u.reg_platform
                                           """), on_day=someday, timezone_offset=timezone_offset,
                                      free_transaction_types=FREE_TRANSACTION_TYPES_TUPLE,
                                      start_index_time=start_index_time, end_index_time=end_index_time)

    result_proxy = with_db_context(db, collection_new_reg_dau_every_platform)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_new_reg_dau_every_platform(connection, transaction):

            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.game == 'All Game',
                BIStatistic.__table__.c.platform == bindparam('_platform')
            )
            values = {'new_reg_game_dau': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg_game_dau for every platform transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' new_reg_game_dau  for  every platform transaction.commit()')

        with_db_context(db, sync_collection_new_reg_dau_every_platform)
