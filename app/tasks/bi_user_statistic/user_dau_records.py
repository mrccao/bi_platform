from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_dau(target):
    _, someday, _, timezone_offset = generate_sql_date(target)

    def collection_user_ring_game_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DISTINCT username                                                 AS
                                                            username,
                                                            DATE(CONVERT_TZ(created_at, '+08:00', :timezone_offset)) AS
                                                            stats_date
                                            FROM   tj_cgz_flow_userpaninfo 
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT DISTINCT username AS username
                                            FROM   tj_cgz_flow_userpaninfo
                                            WHERE  DATE(CONVERT_TZ(created_at, '+08:00', :timezone_offset)) = :stats_date 
                                           """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_ring_game_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_username': row['username']} for row in result_proxy]

    if rows:

        def sync_collection_user_ring_game_dau(connection, transaction):

            for row in rows:

                table_index = row['_stats_date']
                someday_BIUserStatistic = BIUserStatistic.model(table_index)

                where = and_(someday_BIUserStatistic.c.stats_date == bindparam('_stats_date'),
                             someday_BIUserStatistic.c.username == bindparam('_username'))

                values = {'ring_dau': 1}

                try:

                    connection.execute(someday_BIUserStatistic.__table__.update().where(where).values(values), row)

                except:

                    print(target + ' ring dau transaction.rollback()')

                    transaction.rollback()

                    raise

                else:

                    transaction.commit()

                    print(target + ' ring dau transaction.commit()')

        with_db_context(db, sync_collection_user_ring_game_dau)

    # user_id_orig
    def collection_user_sng_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DISTINCT username                                              AS
                                                            org_account,
                                                            DATE(CONVERT_TZ(endtime, '+08:00', :timezone_offset)) AS
                                                            stats_date
                                            FROM   usermatchrecord r
                                                   INNER JOIN tj_matchinfo m
                                                           ON r.matchid = m.matchid
                                            WHERE  m.type = 1 
                                            """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT DISTINCT username AS org_account
                                            FROM   usermatchrecord r
                                                   INNER JOIN tj_matchinfo m
                                                           ON r.matchid = m.matchid
                                            WHERE  m.type = 1
                                                   AND DATE(CONVERT_TZ(endtime, '+08:00', :timezone_offset)) = :stats_date 
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_sng_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_org_account': row['org_account']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_org_account': row['org_account']} for row in result_proxy]

    if rows:

        def sync_collection_user_sng_dau(connection, transaction):

            for row in rows:

                table_index = row['_stats_date']
                someday_BIUserStatistic = BIUserStatistic.model(table_index)

                where = and_(someday_BIUserStatistic.c.stats_date == bindparam('_stats_date'),
                             someday_BIUserStatistic.c.username == bindparam('_username'))

                values = {'sng_dau': 1}

                try:

                    connection.execute(someday_BIUserStatistic.__table__.update().where(where).values(values), row)

                except:

                    print(target + ' sng dau transaction.rollback()')

                    transaction.rollback()

                    raise

                else:

                    transaction.commit()

                    print(target + ' sng dau transaction.commit()')

        with_db_context(db, sync_collection_user_sng_dau)

    # user_id_orig

    def collection_user_mtt_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DISTINCT username                                              AS
                                                            org_account,
                                                            DATE(CONVERT_TZ(endtime, '+08:00', :timezone_offset)) AS
                                                            stats_date
                                            FROM   usermatchrecord r
                                                   INNER JOIN tj_matchinfo m
                                                           ON r.matchid = m.matchid
                                            WHERE  m.type = 2 
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                            SELECT DISTINCT username AS org_account
                                            FROM   usermatchrecord r
                                                   INNER JOIN tj_matchinfo m
                                                           ON r.matchid = m.matchid
                                            WHERE  m.type = 2
                                                   AND DATE(CONVERT_TZ(endtime, '+08:00', :timezone_offset)) = :stats_date 
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_mtt_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_org_account': row['_org_account']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_org_account': row['_org_account']} for row in result_proxy]

    if rows:

        def sync_collection_user_sng_dau(connection, transaction):

            for row in rows:
                table_index = row['_stats_date']
                someday_BIUserStatistic = BIUserStatistic.model(table_index)

                where = and_(someday_BIUserStatistic.c.stats_date == bindparam('_stats_date'),
                             someday_BIUserStatistic.c.username == bindparam('_username'))

                values = {'mtt_dau': 1}

                try:

                    connection.execute(someday_BIUserStatistic.__table__.update().where(where).values(values), row)

                except:

                    print(target + ' mtt dau transaction.rollback()')

                    transaction.rollback()

                    raise

                else:

                    transaction.commit()

                    print(target + ' mtt dau transaction.commit()')

        with_db_context(db, sync_collection_user_sng_dau)

    def collection_user_store_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DISTINCT user_id                                                  AS
                                                            user_id,
                                                            DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS
                                                            stats_date
                                            FROM   bi_user_bill 
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                            SELECT DISTINCT user_id AS user_id
                                            FROM   bi_user_bill
                                            WHERE  DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date 
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_store_dau)

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_user_id': row['user_id']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_user_id': row['user_id']} for row in result_proxy]

    if rows:

        def sync_collection_user_store_dau(connection, transaction):

            for row in rows:
                table_index = row['_stats_date']
                someday_BIUserStatistic = BIUserStatistic.model(table_index)

                where = and_(someday_BIUserStatistic.c.stats_date == bindparam('_stats_date'),
                             someday_BIUserStatistic.c.username == bindparam('_username'))

                values = {'store_dau': 1}

                try:
                    connection.execute(someday_BIUserStatistic.__table__.update().where(where).values(values), row)

                except:
                    print(target + ' store dau transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                    print(target + ' store dau transaction.commit()')

        with_db_context(db, sync_collection_user_store_dau)

    # user_id_orig

    def collection_user_slots_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DISTINCT username                                              AS
                                                            org_account,
                                                            DATE(CONVERT_TZ(recdate, '+08:00', :timezone_offset)) AS
                                                            stats_date
                                            FROM   gamecoin_detail 
                                               """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT DISTINCT username AS org_account
                                            FROM   gamecoin_detail
                                            WHERE  DATE(CONVERT_TZ(recdate, '+08:00', :timezone_offset)) = :stats_date 
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_slots_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_org_account': row['org_account']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_org_account': row['org_account']} for row in result_proxy]

    if rows:

        def sync_collection_user_slots_dau(connection, transaction):

            for row in rows:
                table_index = row['_stats_date']
                someday_BIUserStatistic = BIUserStatistic.model(table_index)

                where = and_(someday_BIUserStatistic.c.stats_date == bindparam('_stats_date'),
                             someday_BIUserStatistic.c.username == bindparam('_username'))

                values = {'slots_dau': 1}

                try:
                    connection.execute(someday_BIUserStatistic.__table__.update().where(where).values(values), row)

                except:

                    print(target + ' slots dau transaction.rollback()')

                    transaction.rollback()

                    raise

                else:

                    transaction.commit()

                    print(target + ' slots dau transaction.commit()')

        with_db_context(db, sync_collection_user_slots_dau)
