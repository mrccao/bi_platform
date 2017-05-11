from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_dau(target):
    _, someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_user_ring_game_dau(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                        SELECT  DISTINCT(username),
                                                DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS stats_date
                                        FROM tj_cgz_flow_userpaninfo
                                        GROUP BY DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) ,
                                                 username 
                                           """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                        SELECT  DISTINCT(username)
                                        FROM tj_cgz_flow_userpaninfo
                                        WHERE DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                           """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_ring_game_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_username': row['username']} for row in result_proxy]

    if rows:

        def sync_collection_user_ring_game_dau(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.username == bindparam('_username'))

            values = {'ring_dau': 1}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
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
                                            SELECT  username,
                                            DATE(CONVERT_TZ(endtime, '+00:00', :timezone_offset))  AS stats_date
                                            FROM usermatchrecord r INNER JOIN tj_matchinfo m
                                                ON r.matchid =m.matchid
                                            WHERE m.type =1
                                            GROUP BY stats_date
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                            SELECT  username
                                            FROM usermatchrecord r INNER JOIN tj_matchinfo m
                                                ON r.matchid =m.matchid
                                            WHERE m.type =1
                                            AND DATE(CONVERT_TZ(endtime, '+00:00', :timezone_offset)) = :stats_date
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_sng_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_username': row['username']} for row in result_proxy]

    if rows:

        def sync_collection_user_sng_dau(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.username == bindparam('_username'))

            values = {'sng_dau': 1}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

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
                                            SELECT  username,
                                            DATE(CONVERT_TZ(endtime, '+00:00', :timezone_offset))  AS stats_date
                                            FROM usermatchrecord r INNER JOIN tj_matchinfo m
                                                ON r.matchid =m.matchid
                                            WHERE m.type =2
                                            GROUP BY stats_date
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                            SELECT  username
                                            FROM usermatchrecord r INNER JOIN tj_matchinfo m
                                                ON r.matchid =m.matchid
                                            WHERE m.type =2
                                            AND DATE(CONVERT_TZ(endtime, '+00:00', :timezone_offset)) = :stats_date
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_mtt_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_username': row['username']} for row in result_proxy]

    if rows:

        def sync_collection_user_sng_dau(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.username == bindparam('_username'))

            values = {'mtt_dau': 1}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

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

                                              SELECT  DISTINCT(user_id)  AS user_id,
                                              DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS stats_date
                                              FROM bi_user_bill
                                              GROUP BY  stats_date
                                              
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                              SELECT  DISTINCT(user_id)  AS user_id
                                              FROM bi_user_bill
                                              WHERE DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
 
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_store_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_user_id': row['user_id']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_user_id': row['user_id']} for row in result_proxy]

    if rows:

        def sync_collection_user_store_dau(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

            values = {'store_dau': 1}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

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

                                              SELECT  DISTINCT(username)  AS username,
                                              DATE(CONVERT_TZ(recdate, '+00:00', :timezone_offset)) AS stats_date
                                              FROM gamecoin_detail
                                              GROUP BY  stats_date
                                               """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
                                              SELECT  DISTINCT(username)  AS username
                                              FROM gamecoin_detail
                                              WHERE DATE(CONVERT_TZ(recdate, '+00:00', :timezone_offset)) = :stats_date
                                               """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_slots_dau, bind='wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_username': row['username']} for row in result_proxy]

    if rows:

        def sync_collection_user_slots_dau(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.user_id == bindparam('_username'))

            values = {'slots_dau': 1}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

            except:
                print(target + ' slots dau transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' slots dau transaction.commit()')

        with_db_context(db, sync_collection_user_slots_dau)
