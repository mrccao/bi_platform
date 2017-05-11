from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_game_records(target):
    today, someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_buy_ins_and_rake(connection, transaction, game_type_id):

        if target == 'lifetime':

            return connection.execute(text("""
                                           SELECT DATE(CONVERT_TZ(usersign.time, '+08:00', :timezone_offset)) AS stats_date,
                                                   usersign.username                                          AS user_name,
                                                   SUM(CASE
                                                       WHEN usersign.type = 2  THEN
                                                            usersign.sign_totals
                                                       WHEN usersign.type = 1  THEN
                                                            usersign.sign_totals * -1
                                                       ELSE 0
                                                       END)                                                    AS buy_ins,
                                                   SUM(CASE
                                                       WHEN usersign.type = 2  THEN
                                                            usersign.tax_totals
                                                       WHEN usersign.type = 1  THEN
                                                            usersign.tax_totals * -1
                                                       ELSE 0
                                                       END)                                                    AS rake
                                           FROM   tj_flow_usersign   AS usersign 
                                             INNER JOIN tj_matchinfo AS matchinfo 
                                               ON usersign.matchid = matchinfo.matchid
                                           WHERE  matchinfo.type = :game_type_id
                                           GROUP  BY stats_date,user_name;
                                           """), timezone_offset=timezone_offset, game_type_id=game_type_id)
        else:

            return connection.execute(text("""
                                            SELECT usersign.username                                          AS user_name,
                                            SUM(CASE
                                                       WHEN usersign.type = 2  THEN
                                                            usersign.sign_totals
                                                       WHEN usersign.type = 1  THEN
                                                            usersign.sign_totals * -1
                                                       ELSE 0
                                                       END) AS buy_ins,
                                                   SUM(CASE
                                                       WHEN usersign.type = 2  THEN
                                                            usersign.tax_totals
                                                       WHEN usersign.type = 1  THEN
                                                            usersign.tax_totals * -1
                                                       ELSE 0
                                                       END) AS rake
                                            FROM   tj_flow_usersign     AS usersign 
                                              INNER JOIN tj_matchinfo   AS matchinfo 
                                                ON usersign.matchid = matchinfo.matchid
                                            WHERE  matchinfo.type = :game_type_id
                                                   AND DATE(CONVERT_TZ(usersign.time, '+08:00', :timezone_offset)) = :stats_date 
                                           GROUP  BY user_name;
                                           """), stats_date=someday, timezone_offset=timezone_offset,
                                      game_type_id=game_type_id)

    def collection_winnings_records(connection, transaction, game_type_id):

        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(userreward.time, '+08:00', :timezone_offset)) AS stats_date,
                                                   userreward.username                                           AS  username,
                                                   SUM(CASE
                                                       WHEN userreward.rewardtype = 3 THEN userreward.totals
                                                       ELSE 0
                                                       END)                                                    AS winnings
                                            FROM   tj_flow_userreward userreward 
                                              INNER JOIN tj_matchinfo matchinfo 
                                                ON matchinfo.matchid = userreward.matchid
                                            WHERE  matchinfo.type = :game_type_id
                                            GROUP  BY stats_date, userreward.username;
                                           """), timezone_offset=timezone_offset, game_type_id=game_type_id)

        else:
            return connection.execute(text("""
                                            SELECT userreward.username                                AS username,
                                            SUM(CASE
                                                       WHEN userreward.rewardtype = 3 THEN userreward.totals
                                                       ELSE 0
                                                       END) AS winnings
                                            FROM   tj_flow_userreward userreward 
                                              INNER JOIN tj_matchinfo matchinfo 
                                                ON matchinfo.matchid = userreward.matchid
                                            WHERE  matchinfo.type = :game_type_id
                                                   AND DATE(CONVERT_TZ(userreward.time, '+08:00', :timezone_offset)) = :stats_date 
                                            GROUP  BY userreward.username;
                                          """), timezone_offset=timezone_offset, game_type_id=game_type_id,
                                      stats_date=someday)

    if target == 'lifetime':

        result_proxy = []
        result_proxy_for_winnings = []

        for game_type_id, game_type in [(1, 'sng'), (2, 'mtt')]:
            buy_ins_and_rake_records = with_db_context(db, collection_buy_ins_and_rake, game_type_id=game_type_id,
                                                       bind='orig_wpt_ods')

            buy_ins_and_rake_records_rows = [
                {'_stats_date': row['stats_date'], '_username': row['username'], 'rake': row['rake'],
                 'buy_ins': row['buy_ins']}
                for row in buy_ins_and_rake_records]

            buy_ins_and_rake_records_rows_dict = dict([(game_type, buy_ins_and_rake_records_rows)])

            result_proxy.append(buy_ins_and_rake_records_rows_dict)

            winnings_records = with_db_context(db, collection_winnings_records, game_type_id=game_type_id,
                                               bind='orig_wpt_ods')

            winnings_records_rows = [
                {'_stats_date': row['stats_date'], '_username': row['username'], 'winnings': row['winnings']} for row in
                winnings_records]

            winnings_records_rows_dict = dict([(game_type, winnings_records_rows)])

            result_proxy_for_winnings.append(winnings_records_rows_dict)


    else:

        result_proxy = []
        result_proxy_for_winnings = []

        for game_type_id, game_type in [(1, 'sng'), (2, 'mtt')]:
            buy_ins_and_rake_records = with_db_context(db, collection_buy_ins_and_rake, game_type_id=game_type_id,
                                                       bind='orig_wpt_ods')

            buy_ins_and_rake_records_rows = [
                {'_stats_date': someday, '_username': row['username'], 'rake': row['rake'], 'buy_ins': row['buy_ins']}
                for
                row in buy_ins_and_rake_records]

            buy_ins_and_rake_records_rows_dict = dict([(game_type, buy_ins_and_rake_records_rows)])

            result_proxy.append(buy_ins_and_rake_records_rows_dict)

            winnings_records = with_db_context(db, collection_winnings_records, game_type_id=game_type_id,
                                               bind='orig_wpt_ods')

            winnings_records_rows = [{'_stats_date': someday, '_username': row['username'], 'winnings': row['winnings']}
                                     for row in winnings_records]

            winnings_records_rows_dict = dict([(game_type, winnings_records_rows)])

            result_proxy_for_winnings.append(winnings_records_rows_dict)

    for game_buy_ins_and_rake_records_rows_dict in result_proxy:

        for game_type, rows in game_buy_ins_and_rake_records_rows_dict.items():

            if rows:

                def sync_collection_game_buy_ins_and_rake_records(connection, transaction):
                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.username == bindparam('_username'))

                    values = {'{}_gold_buyins'.format(game_type): bindparam('buy_ins'),
                              '{}_gold_rake'.format(game_type): bindparam('rake')}

                    try:
                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print(target + ' buy_ins and rake transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        transaction.commit()
                        print(target + ' buy_ins and rake transaction.commit()')

                with_db_context(db, sync_collection_game_buy_ins_and_rake_records)

    for game_winnings_records_rows_dict in result_proxy_for_winnings:

        for game_type, rows in game_winnings_records_rows_dict.items():

            if rows:

                def sync_collection_game_winnings_records(connection, transaction):
                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.username == bindparam('_username'))

                    values = {'{}_gold_winnings'.format(game_type): bindparam('winnings')}

                    try:
                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
                    except:
                        print(target + ' winnings transaction.rollback()')
                        transaction.rollback()
                        raise
                    else:
                        transaction.commit()
                        print(target + ' winnings transaction.commit()')

                with_db_context(db, sync_collection_game_winnings_records)

    def collection_ring_game_rake(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                                SELECT a.dates     AS  stats_date,
                                                       a.username  AS  username,
                                                       SUM(a.rake) AS  rake,
                                                       SUM(a.hands)
                                                FROM   (SELECT DATE(CONVERT_TZ(s.time_update, '+08:00', :timezone_offset)) AS
                                                               dates,
                                                               SUM(s.pay_num)                                              AS
                                                               rake
                                                        FROM   tj_super_game_scharges_record AS s
                                                               LEFT JOIN tj_matchinfo AS m
                                                                      ON s.matchid = m.matchid
                                                        WHERE  m.type = 3
                                                        GROUP  BY DATE(CONVERT_TZ(s.time_update, '+08:00', '-04:00')),s.rolename
                                                        UNION
                                                        SELECT DATE(CONVERT_TZ(f.time_update, '+08:00', :timezone_offset)) AS
                                                               dates,
                                                               SUM(f.scharges)                                             AS
                                                               rake
                                                        FROM   tj_cgz_flow_usergameinfo AS f
                                                               LEFT JOIN tj_matchinfo AS m
                                                                      ON f.matchid = m.matchid
                                                        WHERE  m.type = 3
                                                        GROUP  BY DATE(CONVERT_TZ(f.time_update, '+08:00', '-04:00')),f.username) AS a
                                                GROUP  BY stats_date,username
                                              """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                                SELECT a.dates     AS stats_date,
                                                       a.username AS  username,
                                                       SUM(a.rake) AS rake,
                                                       SUM(a.hands)
                                                FROM   (SELECT DATE(CONVERT_TZ(s.time_update, '+08:00', :timezone_offset)) AS
                                                               dates,
                                                               SUM(s.pay_num)                                              AS
                                                               rake
                                                        FROM   tj_super_game_scharges_record AS s
                                                               LEFT JOIN tj_matchinfo AS m
                                                                      ON s.matchid = m.matchid
                                                        WHERE  m.type = 3
                                                               AND DATE(CONVERT_TZ(s.time_update, '+08:00', :timezone_offset)) =
                                                                   :stats_date
                                                        GROUP BY s.rolename
                                                        UNION
                                                        SELECT DATE(CONVERT_TZ(f.time_update, '+08:00', :timezone_offset)) AS
                                                               dates,
                                                               SUM(f.scharges)                                             AS
                                                               rake
                                                        FROM   tj_cgz_flow_usergameinfo AS f
                                                               LEFT JOIN tj_matchinfo AS m
                                                                      ON f.matchid = m.matchid
                                                        WHERE  m.type = 3
                                                               AND DATE(CONVERT_TZ(f.time_update, '+08:00', :timezone_offset)) =
                                                                   :stats_date
                                                        GROUP BY f.username
                                                                    ) AS a
                                                GROUP  BY stats_date,username
                                              """), timezone_offset=timezone_offset, stats_date=someday)

    ring_game_rake_records = with_db_context(db, collection_ring_game_rake, bind='orig_wpt_ods')

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_username': row['username'], 'rake': row['rake']} for row in
                ring_game_rake_records]
    else:

        rows = [{'_stats_date': someday, '_username': row['username'], 'rake': row['rake']} for row in
                ring_game_rake_records]

    if rows:

        def sync_collection_ring_game_rake(connection, transaction):
            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.username == bindparam('_username'))

            values = {'ring_rake': bindparam('rake')}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' Ring rake transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' Ring rake transaction.commit()')

        with_db_context(db, sync_collection_ring_game_rake)



        # SELECT rolename,SUM(value),count(*) from readdrecord
        # group by datetime


