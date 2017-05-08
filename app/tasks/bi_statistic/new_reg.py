from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_new_reg(target):
    _, someday, _, timezone_offset = generate_sql_date(target)

    def collection_new_reg_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            GROUP  BY on_day
                                            """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                           SELECT COUNT(*)                                               AS sum
                                           FROM   bi_user
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_new_reg_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_new_reg_all_platforms(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'new_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' new_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_new_reg_all_platforms)

    def collection_new_reg(connection, transaction):

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

        else:

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
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           GROUP  BY platform
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_new_reg)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_new_reg(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')

            values = {'new_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' new_reg for every platform transaction.commit()')

        with_db_context(db, sync_collection_new_reg)

    def collection_facebook_reg_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect =1
                                            GROUP  BY on_day  
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT COUNT(*) AS sum
                                            FROM   bi_user
                                            WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                   AND facebook_id IS NOT NULL 
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_reg_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_reg_all_platforms(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_reg for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_facebook_reg_all_platforms)

    def collection_facebook_reg(connection, transaction):

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
                                           WHERE  reg_facebook_connect =1
                                           GROUP  BY on_day,
                                                     platform
    #                                         """), timezone_offset=timezone_offset)

        else:

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
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND    facebook_id IS NOT NULL
                                           GROUP  BY platform
                                                   """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_reg)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_reg(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_reg for every platform  transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_reg for every platform transaction.commit()')

        with_db_context(db, sync_collection_facebook_reg)

    def collection_facebook_game_reg_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_source = 'Facebook Game'
                                            GROUP  BY on_day 
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT COUNT(*) AS sum
                                            FROM   bi_user
                                            WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                   AND reg_source = 'Facebook Game';
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_game_reg_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_game_reg_all_platforms(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_game_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_game_reg for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_game_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_facebook_game_reg_all_platforms)

    def collection_facebook_game_reg(connection, transaction):

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
                                           WHERE  reg_source = 'Facebook Game'
                                           GROUP  BY on_day,
                                                     platform
                                             """), timezone_offset=timezone_offset)

        else:

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
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND   reg_source = 'Facebook Game'
                                           GROUP  BY platform
                                                   """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_game_reg)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_game_reg(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_game_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_game_reg for every platform  transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_game_reg for every platform transaction.commit()')

        with_db_context(db, sync_collection_facebook_game_reg)

    def collection_facebook_login_reg_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect =1
                                            AND    reg_source != 'Facebook Game'
                                            GROUP  BY on_day; 
                                            """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT COUNT(*)                                          AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect =1
                                                   AND   reg_source != 'Facebook Game'
                                                   AND    DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_login_reg_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_login_reg_all_platforms(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_login_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_login_reg for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_login_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_facebook_login_reg_all_platforms)

    def collection_facebook_login_reg(connection, transaction):

        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end                                                    AS platform,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 1
                                                   AND reg_source != 'Facebook Game'
                                            GROUP  BY on_day;  
                                             """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                            SELECT COUNT(*) AS sum,
                                                   CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end      AS platform
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 1
                                                   AND reg_source != 'Facebook Game'
                                                   AND DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                            GROUP  BY platform; 
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_facebook_login_reg)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_facebook_login_reg(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'facebook_login_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' facebook_login_reg for every platform  transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' facebook_login_reg for every platform transaction.commit()')

        with_db_context(db, sync_collection_facebook_login_reg)

    def collection_email_reg_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND email IS NOT NULL
                                            GROUP  BY on_day;
                                            """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT COUNT(*)      AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND    email IS NOT NULL
                                                   AND    DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_email_reg_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_email_reg_all_platforms(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'email_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' email_reg for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' email_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_email_reg_all_platforms)

    def collection_email_reg(connection, transaction):

        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end                                                    AS platform,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND email IS NOT NULL
                                            GROUP  BY on_day;
                                             """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                            SELECT COUNT(*) AS sum,
                                                   CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end      AS platform
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND email IS NOT NULL
                                                   AND DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                            GROUP  BY platform;
                                             """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_email_reg)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_email_reg(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'email_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' email_reg for every platform  transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' email_reg for every platform transaction.commit()')

        with_db_context(db, sync_collection_email_reg)

    def collection_email_validated_all_platforms(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND email_validate_time IS NOT NULL
                                            GROUP  BY on_day
                                            """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                           SELECT COUNT(*)                                               AS sum
                                           FROM   bi_user
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                  AND    facebook_id IS NULL
                                                  AND    email_validate_time  IS NOT NULL
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_email_validated_all_platforms)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_email_validated_all_platforms(connection, transaction):

            where = and_(
                BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                BIStatistic.__table__.c.platform == 'All Platform',
                BIStatistic.__table__.c.game == 'All Game'
            )
            values = {'email_validated': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' email_validated for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' email_validated for all platforms transaction.commit()')

        with_db_context(db, sync_collection_email_validated_all_platforms)

    def collection_email_validated(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                   CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end                                                    AS platform,
                                                   COUNT(*)                                               AS sum
                                            FROM   bi_user
                                            WHERE  reg_facebook_connect = 0
                                                   AND email_validate_time IS NOT NULL
                                            GROUP  BY on_day,
                                                      platform
                                            """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                            SELECT CASE
                                                     WHEN LEFT(reg_source, 10) = 'Web Mobile' THEN 'Web Mobile'
                                                     WHEN LEFT(reg_source, 3) = 'Web' THEN 'Web'
                                                     WHEN LEFT(reg_source, 3) = 'iOS' THEN 'iOS'
                                                     WHEN LEFT(reg_source, 8) = 'Facebook' THEN 'Facebook Game'
                                                     WHEN LEFT(reg_source, 7) = 'Android' THEN 'Android'
                                                   end      AS platform,
                                                   COUNT(*) AS sum
                                            FROM   bi_user
                                            WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :on_day
                                                   AND facebook_id IS NULL
                                                   AND email_validate_time IS NOT NULL
                                            GROUP  BY platform;
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_email_validated)

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, '_platform': row['platform'], 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_email_validated(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'email_validated': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' new_reg_validate for every platform transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' email_validated for every platform transaction.commit()')

        with_db_context(db, sync_collection_email_validated)

    def collection_guest_reg_all_platform(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(add_time, '+08:00', :timezone_offset)) AS on_day,
                                                   COUNT(DISTINCT u_id)                                   AS sum
                                            FROM   tb_app_guest
                                            GROUP  BY on_day
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                          SELECT COUNT(DISTINCT u_id) AS sum
                                          FROM   tb_app_guest
                                          WHERE  DATE(CONVERT_TZ(add_time, '+08:00', :timezone_offset)) = :on_day
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_guest_reg_all_platform, bind='orig_wpt')

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_guest_reg_all_platform(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == 'All Platform',
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'guest_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' guest_reg for all platforms transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' guest_reg for all platforms transaction.commit()')

        with_db_context(db, sync_collection_guest_reg_all_platform)

    def collection_guest_reg(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(add_time, '+08:00', :timezone_offset)) AS on_day,
                                                   COUNT(DISTINCT u_id)                                   AS sum,
                                                   CASE
                                                     WHEN reg_device = 5 THEN 'iOS'
                                                     WHEN reg_device = 6 THEN 'Android'
                                                   end                                                    AS platform
                                            FROM   tb_app_guest
                                            GROUP  BY on_day 
                                           """), timezone_offset=timezone_offset)
        else:

            return connection.execute(text("""
                                            SELECT COUNT(DISTINCT u_id) AS sum,
                                                   CASE
                                                     WHEN reg_device = 5 THEN 'iOS'
                                                     WHEN reg_device = 6 THEN 'Android'
                                                   end                  AS platform
                                            FROM   tb_app_guest
                                            WHERE  DATE(CONVERT_TZ(add_time, '+08:00', :timezone_offset)) = :on_day 
                                           """), on_day=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_guest_reg, bind='orig_wpt')

    if target == 'lifetime':

        rows = [{'_on_day': row['on_day'], 'sum': row['sum'], '_platform': row['platform']} for row in result_proxy]

    else:

        rows = [{'_on_day': someday, 'sum': row['sum'], '_platform': row['platform']} for row in result_proxy]

    if rows:

        def sync_collection_guest_reg(connection, transaction):

            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.platform == bindparam('_platform'),
                         BIStatistic.__table__.c.game == 'All Game')
            values = {'guest_reg': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' guest_reg  transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' guest_reg  transaction.commit()')

        with_db_context(db, sync_collection_guest_reg)
