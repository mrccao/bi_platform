from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_new_reg(target):
    _, someday, _, timezone_offset = generate_sql_date(target)

    def collection_user_new_reg_records(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS stats_date,
                                            user_id
                                            FROM   bi_user
                                            """), timezone_offset=timezone_offset)

        else:

            return connection.execute(text("""
                                           SELECT user_id 
                                           FROM   bi_user
                                           WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) = :stats_date
                                           """), stats_date=someday, timezone_offset=timezone_offset)

    result_proxy = with_db_context(db, collection_user_new_reg_records)

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], '_user_id': row['user_id']} for row in result_proxy]

    else:

        rows = [{'_stats_date': someday, '_user_id': row['user_id']} for row in result_proxy]

    if rows:

        def sync_collection_user_new_reg_records(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

            values = {'new_reg': 1}

            try:

                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

            except:

                print(target + ' user new_reg transaction.rollback()')

                transaction.rollback()

                raise

            else:

                transaction.commit()

                print(target + ' user new_reg transaction.commit()')

        with_db_context(db, sync_collection_user_new_reg_records)
