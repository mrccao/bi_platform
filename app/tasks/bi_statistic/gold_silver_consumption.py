from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam

from app.extensions import db
from app.models.bi import BIStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_statistic_gold_silver_consumption(target):
    someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_gold_silver_consumption(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
            
            
            
            
            
            
                                            """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
            
            
            
            
            
                                            """), timezone_offset=timezone_offset, on_day=someday)

    result_proxy = with_db_context(db, collection_gold_silver_consumption)

    if target == 'lifetime':
        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]
    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]
    if rows:
        def sync_collection_gold_consumption(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'gold_consumption': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' gold_consumption transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' gold_consumption transaction.commit()')

        with_db_context(db, sync_collection_gold_consumption)

    def collection_silver_consumption(connection, transaction):
        if target == 'lifetime':
            return connection.execute(text("""
            
            
            
            
                                            """), timezone_offset=timezone_offset)
        else:
            return connection.execute(text("""
            
            
            
            
            
            
            
            
                                            """), timezone_offset=timezone_offset, on_day=someday)

    result_proxy = with_db_context(db, collection_silver_consumption)

    if target == 'lifetime':
        rows = [{'_on_day': row['on_day'], 'sum': row['sum']} for row in result_proxy]
    else:
        rows = [{'_on_day': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:
        def sync_collection_silver_consumption(connection, transaction):
            where = and_(BIStatistic.__table__.c.on_day == bindparam('_on_day'),
                         BIStatistic.__table__.c.game == 'All Game',
                         BIStatistic.__table__.c.platform == 'All Platform')
            values = {'gold_consumption': bindparam('sum')}

            try:
                connection.execute(BIStatistic.__table__.update().where(where).values(values), rows)
            except:
                print(target + ' silver_consumption transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' silver_consumption transaction.commit()')

        with_db_context(db, sync_collection_silver_consumption)
