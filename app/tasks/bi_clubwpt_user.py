from app.tasks import celery, get_config_value, set_config_value, with_db_context
from app.extensions import db
from app.models.bi import BIClubWPTUser
from app.utils import current_time
from sqlalchemy import text, and_, or_
from sqlalchemy.sql.expression import bindparam


def parse_email_or_username(content):
    if content is not None:
        return content.replace('"', '').replace(' ', '')
    return content


def process_clubwpt_user_newly_added_records():
    config_value = get_config_value(db, 'last_imported_clubwpt_user_add_time')

    if config_value is None:
        def collection(connection, transaction):
            """ Get newly added. """
            return connection.execute(text('SELECT * FROM club_player_detail'))

        result_proxy = with_db_context(db, collection, 'orig_wpt')

        rows = [{
            'orig_user_id': row['cust_id'],
            'email': parse_email_or_username(row['email']),
            'orig_email': row['email'],
            'username': parse_email_or_username(row['username']),
            'orig_username': row['username'],
            'gold_balance': row['gold_balance'],
            'exchanged_at': row['exchange_time'],
            'exchanged_user_id': row['exchanged_user_id']
        } for row in result_proxy]

        if rows:
            def sync_collection(connection, transaction):
                """ Sync newly added. """
                try:
                    connection.execute(BIClubWPTUser.__table__.insert(), rows)
                    set_config_value(connection, 'last_imported_clubwpt_user_add_time', current_time().format('YYYY-MM-DD HH:mm:ss'))
                except:
                    print('process_clubwpt_user_newly_added_records transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print('process_clubwpt_user_newly_added_records transaction.commit()')
                    transaction.commit()
                return

            with_db_context(db, sync_collection)

    return


def process_clubwpt_user_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_clubwpt_user_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text('SELECT * FROM club_player_detail WHERE exchange_time IS NOT NULL ORDER BY exchange_time ASC'))
        return connection.execute(text('SELECT * FROM club_player_detail WHERE exchange_time IS NOT NULL AND exchange_time >= :update_time  ORDER BY exchange_time ASC'), update_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_orig_user_id': row['cust_id'],
        'exchanged_at': row['exchange_time'],
        'exchanged_user_id': row['exchanged_user_id']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['exchanged_at']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIClubWPTUser.__table__.c.orig_user_id == bindparam('_orig_user_id')
            values = {
                'exchanged_at': bindparam('exchanged_at'),
                'exchanged_user_id': bindparam('exchanged_user_id')
                }

            try:
                connection.execute(BIClubWPTUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_clubwpt_user_update_time', new_config_value)
            except:
                print('process_clubwpt_user_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_clubwpt_user_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


@celery.task
def process_bi_clubwpt_user():

    process_clubwpt_user_newly_added_records()
    print('process_clubwpt_user_newly_added_records() done.')

    process_clubwpt_user_newly_updated_records()
    print('process_clubwpt_user_newly_updated_records() done.')
