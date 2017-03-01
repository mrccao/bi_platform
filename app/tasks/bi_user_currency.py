from app.tasks import celery, get_config_value, set_config_value, with_db_context, get_wpt_og_user_mapping
from app.extensions import db
from app.models.bi import BIUser, BIUserCurrency, BIUserBill
from sqlalchemy import text, and_, or_
from sqlalchemy.sql.expression import bindparam


def process_user_gold_currency_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_gold_currency_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT id,
                                                  recdate             AS created_at,
                                                  username            AS og_account,
                                                  gameid              AS game_id,
                                                  producttype         AS transaction_type,
                                                  gamecoin            AS transaction_amount,
                                                  userip              AS ip,
                                                  ( gamecoin + coin ) AS balance
                                           FROM   powergamecoin_detail
                                           WHERE  username IS NOT NULL
                                           ORDER  BY recdate ASC
                                           """))
        return connection.execute(text("""
                                       SELECT id,
                                              recdate             AS created_at,
                                              username            AS og_account,
                                              gameid              AS game_id,
                                              producttype         AS transaction_type,
                                              gamecoin            AS transaction_amount,
                                              userip              AS ip,
                                              ( gamecoin + coin ) AS balance
                                       FROM   powergamecoin_detail
                                       WHERE  recdate >= :recdate AND username IS NOT NULL
                                       ORDER  BY recdate ASC
                                       """), recdate=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_ods')

    #########

    existed_data = []
    if config_value is not None:
        orig_result_proxy = []
        newly_orig_ids = []
        for row in result_proxy:
            orig_result_proxy.append(row)
            newly_orig_ids.append(row['id'])

        def existed_collection(connection, transaction):
            return connection.execute(text("SELECT orig_id FROM bi_user_currency WHERE currency_type = 'Gold' AND orig_id IN :newly_orig_ids"), newly_orig_ids=tuple(newly_orig_ids))
        existed_result_proxy = with_db_context(db, existed_collection)
        existed_data = [row['orig_id'] for row in existed_result_proxy]
        print('process_user_gold_currency_newly_added_records existed data: ' + str(len(existed_data)))

    #########

    proxy = orig_result_proxy if config_value is not None else result_proxy

    rows = [{
        'orig_id': row['id'],
        'user_id': -1,
        'og_account': row['og_account'],
        'game_id': row['game_id'],
        'currency_type': 'Gold',
        'transaction_type': row['transaction_type'],
        'transaction_amount': row['transaction_amount'],
        'ip': row['ip'],
        'balance': row['balance'],
        'created_at': row['created_at']
    } for row in proxy if row['id'] not in existed_data]

    print('process_user_gold_currency_newly_added_records new data: ' + str(len(rows)))

    if rows:
        new_config_value = rows[-1]['created_at']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(BIUserCurrency.__table__.insert(), rows)
                set_config_value(connection, 'last_imported_user_gold_currency_add_time', new_config_value)
            except:
                print('process_user_gold_currency_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_gold_currency_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)
    else:
        print('process_user_gold_currency_newly_added_records no data')

    return


def process_user_silver_currency_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_silver_currency_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT id,
                                                  recdate             AS created_at,
                                                  username            AS og_account,
                                                  gameid              AS game_id,
                                                  producttype         AS transaction_type,
                                                  gamecoin            AS transaction_amount,
                                                  userip              AS ip,
                                                  ( gamecoin + coin ) AS balance
                                           FROM   gamecoin_detail
                                           WHERE  username IS NOT NULL
                                           ORDER  BY recdate ASC
                                           """))
        return connection.execute(text("""
                                       SELECT id,
                                              recdate             AS created_at,
                                              username            AS og_account,
                                              gameid              AS game_id,
                                              producttype         AS transaction_type,
                                              gamecoin            AS transaction_amount,
                                              userip              AS ip,
                                              ( gamecoin + coin ) AS balance
                                       FROM   gamecoin_detail
                                       WHERE  recdate >= :recdate AND username IS NOT NULL
                                       ORDER  BY recdate ASC
                                       """), recdate=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_ods')

    #########

    existed_data = []
    if config_value is not None:
        orig_result_proxy = []
        newly_orig_ids = []
        for row in result_proxy:
            orig_result_proxy.append(row)
            newly_orig_ids.append(row['id'])

        def existed_collection(connection, transaction):
            return connection.execute(text("SELECT orig_id FROM bi_user_currency WHERE currency_type = 'Silver' AND orig_id IN :newly_orig_ids"), newly_orig_ids=tuple(newly_orig_ids))
        existed_result_proxy = with_db_context(db, existed_collection)
        existed_data = [row['orig_id'] for row in existed_result_proxy]
        print('process_user_silver_currency_newly_added_records existed data: ' + str(len(existed_data)))

    #########

    proxy = orig_result_proxy if config_value is not None else result_proxy

    rows = [{
        'orig_id': row['id'],
        'user_id': -1,
        'og_account': row['og_account'],
        'game_id': row['game_id'],
        'currency_type': 'Silver',
        'transaction_type': row['transaction_type'],
        'transaction_amount': row['transaction_amount'],
        'ip': row['ip'],
        'balance': row['balance'],
        'created_at': row['created_at']
    } for row in proxy if row['id'] not in existed_data]

    print('process_user_silver_currency_newly_added_records new data: ' + str(len(rows)))

    if rows:
        for i in range(0, len(rows), 100000):
            group = rows[i:i+100000]
            new_config_value = group[-1]['created_at']

            def sync_collection(connection, transaction):
                """ Sync newly added. """
                try:
                    connection.execute(BIUserCurrency.__table__.insert(), group)
                    set_config_value(connection, 'last_imported_user_silver_currency_add_time', new_config_value)
                except:
                    print('process_user_silver_currency_newly_added_records transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print('process_user_silver_currency_newly_added_records transaction.commit()')
                    transaction.commit()
                return

            with_db_context(db, sync_collection)

        
    else:
        print('process_user_silver_currency_newly_added_records no data')

    return


def process_user_currency_fill_user_id():
    def collection(connection, transaction):
        return connection.execute(text('SELECT user_id, og_account FROM bi_user WHERE og_account IN (SELECT DISTINCT og_account FROM bi_user_currency WHERE user_id = -1)'))

    result_proxy = with_db_context(db, collection)

    rows = [{
        '_og_account': row['og_account'],
        'user_id': row['user_id']
    } for row in result_proxy]

    if rows:

        def sync_collection(connection, transaction):
            where = and_(
                BIUserCurrency.__table__.c.og_account == bindparam('_og_account'),
                BIUserCurrency.__table__.c.user_id == -1
                )
            values = {
                'user_id': bindparam('user_id')
                }

            try:
                connection.execute(BIUserCurrency.__table__.update().where(where).values(values), rows)
            except:
                print('process_user_currency_fill_user_id transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_currency_fill_user_id transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


@celery.task
def process_bi_user_currency():

    process_user_gold_currency_newly_added_records()
    print('process_user_gold_currency_newly_added_records() done.')
    process_user_currency_fill_user_id()
    print('process_user_currency_fill_user_id() done.')

    process_user_silver_currency_newly_added_records()
    print('process_user_silver_currency_newly_added_records() done.')
    process_user_currency_fill_user_id()
    print('process_user_currency_fill_user_id() done.')
