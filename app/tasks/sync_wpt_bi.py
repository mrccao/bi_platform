import arrow

from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam
from flask import current_app as app

from app.extensions import db
from app.models.orig_wpt_bi import WPTBIUserStatistic
from app.tasks import celery, get_config_value, set_config_value, set_config_value_with_db_instance, with_db_context


def process_user_statistic_newly_added_records():
    config_value = get_config_value(db, 'last_synced_wpt_bi_user_statistic_user_id')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT user_id, 
                                                  reg_time, 
                                                  reward_level,
                                                  gold_balance,
                                                  silver_balance,
                                                  dollar_paid_amount,
                                                  dollar_paid_count,
                                                  last_poker_time,
                                                  last_slots_time,
                                                  last_free_spin_time,
                                                  last_premium_spin_time,
                                                  count_of_dollar_exchanged_for_gold,
                                                  last_time_of_dollar_exchanged_for_gold,
                                                  count_of_gold_exchanged_for_lucky_charm,
                                                  last_time_of_gold_exchanged_for_lucky_charm,
                                                  count_of_gold_exchanged_for_avatar,
                                                  last_time_of_gold_exchanged_for_avatar
                                           FROM   bi_user
                                           ORDER  BY user_id ASC
                                           """))
        return connection.execute(text("""
                                       SELECT user_id, 
                                              reg_time, 
                                              reward_level,
                                              gold_balance,
                                              silver_balance,
                                              dollar_paid_amount,
                                              dollar_paid_count,
                                              last_poker_time,
                                              last_slots_time,
                                              last_free_spin_time,
                                              last_premium_spin_time,
                                              count_of_dollar_exchanged_for_gold,
                                              last_time_of_dollar_exchanged_for_gold,
                                              count_of_gold_exchanged_for_lucky_charm,
                                              last_time_of_gold_exchanged_for_lucky_charm,
                                              count_of_gold_exchanged_for_avatar,
                                              last_time_of_gold_exchanged_for_avatar
                                       FROM   bi_user
                                       WHERE  user_id > :user_id
                                       ORDER  BY user_id ASC
                                       """), user_id=config_value)

    result_proxy = with_db_context(db, collection)

    rows = [{
        'user_id': row['user_id'],
        'reg_time': row['reg_time'],
        'reward_level': row['reward_level'],
        'gold_balance': row['gold_balance'],
        'silver_balance': row['silver_balance'],
        'dollar_paid_amount': row['dollar_paid_amount'],
        'dollar_paid_count': row['dollar_paid_count'],
        'last_poker_time': row['last_poker_time'],
        'last_slots_time': row['last_slots_time'],
        'last_free_spin_time': row['last_free_spin_time'],
        'last_premium_spin_time': row['last_premium_spin_time'],
        'count_of_gold_purchase': row['count_of_dollar_exchanged_for_gold'],
        'last_purchase_gold_time': row['last_time_of_dollar_exchanged_for_gold'],
        'count_of_charms_purchase': row['count_of_gold_exchanged_for_lucky_charm'],
        'last_purchase_charms_time': row['last_time_of_gold_exchanged_for_lucky_charm'],
        'count_of_avatar_purchase': row['count_of_gold_exchanged_for_avatar'],
        'last_purchase_avatar_time': row['last_time_of_gold_exchanged_for_avatar'],
    } for row in result_proxy]

    print('sync_wpt_bi process_user_statistic_newly_added_records new data: ' + str(len(rows)))

    if rows:
        new_config_value = rows[-1]['user_id']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(WPTBIUserStatistic.__table__.insert(), rows)
                set_config_value_with_db_instance(db, 'last_synced_wpt_bi_user_statistic_user_id', new_config_value)
            except:
                print('sync_wpt_bi process_user_statistic_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('sync_wpt_bi process_user_statistic_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection, 'orig_wpt_bi')

    return


def process_user_statistic_newly_updated_records():
    config_value = get_config_value(db, 'last_synced_wpt_bi_user_statistic_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT user_id, 
                                                  reg_time, 
                                                  reward_level,
                                                  gold_balance,
                                                  silver_balance,
                                                  dollar_paid_amount,
                                                  dollar_paid_count,
                                                  last_poker_time,
                                                  last_slots_time,
                                                  last_free_spin_time,
                                                  last_premium_spin_time,
                                                  count_of_dollar_exchanged_for_gold,
                                                  last_time_of_dollar_exchanged_for_gold,
                                                  count_of_gold_exchanged_for_lucky_charm,
                                                  last_time_of_gold_exchanged_for_lucky_charm,
                                                  count_of_gold_exchanged_for_avatar,
                                                  last_time_of_gold_exchanged_for_avatar,
                                                  updated_at
                                           FROM   bi_user
                                           WHERE  updated_at IS NOT NULL
                                           ORDER  BY updated_at ASC
                                           """))
        return connection.execute(text("""
                                       SELECT user_id, 
                                              reg_time, 
                                              reward_level,
                                              gold_balance,
                                              silver_balance,
                                              dollar_paid_amount,
                                              dollar_paid_count,
                                              last_poker_time,
                                              last_slots_time,
                                              last_free_spin_time,
                                              last_premium_spin_time,
                                              count_of_dollar_exchanged_for_gold,
                                              last_time_of_dollar_exchanged_for_gold,
                                              count_of_gold_exchanged_for_lucky_charm,
                                              last_time_of_gold_exchanged_for_lucky_charm,
                                              count_of_gold_exchanged_for_avatar,
                                              last_time_of_gold_exchanged_for_avatar,
                                              updated_at
                                       FROM   bi_user
                                       WHERE  updated_at IS NOT NULL AND updated_at > :updated_at
                                       ORDER  BY updated_at ASC
                                       """), updated_at=config_value)

    result_proxy = with_db_context(db, collection)

    rows = [{
        '_user_id': row['user_id'],
        'reg_time': row['reg_time'],
        'reward_level': row['reward_level'],
        'gold_balance': row['gold_balance'],
        'silver_balance': row['silver_balance'],
        'dollar_paid_amount': row['dollar_paid_amount'],
        'dollar_paid_count': row['dollar_paid_count'],
        'last_poker_time': row['last_poker_time'],
        'last_slots_time': row['last_slots_time'],
        'last_free_spin_time': row['last_free_spin_time'],
        'last_premium_spin_time': row['last_premium_spin_time'],
        'count_of_gold_purchase': row['count_of_dollar_exchanged_for_gold'],
        'last_purchase_gold_time': row['last_time_of_dollar_exchanged_for_gold'],
        'count_of_charms_purchase': row['count_of_gold_exchanged_for_lucky_charm'],
        'last_purchase_charms_time': row['last_time_of_gold_exchanged_for_lucky_charm'],
        'count_of_avatar_purchase': row['count_of_gold_exchanged_for_avatar'],
        'last_purchase_avatar_time': row['last_time_of_gold_exchanged_for_avatar'],
        'updated_at': row['updated_at'],
    } for row in result_proxy]

    print('sync_wpt_bi process_user_statistic_newly_updated_records new data: ' + str(len(rows)))

    if rows:
        new_config_value = rows[-1]['updated_at']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = WPTBIUserStatistic.__table__.c.user_id == bindparam('_user_id')
            values = {
                'reward_level': bindparam('reward_level'),
                'gold_balance': bindparam('gold_balance'),
                'silver_balance': bindparam('silver_balance'),
                'dollar_paid_amount': bindparam('dollar_paid_amount'),
                'dollar_paid_count': bindparam('dollar_paid_count'),
                'last_poker_time': bindparam('last_poker_time'),
                'last_slots_time': bindparam('last_slots_time'),
                'last_free_spin_time': bindparam('last_free_spin_time'),
                'last_premium_spin_time': bindparam('last_premium_spin_time'),
                'count_of_gold_purchase': bindparam('count_of_gold_purchase'),
                'last_purchase_gold_time': bindparam('last_purchase_gold_time'),
                'count_of_charms_purchase': bindparam('count_of_charms_purchase'),
                'last_purchase_charms_time': bindparam('last_purchase_charms_time'),
                'count_of_avatar_purchase': bindparam('count_of_avatar_purchase'),
                'last_purchase_avatar_time': bindparam('last_purchase_avatar_time')
                }

            try:
                connection.execute(WPTBIUserStatistic.__table__.update().where(where).values(values), rows)
                set_config_value_with_db_instance(db, 'last_synced_wpt_bi_user_statistic_update_time', new_config_value)
            except:
                print('sync_wpt_bi process_user_statistic_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('sync_wpt_bi process_user_statistic_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection, 'orig_wpt_bi')

    return


@celery.task
def process_wpt_bi_user_statistic():

    process_user_statistic_newly_added_records()
    print('sync_wpt_bi process_user_statistic_newly_added_records() done.')

    process_user_statistic_newly_updated_records()
    print('sync_wpt_bi process_user_statistic_newly_updated_records() done.')
