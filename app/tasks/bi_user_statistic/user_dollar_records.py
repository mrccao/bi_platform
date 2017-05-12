from datetime import date

import pandas as pd
from sqlalchemy import bindparam, and_
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_MAPPING
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_dollar_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    column_name = {'Lucky Spin Set': 'lucky_spin_spend', 'Gold': 'dollar_gold_pkg_spend', 'Avatar Set': 'avatar_spend',
                   'Poker Lucky Charm Set': 'charms_spend', 'Silver Coin': 'dollar_silver_pkg_spend',
                   'Emoji Set': 'emoji_spend'}

    def collection_user_dollar_consumption_records(connection, transaction, category, day):
        return connection.execute(text("""

                                    SELECT      user_id,
                                                SUM(currency_amount)                  AS consumption_amount
                                    FROM  bi_user_bill
                                    WHERE currency_type = 'Dollar'
                                    AND   category =:category
                                    AND   DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                    GROUP BY  user_id
                                    """), stats_date=day, timezone_offset=timezone_offset, category=category)

    def confirm_first_recharge_time(connection, transaction, day):
        return connection.execute(text("""
                                    SELECT user_id,
                                      CASE
                                      WHEN min(date((CONVERT_TZ(created_at, '+00:00', :timezone_offset)))) = :day
                                        THEN 1
                                      ELSE
                                        0
                                      END                                            AS dollar_purchase_1st_time
                                    FROM bi_user_bill
                                    WHERE currency_type = 'Dollar'
                                    GROUP BY user_id;
                                           """), stats_date=day, timezone_offset=timezone_offset)

    def confirm_first_recharge_time_for_gold(connection, transaction, day):

        return connection.execute(text("""
                                        SELECT user_id,
                                          CASE
                                          WHEN min(date((CONVERT_TZ(created_at, '+00:00', :timezone_offset)))) = :day
                                            THEN 1
                                          ELSE
                                            0
                                          END                                              AS dollar_purchase_1st_time_gold
                                        FROM bi_user_bill
                                        WHERE currency_type = 'Dollar'
                                              AND category_orig=1
                                        GROUP BY user_id;
                                           """), stats_date=day, timezone_offset=timezone_offset)

    def collection_user_dollar_recharge_records(connection, transaction, day):

        return connection.execute(text("""
                                        SELECT user_id,
                                               Count(*)                                       AS  dollar_purchase_count,
                                               ROUND(SUM(currency_amount), 2)                 AS  dollar_spend
                                        FROM   bi_user_bill
                                        WHERE  currency_type = 'Dollar'
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                        GROUP BY  user_id
                                           """), stats_date=day, timezone_offset=timezone_offset)

    def get_user_dollar_records():

        dollar_consumption_records_result_proxy = []
        dollar_first_time_records_result_proxy = []
        dollar_first_time_records_for_gold_result_proxy = []
        dollar_recharge_records_result_proxy = []

        if target == 'lifetime':

            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:

                day = day.strftime("%Y-%m-%d")

                print('user dollar records on ' + str(day))

                for category in ['Lucky Spin Set', 'Gold',  'Silver Coin', ]:

                    category_sales_record = with_db_context(db, collection_user_dollar_consumption_records, day=day,
                                                            category=category)

                    every_category_sales_records = [{'_stats_date': str(day), '_user_id': row['user_id'],
                                                     column_name[category]: row['consumption_amount']}
                                                    for row in category_sales_record]

                    every_category_sales_record_dict = dict([(category, every_category_sales_records)])

                    dollar_consumption_records_result_proxy.append(every_category_sales_record_dict)

                take_the_first_dollar_recharge = with_db_context(db, confirm_first_recharge_time, day=day)
                every_user_the_first_time_of_dollar_recharge = [{'_stats_date': str(day),
                                                                 '_user_id': row['user_id'],
                                                                 'dollar_purchase_1st_time': row[
                                                                     'dollar_purchase_1st_time']}
                                                                for row in take_the_first_dollar_recharge]

                take_the_first_dollar_recharge_for_gold = with_db_context(db, confirm_first_recharge_time_for_gold,
                                                                          day=day)
                every_user_the_first_dollar_recharge_for_gold = [{'_stats_date': str(day),
                                                                  '_user_id': row['user_id'],
                                                                  'dollar_purchase_1st_time_gold': row[
                                                                      'dollar_purchase_1st_time_gold']}
                                                                 for row in take_the_first_dollar_recharge_for_gold]

                dollar_recharge_records = with_db_context(db, collection_user_dollar_recharge_records, day=day)
                every_user_dollar_recharge_records = [{'_stats_date': str(day), '_user_id': row['user_id'],
                                                       'dollar_spend': row['dollar_spend'],
                                                       'dollar_purchase_count': row['dollar_purchase_count']}
                                                      for row in dollar_recharge_records]

                dollar_first_time_records_result_proxy.append(every_user_the_first_time_of_dollar_recharge)
                dollar_first_time_records_for_gold_result_proxy.append(every_user_the_first_dollar_recharge_for_gold)
                dollar_recharge_records_result_proxy.append(every_user_dollar_recharge_records)

        else:

            print('user dollar records on ' + str(someday))

            for category in ['gold', 'silver', 'spin_ticket', 'lucky_spin']:
                category_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[category]

                category_sales_record = with_db_context(db, collection_user_dollar_consumption_records, day=someday,
                                                        category_orig=category_orig)
                every_category_sales_records = [{'_stats_date': str(someday), '_user_id': row['user_id'],
                                                 column_name[category]: row['consumption_amount']}
                                                for row in category_sales_record]

                every_category_sales_record_dict = dict([(category, every_category_sales_records)])
                dollar_consumption_records_result_proxy.append(every_category_sales_record_dict)

            take_the_first_dollar_recharge = with_db_context(db, confirm_first_recharge_time, day=someday)
            every_user_the_first_time_of_dollar_recharge = [{'_stats_date': str(someday),
                                                             '_user_id': row['user_id'],
                                                             'dollar_purchase_1st_time': row[
                                                                 'dollar_purchase_1st_time']}
                                                            for row in take_the_first_dollar_recharge]

            take_the_first_dollar_recharge_for_gold = with_db_context(db, confirm_first_recharge_time_for_gold,
                                                                      day=someday)
            every_user_the_first_dollar_recharge_for_gold = [{'_stats_date': str(someday),
                                                              '_user_id': row['user_id'],
                                                              'dollar_purchase_1st_time_gold': row[
                                                                  'dollar_purchase_1st_time_gold']}
                                                             for row in take_the_first_dollar_recharge_for_gold]

            dollar_recharge_records = with_db_context(db, collection_user_dollar_recharge_records, day=someday)
            every_user_dollar_recharge_records = [{'_stats_date': str(someday), '_user_id': row['user_id'],
                                                   'dollar_spend': row['dollar_spend'],
                                                   'dollar_purchase_count': row['dollar_purchase_count']}
                                                  for row in dollar_recharge_records]

            dollar_first_time_records_result_proxy.append(every_user_the_first_time_of_dollar_recharge)
            dollar_first_time_records_for_gold_result_proxy.append(every_user_the_first_dollar_recharge_for_gold)
            dollar_recharge_records_result_proxy.append(every_user_dollar_recharge_records)

        return dollar_consumption_records_result_proxy, dollar_first_time_records_result_proxy, \
               dollar_first_time_records_for_gold_result_proxy, dollar_recharge_records_result_proxy

    dollar_consumption_records_result_proxy, dollar_first_time_records_result_proxy, \
    dollar_first_time_records_for_gold_result_proxy, dollar_recharge_records_result_proxy = get_user_dollar_records()

    if dollar_consumption_records_result_proxy:

        for category_sales_record_rows in dollar_consumption_records_result_proxy:

            for category_name, rows in category_sales_record_rows.items():

                if rows:

                    def sync_collection_dollar_consumption_records(connection, transaction):

                        where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                     BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                        values = {column_name[category_name]: bindparam('consumption_amount')}

                        try:

                            connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
                        except:
                            print(target + ' dollar consumption records  transaction.rollback()')
                            transaction.rollback()
                            raise
                        else:
                            print(target + ' dollar consumption records  transaction.commit()')
                            transaction.commit()

                    with_db_context(db, sync_collection_dollar_consumption_records)

    if dollar_first_time_records_result_proxy:

        for rows in dollar_first_time_records_result_proxy:

            if rows:

                def sync_collection_the_first_dollar_recharge_records(connection, transaction):

                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                    values = {'dollar_purchase_1st_time': bindparam('dollar_purchase_1st_time')}

                    try:

                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                    except:

                        print(target + '  dollar first recharge record transaction.rollback()')

                        transaction.rollback()

                        raise

                    else:

                        print(target + '  dollar first recharge record transaction.commit()')

                        transaction.commit()

                with_db_context(db, sync_collection_the_first_dollar_recharge_records)

    if dollar_first_time_records_for_gold_result_proxy:

        for rows in dollar_first_time_records_for_gold_result_proxy:

            if rows:

                def sync_collection_the_first_dollar_recharge_for_gold_records(connection, transaction):

                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                    values = {'dollar_purchase_1st_time_gold': bindparam('dollar_purchase_1st_time_gold')}

                    try:

                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                    except:

                        print(target + '  dollar first recharge for gold record transaction.rollback()')

                        transaction.rollback()

                        raise

                    else:

                        print(target + '  dollar first recharge for gold record transaction.commit()')

                        transaction.commit()

                with_db_context(db, sync_collection_the_first_dollar_recharge_for_gold_records)

    if dollar_recharge_records_result_proxy:

        for rows in dollar_recharge_records_result_proxy:

            if rows:

                def sync_collection_dollar_recharge_records(connection, transaction):

                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                    values = {'dollar_purchase_count': bindparam('dollar_purchase_count'),
                              'dollar_spend': bindparam('dollar_spend')}

                    try:

                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                    except:

                        print(target + ' dollar recharge records transaction.rollback()')

                        transaction.rollback()

                        raise

                    else:

                        print(target + ' dollar recharge records transaction.commit()')

                        transaction.commit()

                with_db_context(db, sync_collection_dollar_recharge_records)
