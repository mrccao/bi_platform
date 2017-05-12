from datetime import date

import pandas as pd
from sqlalchemy import bindparam, and_
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_MAPPING, GOLD_FREE_TRANSACTION_TYPES_TUPLE
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_consumption_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    column_name = {'Lucky Spin Set': 'lucky_spin_spend', 'Gold': 'dollar_gold_pkg_spend', 'Avatar Set': 'avatar_spend',
                   'Poker Lucky Charm Set': 'charms_spend', 'Silver Coin': 'dollar_silver_pkg_spend',
                   'Emoji Set': 'emoji_spend'}

    def collection_user_gold_consumption_records(connection, transaction, category, day):

        return connection.execute(text("""
                                            SELECT user_id, sum(currency_amount) AS consumption_amount
                                            FROM bi_user_bill
                                            WHERE currency_type = 'gold'
                                                  AND category_orig = :category
                                                  AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                            GROUP BY  user_id
                                           """), stats_date=day, timezone_offset=timezone_offset, category=category)

    def collection_table_gift_records(connection, transaction, category, day):

        return connection.execute(text("""
                                            SELECT u.from_user                   AS user_name,
                                                (count(u.itemcode))*(t.price)    AS table_gift_spend
                                            FROM prop_user_table_gift u
                                            INNER JOIN prop_table_gift t ON u.itemcode=t.itemcode
                                                  AND DATE(CONVERT_TZ(created_time, '+08:00', :timezone_offset)) = :stats_date
                                            WHERE t.category =3
                                            GROUP BY u.from_user;
                                           """), stats_date=day, timezone_offset=timezone_offset)

    def get_user_consumption_records():

        result_proxy = []
        table_gift_result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")
                print('Consumption history on ' + str(day))
                for category in ['Poker Lucky Charm Set', 'Avatar Set', 'Emoji Set']:
                    category_sales_record = with_db_context(db, collection_user_gold_consumption_records, day=day,
                                                            category=category)

                    every_category_sales_record = [{'stats_date': str(day), 'user_id': row['user_id'],
                                                    column_name[category]: row['consumption_amount']}
                                                   for row in category_sales_record]

                    all_category_sales_records = dict([(category, every_category_sales_record)])
                    result_proxy.append(all_category_sales_records)

                table_gift_records = with_db_context(db, collection_table_gift_records, day=day)

                table_gift_records_rows = [
                    {'stats_date': str(day), 'user_name': row['user_name'], 'table_gift_spend': row['table_gift_spend']}
                    for row in table_gift_records]

                table_gift_result_proxy.append(table_gift_records_rows)
        else:

            for category in ['charms', 'avatar', 'emoji']:
                category_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[category]

                category_sales_record = with_db_context(db, collection_user_consumption_records, day=someday,
                                                        category_orig=category_orig)
                every_category_sales_record = [{'stats_date': str(someday), 'user_id': row['user_id'],
                                                '{}_spend'.format(category): row['consumption_amount']} for
                                               row in category_sales_record]

                all_category_sales_records = dict([(category, every_category_sales_record)])
                result_proxy.append(all_category_sales_records)

            table_gift_records = with_db_context(db, collection_table_gift_records, day=someday, bind='wpt_ods')

            table_gift_records_rows = [
                {'stats_date': str(someday), 'user_name': row['user_name'], 'table_gift_spend': row['table_gift_spend']}
                for row in table_gift_records]

            table_gift_result_proxy.append(table_gift_records_rows)

        return result_proxy, table_gift_result_proxy

    result_proxy, table_gift_result_proxy = get_user_consumption_records()

    if result_proxy:

        for every_category_sales_record_rows in result_proxy:

            for category_name, rows in every_category_sales_record_rows.items():

                if rows:

                    def sync_collection_user_consumption_records(connection, transaction):

                        where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                     BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                        values = {
                            '{}_spend'.format(category_name): bindparam('{}_spend'.format(category_name))
                        }

                        try:
                            connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                        except:

                            print(target + ' Consumption history with gold transaction.rollback()')

                            transaction.rollback()

                            raise

                        else:

                            print(target + ' Consumption history  with gold transaction.commit()')

                            transaction.commit()

                    with_db_context(db, sync_collection_user_consumption_records)

    if table_gift_result_proxy:

        for rows in result_proxy:

            if rows:

                def sync_collection_user_table_gift_consumption_records(connection, transaction):

                    where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                 BIUserStatistic.__table__.c.user_id == bindparam('_user_name'))

                    values = {'table_gift': bindparam('table_gift_spend')}

                    try:
                        connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                    except:
                        print(target + ' table gift consumption history with gold transaction.rollback()')

                        transaction.rollback()

                        raise

                    else:
                        print(target + '  table gift consumption history  with gold transaction.commit()')

                        transaction.commit()

                    with_db_context(db, sync_collection_user_table_gift_consumption_records)


def process_bi_user_statistic_convert_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_user_convert_records(connection, transaction, day, category_orig):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT      user_id,
                                                        sum(currency_amount)       AS  convert_amount
                                            FROM  bi_user_bill
                                            WHERE currency_type = 'gold'
                                            AND   category_orig IN :category_orig
                                            AND     DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                            GROUP BY  user_id
                                           """), stats_date=day, timezone_offset=timezone_offset,
                                      category_orig=tuple(category_orig))

    def get_user_convert_records():

        result_proxy = []

        category = 'silver'

        if target == 'lifetime':

            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")

                print('gold to silver history game on ' + str(day))

                category_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[category]

                category_convert_record = with_db_context(db, collection_user_convert_records, day=day,
                                                          category_orig=category_orig)

                every_category_convert_record_row = [{'_stats_date': str(day), '_user_id': row['user_id'],
                                                      'gold_to_silver': row['convert_amount']}
                                                     for row in category_convert_record]

                all_category_convert_records = dict([(category, every_category_convert_record_row)])

                result_proxy.append(all_category_convert_records)

        else:

            category_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[category]

            category_convert_record = with_db_context(db, collection_user_convert_records, day=someday,
                                                      category_orig=category_orig)

            every_category_convert_record_row = [{'_stats_date': str(someday), '_user_id': row['user_id'],
                                                  'gold_to_silver': row['convert_amount']}
                                                 for row in category_convert_record]

            all_category_convert_records = dict([(category, every_category_convert_record_row)])

            result_proxy.append(all_category_convert_records)

        return result_proxy

    gold_convert_silver_records_result_proxy = get_user_convert_records()

    if gold_convert_silver_records_result_proxy:

        for category_convert_record_rows in gold_convert_silver_records_result_proxy:

            for category_name, rows in category_convert_record_rows.items():

                if rows:

                    def sync_collection_user_convert_records(connection, transaction):

                        where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                     BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                        values = {'gold_to_silver': bindparam('convert_amount')}

                        try:

                            connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

                        except:
                            print(target + ' Convert history  transaction.rollback()')
                            transaction.rollback()
                            raise
                        else:
                            print(target + ' Convert history transaction.commit()')
                            transaction.commit()

                    with_db_context(db, sync_collection_user_convert_records)


def process_bi_user_statistic_free_transaction(target):
    _, someday, index_time, timezone_offset = generate_sql_date(target)

    def collection_user_gold_free_transaction(connection, transaction):

        if target == 'lifetime':

            return connection.execute(text("""
                                            SELECT user_id,
                                                   DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS stats_date,
                                                   SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  transaction_type IN  :gold_free_transaction_types
                                            GROUP  BY stats_date, user_id
                                           """), timezone_offset=timezone_offset,
                                      gold_free_transaction_types=GOLD_FREE_TRANSACTION_TYPES_TUPLE)

        else:

            return connection.execute(text("""
                                            SELECT SUM(transaction_amount)                                  AS sum
                                            FROM   bi_user_currency
                                            WHERE  transaction_type IN  :gold_free_transaction_types
                                            AND    created_at > :index_time
                                            AND    DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset))  = :stats_date
                                            GROUP BY user_id
                                           """), timezone_offset=timezone_offset, stats_date=someday,
                                      index_time=index_time,
                                      gold_free_transaction_types=GOLD_FREE_TRANSACTION_TYPES_TUPLE)

    result_proxy = with_db_context(db, collection_user_gold_free_transaction)

    if target == 'lifetime':

        rows = [{'_stats_date': row['stats_date'], 'sum': row['sum']} for row in result_proxy]

    else:
        rows = [{'_stats_date': someday, 'sum': row['sum']} for row in result_proxy]

    if rows:

        def sync_collection_user_gold_free_transaction(connection, transaction):

            where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                         BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

            values = {'free_gold': bindparam('sum')}

            try:
                connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)

            except:
                print(target + ' user  free gold transaction.rollback()')
                transaction.rollback()
                raise
            else:
                transaction.commit()
                print(target + ' free gold transaction.commit()')

        with_db_context(db, sync_collection_user_gold_free_transaction)
