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

    def collection_user_consumption_records(connection, transaction, product_orig, day):

        return connection.execute(text("""
                                            SELECT user_id, sum(currency_amount) AS consumption_amount
                                            FROM bi_user_bill
                                            WHERE currency_type = 'gold'
                                                  AND product_orig IN :product_orig
                                                  AND DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                            GROUP BY user_id
                                           """), stats_date=day, timezone_offset=timezone_offset,
                                  product_orig=tuple(product_orig))

    def collection_table_gift_records(connection, transaction, product_orig, day):

        return connection.execute(text("""
                                            SELECT u.from_user                   AS user_name,
                                                (count(u.itemcode))*(t.price)    AS table_gift_spend
                                            FROM prop_user_table_gift u
                                            INNER JOIN prop_table_gift t ON u.itemcode=t.itemcode
                                                  AND DATE(CONVERT_TZ(created_time, '+00:00', :timezone_offset)) = :stats_date
                                            WHERE t.product =3
                                            GROUP BY u.from_user;
                                           """), stats_date=day, timezone_offset=timezone_offset,
                                  product_orig=tuple(product_orig))

    def get_user_consumption_records():

        result_proxy = []
        table_gift_result_proxy = []

        if target == 'lifetime':
            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")
                print('Consumption history on ' + str(day))
                for product in ['charms', 'avatar', 'emoji']:
                    product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]

                    product_sales_record = with_db_context(db, collection_user_consumption_records, day=day,
                                                           product_orig=product_orig)
                    every_product_sales_record = [{'stats_date': str(day), 'user_id': row['user_id'],
                                                   '{}_spend'.format(product): row['consumption_amount']}
                                                  for row in product_sales_record]

                    all_product_sales_records = dict([(product, every_product_sales_record)])
                    result_proxy.append(all_product_sales_records)

                table_gift_records = with_db_context(db, collection_table_gift_records, day=day)

                table_gift_records_rows = [
                    {'stats_date': str(day), 'user_name': row['user_name'], 'table_gift_spend': row['table_gift_spend']}
                    for row in table_gift_records]

                table_gift_result_proxy.append(table_gift_records_rows)
        else:

            for product in ['charms', 'avatar', 'emoji']:
                product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]

                product_sales_record = with_db_context(db, collection_user_consumption_records, day=someday,
                                                       product_orig=product_orig)
                every_product_sales_record = [{'stats_date': str(someday), 'user_id': row['user_id'],
                                               '{}_spend'.format(product): row['consumption_amount']} for
                                              row in product_sales_record]

                all_product_sales_records = dict([(product, every_product_sales_record)])
                result_proxy.append(all_product_sales_records)

            table_gift_records = with_db_context(db, collection_table_gift_records, day=someday, bind='wpt_ods')

            table_gift_records_rows = [
                {'stats_date': str(someday), 'user_name': row['user_name'], 'table_gift_spend': row['table_gift_spend']}
                for row in table_gift_records]

            table_gift_result_proxy.append(table_gift_records_rows)

        return result_proxy, table_gift_result_proxy

    result_proxy, table_gift_result_proxy = get_user_consumption_records()

    if result_proxy:

        for every_product_sales_record_rows in result_proxy:

            for product_name, rows in every_product_sales_record_rows.items():

                if rows:

                    def sync_collection_user_consumption_records(connection, transaction):

                        where = and_(BIUserStatistic.__table__.c.stats_date == bindparam('_stats_date'),
                                     BIUserStatistic.__table__.c.user_id == bindparam('_user_id'))

                        values = {
                            '{}_spend'.format(product_name): bindparam('{}_spend'.format(product_name))
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

    def collection_user_convert_records(connection, transaction, day, product_orig):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT      user_id,
                                                        sum(currency_amount)       AS  convert_amount
                                            FROM  bi_user_bill
                                            WHERE currency_type = 'gold'
                                            AND   product_orig IN :product_orig
                                            AND     DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :stats_date
                                            GROUP BY  user_id
                                           """), stats_date=day, timezone_offset=timezone_offset,
                                      product_orig=tuple(product_orig))

    def get_user_convert_records():

        result_proxy = []

        product = 'silver'

        if target == 'lifetime':

            date_range_reversed = sorted(pd.date_range(date(2016, 6, 1), today), reverse=True)

            for day in date_range_reversed:
                day = day.strftime("%Y-%m-%d")

                print('gold to silver history game on ' + str(day))

                product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]

                product_convert_record = with_db_context(db, collection_user_convert_records, day=day,
                                                         product_orig=product_orig)

                every_product_convert_record_row = [{'_stats_date': str(day), '_user_id': row['user_id'],
                                                     'gold_to_silver': row['convert_amount']}
                                                    for row in product_convert_record]

                all_product_convert_records = dict([(product, every_product_convert_record_row)])

                result_proxy.append(all_product_convert_records)

        else:

            product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]

            product_convert_record = with_db_context(db, collection_user_convert_records, day=someday,
                                                     product_orig=product_orig)

            every_product_convert_record_row = [{'_stats_date': str(someday), '_user_id': row['user_id'],
                                                 'gold_to_silver': row['convert_amount']}
                                                for row in product_convert_record]

            all_product_convert_records = dict([(product, every_product_convert_record_row)])

            result_proxy.append(all_product_convert_records)

        return result_proxy

    gold_convert_silver_records_result_proxy = get_user_convert_records()

    if gold_convert_silver_records_result_proxy:

        for product_convert_record_rows in gold_convert_silver_records_result_proxy:

            for product_name, rows in product_convert_record_rows.items():

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
