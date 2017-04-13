from datetime import date

import pandas as pd
from sqlalchemy import bindparam
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_MAPPING
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_recharge_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_user_recharge_records(connection, transaction, product_orig, day):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT      user_id,
                                                        sum(currency_amount) AS  recharge_amount,
                                                        count(*)             AS  recharge_count,
                                                        sum(quantity)        AS  purchase_quantity
                                            FROM  bi_user_bill
                                            WHERE currency_type = 'Dollar'
                                            AND   product_orig IN :product_orig
                                            AND     DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :on_day
                                            GROUP BY  user_id
                                           """), on_day=day, timezone_offset=timezone_offset,
                                      product_orig=tuple(product_orig))

    def get_user_recharge_records():
        result_proxy = []
        if target == 'lifetime':

            for day in pd.date_range(date(2016, 6, 1), today):
                day = day.strftime("%Y-%m-%d")
                print('Recharge history on ' + str(day))
                for product in ['gold', 'silver']:
                    product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
                    product_sales_record = with_db_context(db, collection_user_recharge_records,
                                                           day=day, product_orig=product_orig)
                    every_product_sales_record_rows = [{'on_day': str(day), 'user_id': row['user_id'],
                                                        'purchase_{}_dollar'.format(product): row['recharge_amount'],
                                                        'purchase_{}_count'.format(product): row['recharge_count'],
                                                        'purchase_{}'.format(product): row['purchase_quantity']}
                                                       for row in product_sales_record]

                    all_product_sales_records = dict([(product, every_product_sales_record_rows)])

                    result_proxy.append(all_product_sales_records)
        else:
            for product in ['gold', 'silver']:
                product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
                product_sales_record = with_db_context(db, collection_user_recharge_records, day=someday,
                                                       product_orig=product_orig)
                every_product_sales_record_rows = [{'on_day': str(someday), 'user_id': row['user_id'],
                                                    'purchase_{}_dollar'.format(product): row['recharge_amount'],
                                                    'purchase_{}_count'.format(product): row['Consumption'],
                                                    'purchase_{}'.format(product): row['purchase_quantity']}
                                                   for row in product_sales_record]

                all_product_sales_records = dict([(product, every_product_sales_record_rows)])

                result_proxy.append(all_product_sales_records)

        return result_proxy

    result_proxy_for_recharge = get_user_recharge_records()

    if result_proxy_for_recharge:

        for product_sales_record_rows in result_proxy_for_recharge:

            for product_name, rows in product_sales_record_rows.items():

                if rows:

                    def sync_collection_user_recharge_records(connection, transaction):

                        values = {'on_day': bindparam('on_day'), 'user_id': bindparam('user_id'),
                                  'purchase_{}_dollar'.format(product_name): bindparam(
                                      'purchase_{}_dollar'.format(product_name)),
                                  'purchase_{}_count'.format(product_name): bindparam(
                                      'purchase_{}_count'.format(product_name)),
                                  'purchase_{}'.format(product_name): bindparam('purchase_{}'.format(product_name)),
                                  }

                        try:
                            connection.execute(BIUserStatistic.__table__.insert().values(values), rows)
                        except:
                            print(target + ' Recharge history  transaction.rollback()')
                            transaction.rollback()
                            raise
                        else:
                            print(target + ' Recharge history  transaction.commit()')
                            transaction.commit()

                    with_db_context(db, sync_collection_user_recharge_records)
