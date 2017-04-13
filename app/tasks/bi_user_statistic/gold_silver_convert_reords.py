from datetime import date

import pandas as pd
from sqlalchemy import bindparam
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_MAPPING
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import generate_sql_date


def process_bi_user_statistic_convert_records(target):
    today, someday, _, timezone_offset = generate_sql_date(target)

    def collection_user_convert_records(connection, transaction, day, product_orig):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT      user_id,
                                                        sum(currency_amount)       AS  convert_amount,
                                                        count(*)                   AS  convert_count,
                                                        sum(quantity)              AS  convert_quantity
                                            FROM  bi_user_bill
                                            WHERE currency_type = 'gold'
                                            AND   product_orig IN :product_orig
                                            AND     DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) = :on_day
                                            GROUP BY  user_id
                                           """), on_day=day, timezone_offset=timezone_offset,
                                      product_orig=tuple(product_orig))

    def get_user_convert_records():
        result_proxy = []
        product = 'silver'
        if target == 'lifetime':

            for day in pd.date_range(date(2016, 4, 22), today):
                day = day.strftime("%Y-%m-%d")
                print('Recharge history game on ' + str(day))
                product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
                product_convert_record = with_db_context(db, collection_user_convert_records, day=day,
                                                         product_orig=product_orig)
                every_product_convert_record_row = [{'on_day': str(day), 'user_id': row['user_id'],
                                                     'convert_{}_gold'.format(product): row['convert_amount'],
                                                     'convert_{}_count'.format(product): row['convert_count'],
                                                     'convert_{}'.format(product): row['convert_quantity']}
                                                    for row in product_convert_record]
                all_product_convert_records = dict([(product, every_product_convert_record_row)])

                result_proxy.append(all_product_convert_records)

        else:
            product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
            product_convert_record = with_db_context(db, collection_user_convert_records, day=someday,
                                                     product_orig=product_orig)

            every_product_convert_record_row = [{'on_day': str(someday), 'user_id': row['user_id'],
                                                 'convert_{}_gold'.format(product): row['convert_amount'],
                                                 'convert_{}_count'.format(product): row['Consumption'],
                                                 'convert_{}'.format(product): row['convert_quantity']}
                                                for row in product_convert_record]

            all_product_convert_records = dict([(product, every_product_convert_record_row)])

            result_proxy.append(all_product_convert_records)

        return result_proxy

    result_proxy_for_convert = get_user_convert_records()

    if result_proxy_for_convert:

        for product_convert_record_rows in result_proxy_for_convert:

            for product_name, rows in product_convert_record_rows.items():

                if rows:

                    def sync_collection_user_convert_records(connection, transaction):

                        values = {'on_day': bindparam('on_day'), 'user_id': bindparam('user_id'),
                                  'convert_{}_gold'.format(product_name): bindparam(
                                      'convert_{}_gold'.format(product_name)),
                                  'convert_{}_count'.format(product_name): bindparam(
                                      'convert_{}_count'.format(product_name)),
                                  'convert_{}'.format(product_name): bindparam('convert_{}'.format(product_name)), }

                        try:
                            connection.execute(BIUserStatistic.__table__.insert().values(values), rows)
                        except:
                            print(target + ' Convert history  transaction.rollback()')
                            transaction.rollback()
                            raise
                        else:
                            print(target + ' Convert history transaction.commit()')
                            transaction.commit()

                    with_db_context(db, sync_collection_user_convert_records)
