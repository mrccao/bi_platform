from datetime import date

import pandas as pd
from flask import current_app as app
from sqlalchemy import bindparam
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_MAPPING
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_user_statistic_convert_records(target):
    today = current_time(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')
    yesterday = current_time(app.config['APP_TIMEZONE']).replace(days=-1).format('YYYY-MM-DD')
    timezone_offset = app.config['APP_TIMEZONE']

    def collection_user_convert_records(connection, transaction, day, product_orig):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT                              user_id,
                                                                                sum(currency_amount) AS  convert_amount,
                                                                                count(*)             AS  convert_count,
                                                                                sum(quantity)        AS  convert_quantity
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
                every_product_convert_record_rows = [
                    {'on_day': str(day),
                     'user_id': row['user_id'],
                     'convert_{}_gold'.format(product): row['convert_amount'],
                     'convert_{}_count'.format(product): row['convert_count'],
                     'convert_{}'.format(product): row['convert_quantity']}
                    for row in product_convert_record]

                product_convert_record_rows = dict([(product, every_product_convert_record_rows)])

                result_proxy.append(product_convert_record_rows)

        if target == 'today':
            day = today

            product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
            product_convert_record = with_db_context(db, collection_user_convert_records, day=day,
                                                     product_orig=product_orig)

            if product_convert_record is None:
                return None
            every_product_convert_record_rows = [
                {'on_day': str(day),
                 'user_id': row['user_id'],
                 'convert_{}_gold'.format(product): row['convert_amount'],
                 'convert_{}_count'.format(product): row['Consumption'],
                 'convert_{}'.format(product): row['convert_quantity']}
                for row in product_convert_record]

            product_convert_record_rows = dict([(product, every_product_convert_record_rows)])

            result_proxy.append(product_convert_record_rows)

        if target == 'yesterday':
            day = yesterday

            product_orig = PRODUCT_AND_PRODUCT_ORIG_MAPPING[product]
            product_convert_record = with_db_context(db, collection_user_convert_records, day=day,
                                                     product_orig=product_orig)

            if product_convert_record is None:
                return None
            every_product_convert_record_rows = [
                {'on_day': str(day),
                 'user_id': row['user_id'],
                 'convert_{}_gold'.format(product): row['convert_amount'],
                 'convert_{}_count'.format(product): row['Consumption'],
                 'convert_{}'.format(product): row['convert_quantity']}
                for row in product_convert_record]

            product_convert_record_rows = dict([(product, every_product_convert_record_rows)])

            result_proxy.append(product_convert_record_rows)

        return result_proxy

    result_proxy_for_convert = get_user_convert_records()

    if result_proxy_for_convert:

        for product_convert_record_rows in result_proxy_for_convert:

            for product, rows in product_convert_record_rows.items():

                if rows:

                    def sync_collection_user_convert_records(connection, transaction):

                        values = {
                            'on_day': bindparam('on_day'),
                            'user_id': bindparam('user_id'),
                            'convert_{}_gold'.format(product): bindparam('convert_{}_gold'.format(product)),
                            'convert_{}_count'.format(product): bindparam('convert_{}_count'.format(product)),
                            'convert_{}'.format(product): bindparam('convert_{}'.format(product)),
                        }

                        try:
                            connection.execute(BIUserStatistic.__table__.insert().values(values), rows)
                        except:
                            print('Convert history  transaction.rollback()')
                            transaction.rollback()
                            raise
                        else:
                            print('Convert history transaction.commit()')
                            transaction.commit()

                    with_db_context(db, sync_collection_user_convert_records)
