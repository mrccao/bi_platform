from datetime import date

import pandas as pd
from flask import current_app as app
from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import text

from app.constants import PRODUCT_AND_PRODUCT_ORIG_mapping
from app.extensions import db
from app.models.bi import BIUserStatistic
from app.tasks import with_db_context
from app.utils import current_time


def process_bi_user_statistic_recharge_history(target):
    today = current_time(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')
    timezone_offset = app.config['APP_TIMEZONE']

    def collection_user_recharge_history(connection, transaction,  product_orig, user_id, day):
        if target == 'lifetime':
            return connection.execute(text("""
                                            SELECT  DATE(CONVERT_TZ(created_at, '+00:00', :timezone_offset)) AS on_day,
                                                                                sum(currency_amount) AS consumption_amount,
                                                                                count(*)             AS  purchase_count,
                                                                                sum(quantity)        AS  purchase_quantity
                                            FROM  bi_user_bill
                                            WHERE currency_type = 'Dollar'
                                            AND   product_orig = :product_orig
                                            AND   user_id = :user_id
                                            GROUP BY on_day
                                            HAVING  on_day=:day
                                           """), day=day,
                                      timezone_offset=timezone_offset, product_orig=product_orig,
                                      user_id=user_id)

    def get_user_recharge_history():
        if target == 'lifetime':
            result_proxy = []

            for day in pd.date_range(date(2016, 6, 1), today):
                day = day.strftime("%Y-%m-%d")
                print('Recharge history for every game on ' + str(day))
                for user_id in [6, 8, 19, 25, 28, 30, 31, 32, 35, 36, 41, 61, 678, 800, 801, 2007, 2011, 2015, 2016,
                                2056, 2058, 2060, ]:
                    for product in ['gold', 'silver']:
                        for product_orig in PRODUCT_AND_PRODUCT_ORIG_mapping[product]:
                            every_currency_product_result = with_db_context(db, collection_user_recharge_history,
                                                                            day=day, product_orig=product_orig,
                                                                            user_id=user_id)
                            every_currency_product_rows = [
                                {'_on_day': str(day),
                                 'user_id': user_id,
                                 'purchase_{}_dollar'.format(product): row['consumption_amount'],
                                 'purchase_{}_count'.format(product): row['purchase_count'],
                                 'purchase_{}'.format(product): row['purchase_quantity']}
                                for row in every_currency_product_result]

                            result_proxy.append(every_currency_product_rows)

            return result_proxy

    result_proxy_for_recharge = get_user_recharge_history()

    for rows in result_proxy_for_recharge:

        if rows:
            def sync_collection_user_recharge_history(connection, transaction):

                where = and_(
                    BIUserStatistic.__table__.c.on_day == bindparam('_on_day'),
                    BIUserStatistic.__table__.c.user_id == bindparam(''),
                )
                values = {
                    'purchase_gold_dollar': bindparam('purchase_gold_dollar'),
                    'purchase_gold_count': bindparam('purchase_gold_count'),
                    'purchase_gold': bindparam('purchase_gold'),
                    'user_id': bindparam('user_id')
                }

                try:
                    connection.execute(BIUserStatistic.__table__.update().where(where).values(values), rows)
                except:
                    print('Recharge history for every game transaction.rollback()')
                    transaction.rollback()
                    raise
                else:
                    print('Recharge history for every game transaction.commit()')
                    transaction.commit()

            with_db_context(db, sync_collection_user_recharge_history)
