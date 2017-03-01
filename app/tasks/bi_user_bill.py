from app.tasks import celery, get_config_value, set_config_value, with_db_context, get_wpt_og_user_mapping
from app.extensions import db
from app.models.bi import BIUser, BIUserCurrency, BIUserBill
from sqlalchemy import text, and_, or_
from sqlalchemy.sql.expression import bindparam


def parse_user_mall_platform(platform, desc):
    if platform == 0:
        if desc is None:
            return 'Undetected'
        return 'iOS'
    if platform == 100:
        if desc is None:
            return 'Undetected'
        return 'iOS'
    if platform == 200:
        return 'Web'
    if platform == 300:
        return 'Facebook Game'
    if platform == 400:
        return 'iOS'
    if platform == 500:
        return 'Android'


def parse_user_mall_platform_from_reg_source(reg_source):
    if reg_source.startswith('Web'):
        return 'Web'
    if reg_source.startswith('iOS'):
        return 'iOS'
    if reg_source.startswith('Android'):
        return 'Android'
    if reg_source.startswith('Facebook Game'):
        return 'Facebook Game'
    return 'Unknown'


def process_user_mall_bill_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_mall_bill_order_id')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT o.OrderId,
                                                  o.UserId,
                                                  o.GameId,
                                                  o.PlatformId,
                                                  o.Descr,
                                                  c.CurrencyCode,
                                                  c.CurrencyName,
                                                  o.TotalPrice,
                                                  p.ParentId,
                                                  g.GoodsName,
                                                  p.ProductName,
                                                  op.GoodsId,
                                                  op.ProductId,
                                                  op.Quantity,
                                                  o.CDate,
                                                  o.UDate,
                                                  (CASE WHEN p.ParentId IS NULL THEN p.ProductName ELSE (SELECT ProductName FROM Mall_tProduct WHERE Id = p.ParentId) END) AS Category
                                           FROM   Mall_tOrder o
                                                  LEFT JOIN Mall_tOrderProductLog op
                                                         ON op.OrderId = o.OrderId
                                                  LEFT JOIN Mall_tProduct p
                                                         ON op.ProductId = p.Id
                                                  LEFT JOIN Mall_tCurrency c
                                                         ON o.CurrencyCode = c.CurrencyCode
                                                  LEFT JOIN Mall_tPlatform s
                                                         ON s.PlatformId = o.PlatformId
                                                  LEFT JOIN Mall_tGoods g
                                                         ON g.Id = op.GoodsId
                                           WHERE  o.OrderStatus != 1
                                                  AND o.OrderStatus != 41
                                                  AND o.PaymentMode = 1
                                           ORDER BY o.OrderId ASC
                                           """))
        return connection.execute(text("""
                                       SELECT o.OrderId,
                                              o.UserId,
                                              o.GameId,
                                              o.PlatformId,
                                              o.Descr,
                                              c.CurrencyCode,
                                              c.CurrencyName,
                                              o.TotalPrice,
                                              p.ParentId,
                                              g.GoodsName,
                                              p.ProductName,
                                              op.GoodsId,
                                              op.ProductId,
                                              op.Quantity,
                                              o.CDate,
                                              o.UDate,
                                              (CASE WHEN p.ParentId IS NULL THEN p.ProductName ELSE (SELECT ProductName FROM Mall_tProduct WHERE Id = p.ParentId) END) AS Category
                                       FROM   Mall_tOrder o
                                              LEFT JOIN Mall_tOrderProductLog op
                                                     ON op.OrderId = o.OrderId
                                              LEFT JOIN Mall_tProduct p
                                                     ON op.ProductId = p.Id
                                              LEFT JOIN Mall_tCurrency c
                                                     ON o.CurrencyCode = c.CurrencyCode
                                              LEFT JOIN Mall_tPlatform s
                                                         ON s.PlatformId = o.PlatformId
                                              LEFT JOIN Mall_tGoods g
                                                         ON g.Id = op.GoodsId
                                       WHERE  o.OrderStatus != 1
                                              AND o.OrderStatus != 41
                                              AND o.PaymentMode = 1
                                              AND o.OrderId > :order_id
                                       ORDER BY o.OrderId ASC
                                       """), order_id=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_mall')

    #########

    orig_result_proxy = []
    for row in result_proxy:
        orig_result_proxy.append(row)

    def existed_collection(connection, transaction):
        return connection.execute(text("SELECT orig_id FROM bi_user_bill WHERE orig_db = 'WPT_MALL'"))
    existed_result_proxy = with_db_context(db, existed_collection)
    existed_data = [row['orig_id'] for row in existed_result_proxy]

    #########

    rows = [{
        'orig_id': row['OrderId'],
        'orig_db': 'WPT_MALL',
        'user_id': row['UserId'],
        'game_id': row['GameId'],
        'platform': parse_user_mall_platform(row['PlatformId'], row['Descr']),
        'platform_orig': row['PlatformId'],
        'currency_type': row['CurrencyName'],
        'currency_type_orig': row['CurrencyCode'],
        'currency_amount': row['TotalPrice'],
        'category': row['Category'],
        'category_orig': row['ParentId'],
        'product': row['ProductName'],
        'product_orig': row['ProductId'],
        'goods': row['GoodsName'],
        'goods_orig': row['GoodsId'],
        'quantity': row['Quantity'],
        'created_at': row['CDate']
    } for row in orig_result_proxy if row['OrderId'] not in existed_data]

    if rows:
        new_config_value = rows[-1]['orig_id']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(BIUserBill.__table__.insert(), rows)
                set_config_value(connection, 'last_imported_user_mall_bill_order_id', new_config_value)
            except:
                print('process_user_mall_bill_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_mall_bill_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_mall_bill_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_user_mall_bill_order_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT o.OrderId,
                                                  o.UserId,
                                                  o.GameId,
                                                  o.PlatformId,
                                                  o.Descr,
                                                  c.CurrencyCode,
                                                  c.CurrencyName,
                                                  o.TotalPrice,
                                                  p.ParentId,
                                                  g.GoodsName,
                                                  p.ProductName,
                                                  op.GoodsId,
                                                  op.ProductId,
                                                  op.Quantity,
                                                  o.CDate,
                                                  o.UDate,
                                                  (CASE WHEN p.ParentId IS NULL THEN p.ProductName ELSE (SELECT ProductName FROM Mall_tProduct WHERE Id = p.ParentId) END) AS Category
                                           FROM   Mall_tOrder o
                                                  LEFT JOIN Mall_tOrderProductLog op
                                                         ON op.OrderId = o.OrderId
                                                  LEFT JOIN Mall_tProduct p
                                                         ON op.ProductId = p.Id
                                                  LEFT JOIN Mall_tCurrency c
                                                         ON o.CurrencyCode = c.CurrencyCode
                                                  LEFT JOIN Mall_tPlatform s
                                                         ON s.PlatformId = o.PlatformId
                                                  LEFT JOIN Mall_tGoods g
                                                         ON g.Id = op.GoodsId
                                           WHERE  o.OrderStatus != 1
                                                  AND o.OrderStatus != 41
                                                  AND o.PaymentMode = 1
                                           ORDER BY o.UDate ASC
                                           """))
        return connection.execute(text("""
                                       SELECT o.OrderId,
                                              o.UserId,
                                              o.GameId,
                                              o.PlatformId,
                                              o.Descr,
                                              c.CurrencyCode,
                                              c.CurrencyName,
                                              o.TotalPrice,
                                              p.ParentId,
                                              g.GoodsName,
                                              p.ProductName,
                                              op.GoodsId,
                                              op.ProductId,
                                              op.Quantity,
                                              o.CDate,
                                              o.UDate,
                                              (CASE WHEN p.ParentId IS NULL THEN p.ProductName ELSE (SELECT ProductName FROM Mall_tProduct WHERE Id = p.ParentId) END) AS Category
                                       FROM   Mall_tOrder o
                                              LEFT JOIN Mall_tOrderProductLog op
                                                     ON op.OrderId = o.OrderId
                                              LEFT JOIN Mall_tProduct p
                                                     ON op.ProductId = p.Id
                                              LEFT JOIN Mall_tCurrency c
                                                     ON o.CurrencyCode = c.CurrencyCode
                                              LEFT JOIN Mall_tPlatform s
                                                         ON s.PlatformId = o.PlatformId
                                              LEFT JOIN Mall_tGoods g
                                                         ON g.Id = op.GoodsId
                                       WHERE  o.OrderStatus != 1
                                              AND o.OrderStatus != 41
                                              AND o.PaymentMode = 1
                                              AND o.UDate IS NOT NULL AND o.UDate >= :update_time
                                       ORDER BY o.UDate ASC
                                       """), update_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_mall')

    #########

    orig_result_proxy = []
    for row in result_proxy:
        orig_result_proxy.append(row)

    def existed_collection(connection, transaction):
        return connection.execute(text("SELECT orig_id FROM bi_user_bill WHERE orig_db = 'WPT_MALL'"))
    existed_result_proxy = with_db_context(db, existed_collection)
    existed_data = [row['orig_id'] for row in existed_result_proxy]

    #########

    rows = [{
        'orig_id': row['OrderId'],
        'orig_db': 'WPT_MALL',
        'user_id': row['UserId'],
        'game_id': row['GameId'],
        'platform': parse_user_mall_platform(row['PlatformId'], row['Descr']),
        'platform_orig': row['PlatformId'],
        'currency_type': row['CurrencyName'],
        'currency_type_orig': row['CurrencyCode'],
        'currency_amount': row['TotalPrice'],
        'category': row['Category'],
        'category_orig': row['ParentId'],
        'product': row['ProductName'],
        'product_orig': row['ProductId'],
        'goods': row['GoodsName'],
        'goods_orig': row['GoodsId'],
        'quantity': row['Quantity'],
        'created_at': row['CDate'],
        'updated_at': row['UDate']
    } for row in orig_result_proxy if row['OrderId'] not in existed_data]

    if rows:
        new_config_value = rows[-1]['updated_at']

        for row in rows: del row['updated_at']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(BIUserBill.__table__.insert(), rows)
                set_config_value(connection, 'last_imported_user_mall_bill_order_update_time', new_config_value)
            except:
                print('process_user_mall_bill_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_mall_bill_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


# def process_user_payment_bill_newly_added_records():
#     config_value = get_config_value(db, 'last_imported_user_payment_bill_order_id')

#     def collection(connection, transaction):
#         """ Get newly added. """
#         if config_value is None:
#             return connection.execute(text("""
#                                            SELECT user_paylog_id, u_id, order_code, createtime, user_ip,
#                                            Round(order_price / 100, 2) as total_price
#                                            FROM   user_paylog
#                                            WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
#                                            ORDER  BY user_paylog_id ASC
#                                            """))
#         return connection.execute(text("""
#                                        SELECT user_paylog_id, u_id, order_code, createtime, user_ip,
#                                        Round(order_price / 100, 2) as total_price
#                                        FROM   user_paylog
#                                        WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3 AND user_paylog_id > :paylog_id
#                                        ORDER  BY user_paylog_id ASC
#                                        """), paylog_id=config_value)

#     result_proxy = with_db_context(db, collection, 'orig_wpt_payment')

#     rows = [{
#         'orig_id': row['user_paylog_id'],
#         'orig_db': 'WPT_PAYMENT',
#         'user_id': row['u_id'],
#         'game_id': 25011,
#         'platform': 'Web',
#         'platform_orig': 200,
#         'currency_type': 'Dollar',
#         'currency_type_orig': 201,
#         'currency_amount': row['total_price'],
#         'category': 'Lucky Spin Set',
#         'category_orig': 3,
#         'product': 'Lucky Spin($0.99)',
#         'product_orig': -1,
#         'goods': 'Lucky Spin($0.99)',
#         'goods_orig': -1,
#         'quantity': 1,
#         'created_at': row['createtime']
#     } for row in result_proxy]

#     if rows:
#         new_config_value = rows[-1]['orig_id']

#         def sync_collection(connection, transaction):
#             """ Sync newly added. """
#             try:
#                 connection.execute(BIUserBill.__table__.insert(), rows)
#                 set_config_value(connection, 'last_imported_user_payment_bill_order_id', new_config_value)
#             except:
#                 print('process_user_payment_bill_newly_added_records transaction.rollback()')
#                 transaction.rollback()
#                 raise
#             else:
#                 print('process_user_payment_bill_newly_added_records transaction.commit()')
#                 transaction.commit()
#             return

#         with_db_context(db, sync_collection)

#     return


def process_user_payment_bill_newly_added_records():

    def collection(connection, transaction):
        return connection.execute(text("""
                                       SELECT user_paylog_id, u_id, order_code, createtime, user_ip,
                                       Round(order_price / 100, 2) as total_price
                                       FROM   user_paylog
                                       WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
                                       """))

    result_proxy = with_db_context(db, collection, 'orig_wpt_payment')

    #########

    orig_result_proxy = []
    for row in result_proxy:
        orig_result_proxy.append(row)

    def existed_collection(connection, transaction):
        return connection.execute(text("SELECT orig_id FROM bi_user_bill WHERE orig_db = 'WPT_PAYMENT'"))
    existed_result_proxy = with_db_context(db, existed_collection)
    existed_data = [row['orig_id'] for row in existed_result_proxy]

    #########

    rows = [{
        'orig_id': row['user_paylog_id'],
        'orig_db': 'WPT_PAYMENT',
        'user_id': row['u_id'],
        'game_id': 25011,
        'platform': 'Web',
        'platform_orig': 200,
        'currency_type': 'Dollar',
        'currency_type_orig': 201,
        'currency_amount': row['total_price'],
        'category': 'Lucky Spin Set',
        'category_orig': 3,
        'product': 'Lucky Spin($0.99)',
        'product_orig': -1,
        'goods': 'Lucky Spin($0.99)',
        'goods_orig': -1,
        'quantity': 1,
        'created_at': row['createtime']
    } for row in orig_result_proxy if str(row['user_paylog_id']) not in existed_data]

    if rows:

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(BIUserBill.__table__.insert(), rows)
            except:
                print('process_user_payment_bill_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_payment_bill_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_mall_bill_fix_platform():
    def collection(connection, transaction):
        return connection.execute(text("SELECT user_id, reg_source FROM bi_user WHERE user_id IN (SELECT DISTINCT user_id FROM bi_user_bill WHERE platform = 'Undetected')"))

    result_proxy = with_db_context(db, collection)

    rows = [{
        '_user_id': row['user_id'],
        'platform': parse_user_mall_platform_from_reg_source(row['reg_source'])
    } for row in result_proxy]

    if rows:

        def sync_collection(connection, transaction):
            where = and_(
                BIUserBill.__table__.c.user_id == bindparam('_user_id'),
                BIUserBill.__table__.c.platform == 'Undetected'
                )
            values = {
                'platform': bindparam('platform')
                }

            try:
                connection.execute(BIUserBill.__table__.update().where(where).values(values), rows)
            except:
                print('process_user_mall_bill_fix_platform transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_mall_bill_fix_platform transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_dollar_paid_total():
    config_value = get_config_value(db, 'last_imported_user_bill_dollar_paid_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT user_id,
                                                  COUNT(*) AS dollar_paid_count,
                                                  ROUND(SUM(currency_amount), 2) AS dollar_paid_amount,
                                                  MAX(created_at) AS max_createtime
                                           FROM bi_user_bill
                                           WHERE currency_type = 'Dollar'
                                           GROUP BY user_id
                                           ORDER BY max_createtime ASC
                                           """))
        return connection.execute(text("""
                                       SELECT user_id,
                                              COUNT(*) AS dollar_paid_count,
                                              ROUND(SUM(currency_amount), 2) AS dollar_paid_amount,
                                              MAX(created_at) AS max_createtime
                                       FROM bi_user_bill
                                       WHERE currency_type = 'Dollar'
                                             AND user_id IN (SELECT DISTINCT user_id
                                                             FROM   bi_user_bill
                                                             WHERE  created_at >= :add_time)
                                       GROUP BY user_id
                                       ORDER BY max_createtime ASC
                                       """), add_time=config_value)

    result_proxy = with_db_context(db, collection)

    rows = [{
        '_user_id': row['user_id'],
        'dollar_paid_count': row['dollar_paid_count'],
        'dollar_paid_amount': row['dollar_paid_amount'],
        'max_createtime': row['max_createtime']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_createtime']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'dollar_paid_count': bindparam('dollar_paid_count'),
                'dollar_paid_amount': bindparam('dollar_paid_amount'),
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_bill_dollar_paid_add_time', new_config_value)
            except:
                print('process_user_dollar_paid_total transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_dollar_paid_total transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


@celery.task
def process_bi_user_bill():

    process_user_mall_bill_newly_added_records()
    print('process_user_mall_bill_newly_added_records() done.')

    process_user_mall_bill_newly_updated_records()
    print('process_user_mall_bill_newly_updated_records() done.')

    process_user_payment_bill_newly_added_records()
    print('process_user_payment_bill_newly_added_records() done.')

    process_user_mall_bill_fix_platform()
    print('process_user_mall_bill_fix_platform() done.')

    process_user_dollar_paid_total()
    print('process_user_dollar_paid_total() done.')
