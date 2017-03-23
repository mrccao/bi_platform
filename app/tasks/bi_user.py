import arrow
from sqlalchemy import text, and_
from sqlalchemy.sql.expression import bindparam
from flask import current_app as app

from app.extensions import db
from app.models.bi import BIUser
from app.tasks import celery, get_config_value, set_config_value, with_db_context


def parse_user_source(reg_device, p_id, u_type):
    if reg_device == 1 or reg_device == 10: # PlayWPT
        if p_id == 100:
            if u_type == 1:
                return 'Web Facebook'
            if u_type == 2:
                return 'Facebook Game'

        if p_id == 0:
            return 'Web'

    if reg_device == 2: # PlayWPT Mobile
        if p_id == 100:
            return 'Web Mobile Facebook'

        if p_id == 0:
            return 'Web Mobile'

    if reg_device == 5: # iOS
        if p_id == 100:
            return 'iOS Facebook'

        if p_id == 0:
            return 'iOS'

    if reg_device == 6: # Android
        if p_id == 100:
            return 'Android Facebook'

        if p_id == 0:
            return 'Android'

    return 'Unknown'


def parse_user_platform(reg_device, p_id, u_type):
    if reg_device == 1 or reg_device == 10: # PlayWPT
        if p_id == 100:
            if u_type == 1:
                return 'Web'
            if u_type == 2:
                return 'Facebook Game'

        if p_id == 0:
            return 'Web'

    if reg_device == 2: # PlayWPT Mobile
        return 'Web Mobile'

    if reg_device == 5: # iOS
        return 'iOS'

    if reg_device == 6: # Android
        return 'Android'

    return 'Unknown'


def parse_user_facebook_connect(p_id):
    if p_id == 100:
        return True

    if p_id == 0:
        return False

    return None


def parse_user_account_status(status):
    if status == 10:
        return 'Email validated'
    if status == 2:
        return 'Waiting for email validation'
    if status == 3:
        return 'Email validation timeout'
    if status == 4:
        return 'Locked'
    if status == 5:
        return 'Banned'

    return 'Unknown'


def parse_user_gender(gender):
    if gender == 0:
        return 'Confidential'
    if gender == 1:
        return 'Male'
    if gender == 2:
        return 'Female'

    return 'Unknown'


def parse_user_billing_info_contact(contact):
    if contact is not None:
        return ' '.join([x.strip().capitalize() for x in contact.split('-') if x.strip()])
    return None


def process_user_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_id')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT u.*,
                                                  ( CASE
                                                      WHEN u.u_email IS NOT NULL THEN u.u_email
                                                      WHEN pu.pu_email IS NOT NULL THEN pu.pu_email
                                                      ELSE NULL
                                                    end )           AS final_email,
                                                  aff.email         AS reg_affiliate,
                                                  pu.pu_id          AS facebook_id,
                                                  com.compaign_desc AS reg_compaign
                                           FROM   tb_user_base u
                                                  LEFT JOIN tb_platform_info p
                                                         ON u.p_id = p.p_id
                                                  LEFT JOIN tb_platform_user_info pu
                                                         ON u.u_id = pu.u_id
                                                  LEFT JOIN tb_compaign com
                                                         ON u.compaign_pk = com.compaign_pk
                                                  LEFT JOIN tb_affiliate aff
                                                         ON u.affiliate_pk = aff.affiliate_pk
                                           ORDER  BY u.u_id ASC
                                           """))
        return connection.execute(text("""
                                       SELECT u.*,
                                              ( CASE
                                                  WHEN u.u_email IS NOT NULL THEN u.u_email
                                                  WHEN pu.pu_email IS NOT NULL THEN pu.pu_email
                                                  ELSE NULL
                                                end )             AS final_email,
                                                aff.email         AS reg_affiliate,
                                                pu.pu_id          AS facebook_id,
                                                com.compaign_desc AS reg_compaign
                                       FROM   tb_user_base u
                                              LEFT JOIN tb_platform_info p
                                                     ON u.p_id = p.p_id
                                              LEFT JOIN tb_platform_user_info pu
                                                     ON u.u_id = pu.u_id
                                              LEFT JOIN tb_compaign com
                                                     ON u.compaign_pk = com.compaign_pk
                                              LEFT JOIN tb_affiliate aff
                                                     ON u.affiliate_pk = aff.affiliate_pk
                                       WHERE  u.u_id > :user_id
                                       ORDER  BY u.u_id ASC
                                       """), user_id=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        'user_id': row['u_id'],
        'email': row['final_email'],
        'facebook_id': row['facebook_id'],
        'email_validate_time': row['email_validate_time'],
        'email_promotion_allowed': row['is_promotion'],
        'reg_ip': row['reg_ip'],
        'reg_time': row['reg_time'],
        'reg_source': parse_user_source(row['reg_device'], row['p_id'], row['u_type']),
        'reg_platform': parse_user_platform(row['reg_device'], row['p_id'], row['u_type']),
        'reg_facebook_connect': parse_user_facebook_connect(row['p_id']),
        'reg_type_orig': row['u_type'],
        'reg_platform_orig': row['p_id'],
        'reg_device_orig': row['reg_device'],
        'reg_affiliate': row['reg_affiliate'],
        'reg_affiliate_orig': row['affiliate_pk'],
        'reg_campaign': row['reg_compaign'],
        'reg_campaign_orig': row['compaign_pk'],
        'account_status': parse_user_account_status(row['u_status']),
        'account_status_orig': row['u_status']
    } for row in result_proxy]

    print('process_user_newly_added_records new data: ' + str(len(rows)))

    if rows:
        new_config_value = rows[-1]['user_id']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            try:
                connection.execute(BIUser.__table__.insert(), rows)
                set_config_value(connection, 'last_imported_user_id', new_config_value)
            except:
                print('process_user_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_user_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT u.*,
                                                  ( CASE
                                                      WHEN u.u_email IS NOT NULL THEN u.u_email
                                                      WHEN pu.pu_email IS NOT NULL THEN pu.pu_email
                                                      ELSE NULL
                                                    end )           AS final_email,
                                                  aff.email         AS reg_affiliate,
                                                  com.compaign_desc AS reg_compaign
                                           FROM   tb_user_base u
                                                  LEFT JOIN tb_platform_info p
                                                         ON u.p_id = p.p_id
                                                  LEFT JOIN tb_platform_user_info pu
                                                         ON u.u_id = pu.u_id
                                                  LEFT JOIN tb_compaign com
                                                         ON u.compaign_pk = com.compaign_pk
                                                  LEFT JOIN tb_affiliate aff
                                                         ON u.affiliate_pk = aff.affiliate_pk
                                           WHERE  u.update_time IS NOT NULL
                                           ORDER  BY u.u_id ASC
                                           """))
        return connection.execute(text("""
                                       SELECT u.*,
                                              ( CASE
                                                  WHEN u.u_email IS NOT NULL THEN u.u_email
                                                  WHEN pu.pu_email IS NOT NULL THEN pu.pu_email
                                                  ELSE NULL
                                                end )             AS final_email,
                                                aff.email         AS reg_affiliate,
                                                com.compaign_desc AS reg_compaign
                                       FROM   tb_user_base u
                                              LEFT JOIN tb_platform_info p
                                                     ON u.p_id = p.p_id
                                              LEFT JOIN tb_platform_user_info pu
                                                     ON u.u_id = pu.u_id
                                              LEFT JOIN tb_compaign com
                                                     ON u.compaign_pk = com.compaign_pk
                                              LEFT JOIN tb_affiliate aff
                                                     ON u.affiliate_pk = aff.affiliate_pk
                                       WHERE  u.update_time >= :update_time
                                       ORDER  BY u.update_time ASC
                                       """), update_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_user_id': row['u_id'],
        'email': row['final_email'],
        'email_validate_time': row['email_validate_time'],
        'email_promotion_allowed': row['is_promotion'],
        'account_status': parse_user_account_status(row['u_status']),
        'account_status_orig': row['u_status'],
        'update_time': row['update_time']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['update_time']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'email': bindparam('email'),
                'email_validate_time': bindparam('email_validate_time'),
                'email_promotion_allowed': bindparam('email_promotion_allowed'),
                'account_status': bindparam('account_status'),
                'account_status_orig': bindparam('account_status_orig')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_update_time', new_config_value)
            except:
                print('process_user_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_info_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_info_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text('SELECT * FROM tb_user_info ORDER BY add_time ASC'))
        return connection.execute(text('SELECT * FROM tb_user_info WHERE add_time >= :add_time ORDER BY add_time ASC'), add_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_user_id': row['u_id'],
        'gender': parse_user_gender(row['u_sex']),
        'gender_orig': row['u_sex'],
        'first_name': row['name_first'],
        'middle_name': row['name_middle'],
        'last_name': row['name_last'],
        'address': row['addr_detail'],
        'city': row['addr_city'],
        'state': row['addr_state'],
        'zip_code': row['addr_post_code'],
        'country': row['addr_country'],
        'phone': row['u_phone'],
        'birthday': row['u_birth'],
        'add_time': row['add_time'],
        'update_time': row['update_time']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['add_time']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'gender': bindparam('gender'),
                'gender_orig': bindparam('gender_orig'),
                'first_name': bindparam('first_name'),
                'middle_name': bindparam('middle_name'),
                'last_name': bindparam('last_name'),
                'address': bindparam('address'),
                'city': bindparam('city'),
                'state': bindparam('state'),
                'zip_code': bindparam('zip_code'),
                'country': bindparam('country'),
                'phone': bindparam('phone'),
                'birthday': bindparam('birthday')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_info_add_time', new_config_value)
            except:
                print('process_user_info_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_info_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_info_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_user_info_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text('SELECT * FROM tb_user_info WHERE update_time IS NOT NULL ORDER BY update_time ASC'))
        return connection.execute(text('SELECT * FROM tb_user_info WHERE update_time >= :update_time ORDER BY update_time ASC'), update_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_user_id': row['u_id'],
        'gender': parse_user_gender(row['u_sex']),
        'gender_orig': row['u_sex'],
        'first_name': row['name_first'],
        'middle_name': row['name_middle'],
        'last_name': row['name_last'],
        'address': row['addr_detail'],
        'city': row['addr_city'],
        'state': row['addr_state'],
        'zip_code': row['addr_post_code'],
        'country': row['addr_country'],
        'phone': row['u_phone'],
        'birthday': row['u_birth'],
        'add_time': row['add_time'],
        'update_time': row['update_time']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['update_time']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'gender': bindparam('gender'),
                'gender_orig': bindparam('gender_orig'),
                'first_name': bindparam('first_name'),
                'middle_name': bindparam('middle_name'),
                'last_name': bindparam('last_name'),
                'address': bindparam('address'),
                'city': bindparam('city'),
                'state': bindparam('state'),
                'zip_code': bindparam('zip_code'),
                'country': bindparam('country'),
                'phone': bindparam('phone'),
                'birthday': bindparam('birthday')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_info_update_time', new_config_value)
            except:
                print('process_user_info_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_info_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_billing_info_added_records():
    config_value = get_config_value(db, 'last_imported_user_billing_info_id')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT t1.*
                                           FROM   billing_info t1
                                                  INNER JOIN (SELECT u_id,
                                                                     Max(billing_info_id) max_id
                                                              FROM   billing_info
                                                              WHERE  billing_info_state = 1
                                                              GROUP  BY u_id) t2
                                                          ON t1.u_id = t2.u_id
                                                             AND t1.billing_info_id = t2.max_id
                                           ORDER  BY t1.billing_info_id ASC
                                           """))
        return connection.execute(text("""
                                       SELECT t1.*
                                       FROM   billing_info t1
                                              INNER JOIN (SELECT u_id,
                                                                 Max(billing_info_id) max_id
                                                          FROM   billing_info
                                                          WHERE  billing_info_state = 1
                                                                 AND billing_info_id > :billing_info_id
                                                          GROUP  BY u_id) t2
                                                      ON t1.u_id = t2.u_id
                                                         AND t1.billing_info_id = t2.max_id
                                       ORDER  BY t1.billing_info_id ASC
                                       """), billing_info_id=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_payment')

    rows = [{
        '_user_id': row['u_id'],
        'billing_info_id': row['billing_info_id'],
        'billing_contact': parse_user_billing_info_contact(row['contact_name']),
        'billing_address': row['address_line'],
        'billing_city': row['city'],
        'billing_state': row['state'],
        'billing_zip_code': row['zip'],
        'billing_country': row['country']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['billing_info_id']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'billing_contact': bindparam('billing_contact'),
                'billing_address': bindparam('billing_address'),
                'billing_city': bindparam('billing_city'),
                'billing_state': bindparam('billing_state'),
                'billing_zip_code': bindparam('billing_zip_code'),
                'billing_country': bindparam('billing_country')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_billing_info_id', new_config_value)
            except:
                print('process_user_billing_info_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_billing_info_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_login_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_login_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT t1.*
                                           FROM   tb_user_login_log t1
                                                  INNER JOIN (SELECT u_id,
                                                                     Max(id) max_id
                                                              FROM   tb_user_login_log
                                                              WHERE  remark = "true~"
                                                              GROUP  BY u_id) t2
                                                          ON t1.u_id = t2.u_id
                                                             AND t1.id = t2.max_id
                                           ORDER  BY t1.login_time ASC
                                           """))
        return connection.execute(text("""
                                       SELECT t1.*
                                       FROM   tb_user_login_log t1
                                              INNER JOIN (SELECT u_id,
                                                                 Max(id) max_id
                                                          FROM   tb_user_login_log
                                                          WHERE  remark = "true~"
                                                                 AND login_time >= :login_time
                                                          GROUP  BY u_id) t2
                                                      ON t1.u_id = t2.u_id
                                                         AND t1.id = t2.max_id
                                       ORDER  BY t1.login_time ASC
                                       """), login_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_user_id': row['u_id'],
        'last_login_ip': row['login_ip'],
        'last_login_time': row['login_time']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['last_login_time']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'last_login_ip': bindparam('last_login_ip'),
                'last_login_time': bindparam('last_login_time')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_login_time', new_config_value)
            except:
                print('process_user_login_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_login_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_ourgame_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_ourgame_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text('SELECT * FROM tb_user_ourgame ORDER BY create_time ASC'))
        return connection.execute(text('SELECT * FROM tb_user_ourgame WHERE create_time >= :create_time ORDER BY create_time ASC'), create_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt')

    rows = [{
        '_user_id': row['u_id'],
        'username': row['og_role_name'],
        'og_account': row['og_account'],
        'create_time': row['create_time']
    } for row in result_proxy]

    if rows:

        new_config_value = rows[-1]['create_time']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'username': bindparam('username'),
                'og_account': bindparam('og_account')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_user_ourgame_add_time', new_config_value)
            except:
                print('process_user_ourgame_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_ourgame_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


# def process_user_ourgame_newly_updated_records():
#     config_value = get_config_value(db, 'last_imported_user_ourgame_update_time')

#     def collection(connection, transaction):
#         """ Get newly added. """
#         if config_value is None:
#             return connection.execute(text('SELECT * FROM tb_user_ourgame ORDER BY update_time ASC'))
#         return connection.execute(text('SELECT * FROM tb_user_ourgame WHERE update_time >= :update_time ORDER BY update_time ASC'), update_time=config_value)

#     result_proxy = with_db_context(db, collection, 'orig_wpt')

#     rows = [{
#         '_user_id': row['u_id'],
#         'username': row['og_role_name'],
#         'og_account': row['og_account'],
#         'update_time': row['update_time']
#     } for row in result_proxy]

#     if rows:

#         new_config_value = rows[-1]['update_time']

#         def sync_collection(connection, transaction):
#             """ Sync newly added. """
#             where = BIUser.__table__.c.user_id == bindparam('_user_id')
#             values = {
#                 'username': bindparam('username'),
#                 'og_account': bindparam('og_account')
#                 }

#             try:
#                 connection.execute(BIUser.__table__.update().where(where).values(values), rows)
#                 set_config_value(connection, 'last_imported_user_ourgame_update_time', new_config_value)
#             except:
#                 print('process_user_ourgame_newly_updated_records transaction.rollback()')
#                 transaction.rollback()
#                 raise
#             else:
#                 print('process_user_ourgame_newly_updated_records transaction.commit()')
#                 transaction.commit()
#             return

#         with_db_context(db, sync_collection)

#     return


# def process_user_payment_newly_added_records():
#     config_value = get_config_value(db, 'last_imported_user_bill_dollar_paid_add_time')
#
#     def collection(connection, transaction):
#         """ Get newly added. """
#         if config_value is None:
#             return connection.execute(text("""
#                                            SELECT u_id,
#                                                   Sum(CASE
#                                                         WHEN user_paylog_status_id = 3 THEN 1
#                                                         ELSE 0
#                                                       end)                         AS dollar_paid_succeeded_count,
#                                                   Round(Sum(CASE
#                                                               WHEN user_paylog_status_id = 3 THEN order_price
#                                                               ELSE 0
#                                                             end) / 100, 2)         AS dollar_paid_succeeded_amount,
#                                                   Sum(CASE
#                                                         WHEN ( user_paylog_status_id = 4 OR user_paylog_status_id = 5 ) THEN 1
#                                                         ELSE 0
#                                                       end)                         AS dollar_paid_failed_count,
#                                                   Round(Sum(CASE
#                                                               WHEN ( user_paylog_status_id = 4 OR user_paylog_status_id = 5 ) THEN order_price
#                                                               ELSE 0
#                                                             end) / 100, 2)         AS dollar_paid_failed_amount,
#                                                   Max(createtime)                  AS max_createtime
#                                            FROM   user_paylog
#                                            GROUP  BY u_id
#                                            ORDER  BY max_createtime ASC
#                                            """))
#         fixed_config_value = arrow.get(config_value).replace(hours=-24).format('YYYY-MM-DD HH:mm:ss')
#         return connection.execute(text("""
#                                        SELECT u_id,
#                                               Sum(CASE
#                                                     WHEN user_paylog_status_id = 3 THEN 1
#                                                     ELSE 0
#                                                   end)                         AS dollar_paid_succeeded_count,
#                                               Round(Sum(CASE
#                                                           WHEN user_paylog_status_id = 3 THEN order_price
#                                                           ELSE 0
#                                                         end) / 100, 2)         AS dollar_paid_succeeded_amount,
#                                               Sum(CASE
#                                                     WHEN ( user_paylog_status_id = 4 OR user_paylog_status_id = 5 ) THEN 1
#                                                     ELSE 0
#                                                   end)                         AS dollar_paid_failed_count,
#                                               Round(Sum(CASE
#                                                           WHEN ( user_paylog_status_id = 4 OR user_paylog_status_id = 5 ) THEN order_price
#                                                           ELSE 0
#                                                         end) / 100, 2)         AS dollar_paid_failed_amount,
#                                               Max(createtime)                  AS max_createtime
#                                        FROM   user_paylog
#                                        WHERE  u_id IN (SELECT DISTINCT u_id
#                                                        FROM   user_paylog
#                                                        WHERE  createtime >= :add_time)
#                                        GROUP  BY u_id
#                                        ORDER  BY max_createtime ASC
#                                        """), add_time=fixed_config_value)
#
#     result_proxy = with_db_context(db, collection, 'orig_wpt_payment')
#
#     rows = [{
#         '_user_id': row['u_id'],
#         'dollar_paid_succeeded_count': row['dollar_paid_succeeded_count'],
#         'dollar_paid_succeeded_amount': row['dollar_paid_succeeded_amount'],
#         'dollar_paid_failed_count': row['dollar_paid_failed_count'],
#         'dollar_paid_failed_amount': row['dollar_paid_failed_amount'],
#         'max_createtime': row['max_createtime']
#     } for row in result_proxy]
#
#     if rows:
#         new_config_value = rows[-1]['max_createtime']
#
#         def sync_collection(connection, transaction):
#             """ Sync newly added. """
#             where = BIUser.__table__.c.user_id == bindparam('_user_id')
#             values = {
#                 'dollar_paid_succeeded_count': bindparam('dollar_paid_succeeded_count'),
#                 'dollar_paid_succeeded_amount': bindparam('dollar_paid_succeeded_amount'),
#                 'dollar_paid_failed_count': bindparam('dollar_paid_failed_count'),
#                 'dollar_paid_failed_amount': bindparam('dollar_paid_failed_amount')
#                 }
#
#             try:
#                 connection.execute(BIUser.__table__.update().where(where).values(values), rows)
#                 set_config_value(connection, 'last_imported_user_bill_dollar_paid_add_time', new_config_value)
#             except:
#                 print('process_user_payment_newly_added_records transaction.rollback()')
#                 transaction.rollback()
#                 raise
#             else:
#                 print('process_user_payment_newly_added_records transaction.commit()')
#                 transaction.commit()
#             return
#
#         with_db_context(db, sync_collection)
#
#     return


def process_user_payment_spin_purchase_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_payment_spin_purchase_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT u_id,
                                                  SUM(CASE
                                                        WHEN user_paylog_status_id = 3 THEN 1
                                                        ELSE 0
                                                      END)                 AS count_of_dollar_exchanged_for_spin_purchase,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            END) / 100, 2) AS amount_of_dollar_exchanged_for_spin_purchase,
                                                  MIN(CASE
                                                        WHEN user_paylog_status_id = 3 THEN createtime
                                                        ELSE NULL
                                                      END)                 AS first_time_of_dollar_exchanged_for_spin_purchase,
                                                  MAX(CASE
                                                        WHEN user_paylog_status_id = 3 THEN createtime
                                                        ELSE NULL
                                                      END)                 AS last_time_of_dollar_exchanged_for_spin_purchase,
                                                  Max(createtime)          AS max_createtime
                                           FROM   user_paylog
                                           WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
                                           GROUP  BY u_id
                                           ORDER  BY max_createtime ASC
                                           """))
        fixed_config_value = arrow.get(config_value).replace(hours=-24).format('YYYY-MM-DD HH:mm:ss')
        return connection.execute(text("""
                                       SELECT u_id,
                                              SUM(CASE
                                                    WHEN user_paylog_status_id = 3 THEN 1
                                                    ELSE 0
                                                  END)                 AS count_of_dollar_exchanged_for_spin_purchase,
                                              ROUND(SUM(CASE
                                                          WHEN user_paylog_status_id = 3 THEN order_price
                                                          ELSE 0
                                                        END) / 100, 2) AS amount_of_dollar_exchanged_for_spin_purchase,
                                              MIN(CASE
                                                    WHEN user_paylog_status_id = 3 THEN createtime
                                                    ELSE NULL
                                                  END)                 AS first_time_of_dollar_exchanged_for_spin_purchase,
                                              MAX(CASE
                                                    WHEN user_paylog_status_id = 3 THEN createtime
                                                    ELSE NULL
                                                  END)                 AS last_time_of_dollar_exchanged_for_spin_purchase,
                                              Max(createtime)          AS max_createtime
                                       FROM   user_paylog
                                       WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
                                              AND u_id IN (SELECT DISTINCT u_id
                                                           FROM   user_paylog
                                                           WHERE  createtime >= :add_time)
                                       GROUP  BY u_id
                                       ORDER  BY max_createtime ASC 
                                       """), add_time=fixed_config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_payment')

    rows = [{
        '_user_id': row['u_id'],
        'count_of_dollar_exchanged_for_spin_purchase': row['count_of_dollar_exchanged_for_spin_purchase'],
        'amount_of_dollar_exchanged_for_spin_purchase': row['amount_of_dollar_exchanged_for_spin_purchase'],
        'first_time_of_dollar_exchanged_for_spin_purchase': row['first_time_of_dollar_exchanged_for_spin_purchase'],
        'last_time_of_dollar_exchanged_for_spin_purchase': row['last_time_of_dollar_exchanged_for_spin_purchase'],
        'max_createtime': row['max_createtime']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_createtime']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'count_of_dollar_exchanged_for_spin_purchase': bindparam('count_of_dollar_exchanged_for_spin_purchase'),
                'amount_of_dollar_exchanged_for_spin_purchase': bindparam('amount_of_dollar_exchanged_for_spin_purchase'),
                'last_time_of_dollar_exchanged_for_spin_purchase': bindparam('last_time_of_dollar_exchanged_for_spin_purchase')
                }

            column_keys = ['first_time_of_dollar_exchanged_for_spin_purchase']
            column_params = {}
            for column in column_keys:
                column_params[column + '_where'] = and_(
                                                       BIUser.__table__.c.user_id == bindparam('_user_id'),
                                                       BIUser.__table__.c[column] == None
                                                   )
                column_params[column + '_values'] = {column: bindparam(column)}

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                for column in column_keys:
                    connection.execute(BIUser.__table__.update().where(column_params[column + '_where']).values(column_params[column + '_values']), rows)
                set_config_value(connection, 'last_imported_user_payment_spin_purchase_add_time', new_config_value)
            except:
                print('process_user_payment_spin_purchase_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_payment_spin_purchase_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_payment_spin_purchase_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_user_payment_spin_purchase_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT u_id,
                                                  SUM(CASE
                                                        WHEN user_paylog_status_id = 3 THEN 1
                                                        ELSE 0
                                                      END)                 AS count_of_dollar_exchanged_for_spin_purchase,
                                                  ROUND(SUM(CASE
                                                              WHEN user_paylog_status_id = 3 THEN order_price
                                                              ELSE 0
                                                            END) / 100, 2) AS amount_of_dollar_exchanged_for_spin_purchase,
                                                  MIN(CASE
                                                        WHEN user_paylog_status_id = 3 THEN createtime
                                                        ELSE NULL
                                                      END)                 AS first_time_of_dollar_exchanged_for_spin_purchase,
                                                  MAX(CASE
                                                        WHEN user_paylog_status_id = 3 THEN createtime
                                                        ELSE NULL
                                                      END)                 AS last_time_of_dollar_exchanged_for_spin_purchase,
                                                  MAX(platform_return_time)          AS max_updatetime
                                           FROM   user_paylog
                                           WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
                                           GROUP  BY u_id
                                           ORDER  BY max_updatetime ASC
                                           """))
        fixed_config_value = arrow.get(config_value).replace(hours=-24).format('YYYY-MM-DD HH:mm:ss')
        return connection.execute(text("""
                                       SELECT u_id,
                                              SUM(CASE
                                                    WHEN user_paylog_status_id = 3 THEN 1
                                                    ELSE 0
                                                  END)                 AS count_of_dollar_exchanged_for_spin_purchase,
                                              ROUND(SUM(CASE
                                                          WHEN user_paylog_status_id = 3 THEN order_price
                                                          ELSE 0
                                                        END) / 100, 2) AS amount_of_dollar_exchanged_for_spin_purchase,
                                              MIN(CASE
                                                    WHEN user_paylog_status_id = 3 THEN createtime
                                                    ELSE NULL
                                                  END)                 AS first_time_of_dollar_exchanged_for_spin_purchase,
                                              MAX(CASE
                                                    WHEN user_paylog_status_id = 3 THEN createtime
                                                    ELSE NULL
                                                  END)                 AS last_time_of_dollar_exchanged_for_spin_purchase,
                                              Max(platform_return_time)          AS max_updatetime
                                       FROM   user_paylog
                                       WHERE  tb_product_id = 925011306 AND user_paylog_status_id = 3
                                              AND u_id IN (SELECT DISTINCT u_id
                                                           FROM   user_paylog
                                                           WHERE  platform_return_time >= :update_time)
                                       GROUP  BY u_id
                                       ORDER  BY max_updatetime ASC 
                                       """), update_time=fixed_config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_payment')

    rows = [{
        '_user_id': row['u_id'],
        'count_of_dollar_exchanged_for_spin_purchase': row['count_of_dollar_exchanged_for_spin_purchase'],
        'amount_of_dollar_exchanged_for_spin_purchase': row['amount_of_dollar_exchanged_for_spin_purchase'],
        'first_time_of_dollar_exchanged_for_spin_purchase': row['first_time_of_dollar_exchanged_for_spin_purchase'],
        'last_time_of_dollar_exchanged_for_spin_purchase': row['last_time_of_dollar_exchanged_for_spin_purchase'],
        'max_updatetime': row['max_updatetime']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_updatetime']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'count_of_dollar_exchanged_for_spin_purchase': bindparam('count_of_dollar_exchanged_for_spin_purchase'),
                'amount_of_dollar_exchanged_for_spin_purchase': bindparam('amount_of_dollar_exchanged_for_spin_purchase'),
                'last_time_of_dollar_exchanged_for_spin_purchase': bindparam('last_time_of_dollar_exchanged_for_spin_purchase')
                }

            column_keys = ['first_time_of_dollar_exchanged_for_spin_purchase']
            column_params = {}
            for column in column_keys:
                column_params[column + '_where'] = and_(
                                                       BIUser.__table__.c.user_id == bindparam('_user_id'),
                                                       BIUser.__table__.c[column] == None
                                                   )
                column_params[column + '_values'] = {column: bindparam(column)}

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                for column in column_keys:
                    connection.execute(BIUser.__table__.update().where(column_params[column + '_where']).values(column_params[column + '_values']), rows)
                set_config_value(connection, 'last_imported_user_payment_spin_purchase_update_time', new_config_value)
            except:
                print('process_user_payment_spin_purchase_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_payment_spin_purchase_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_gold_balance_related_records():
    config_value = get_config_value(db, 'last_imported_og_powergamecoin_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT tb.username     AS og_account,
                                                  (SELECT MIN(recdate)
                                                   FROM   powergamecoin_detail
                                                   WHERE  tb.username = username AND producttype = 925011307
                                                   ORDER  BY recdate ASC
                                                   LIMIT  1)      AS first_free_spin_time,
                                                  (SELECT MAX(recdate)
                                                   FROM   powergamecoin_detail
                                                   WHERE  tb.username = username AND producttype = 925011307
                                                   ORDER  BY recdate DESC
                                                   LIMIT  1)      AS last_free_spin_time,
                                                  (SELECT recdate
                                                   FROM   powergamecoin_detail
                                                   WHERE  tb.username = username AND gamecoin < 0
                                                   ORDER  BY recdate ASC
                                                   LIMIT  1)      AS first_poker_time,
                                                  (SELECT recdate
                                                   FROM   powergamecoin_detail
                                                   WHERE  tb.username = username AND gamecoin < 0
                                                   ORDER  BY recdate DESC
                                                   LIMIT  1)      AS last_poker_time,
                                                  (SELECT ( gamecoin + coin )
                                                   FROM   powergamecoin_detail
                                                   WHERE  tb.username = username
                                                   ORDER  BY recdate DESC
                                                   LIMIT  1)      AS gold_balance,
                                                  Max(tb.recdate) AS max_recdate
                                           FROM   powergamecoin_detail tb
                                           GROUP  BY og_account
                                           ORDER  BY max_recdate ASC
                                           """))
        return connection.execute(text("""
                                       SELECT tb.username     AS og_account,
                                              (SELECT MIN(recdate)
                                               FROM   powergamecoin_detail
                                               WHERE  tb.username = username AND producttype = 925011307
                                               ORDER  BY recdate ASC
                                               LIMIT  1)      AS first_free_spin_time,
                                              (SELECT MAX(recdate)
                                               FROM   powergamecoin_detail
                                               WHERE  tb.username = username AND producttype = 925011307
                                               ORDER  BY recdate DESC
                                               LIMIT  1)      AS last_free_spin_time,
                                              (SELECT recdate
                                               FROM   powergamecoin_detail
                                               WHERE  tb.username = username AND gamecoin < 0
                                               ORDER  BY recdate ASC
                                               LIMIT  1)      AS first_poker_time,
                                              (SELECT recdate
                                               FROM   powergamecoin_detail
                                               WHERE  tb.username = username AND gamecoin < 0
                                               ORDER  BY recdate DESC
                                               LIMIT  1)      AS last_poker_time,
                                              (SELECT ( gamecoin + coin )
                                               FROM   powergamecoin_detail
                                               WHERE  tb.username = username
                                               ORDER  BY recdate DESC
                                               LIMIT  1)      AS gold_balance,
                                              Max(tb.recdate) AS max_recdate
                                       FROM   powergamecoin_detail tb
                                       WHERE  tb.recdate >= :add_time
                                       GROUP  BY og_account
                                       ORDER  BY max_recdate ASC
                                       """), add_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_ods')

    rows = [{
        '_og_account': row['og_account'],
        'gold_balance': row['gold_balance'],
        'first_poker_time': row['first_poker_time'],
        'last_poker_time': row['last_poker_time'],
        'first_free_spin_time': row['first_free_spin_time'],
        'last_free_spin_time': row['last_free_spin_time'],
        'max_recdate': row['max_recdate']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_recdate']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.og_account == bindparam('_og_account')
            values = {
                'gold_balance': bindparam('gold_balance'),
                'first_poker_time': bindparam('first_poker_time'),
                'last_poker_time': bindparam('last_poker_time'),
                'first_free_spin_time': bindparam('first_free_spin_time'),
                'last_free_spin_time': bindparam('last_free_spin_time')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_og_powergamecoin_add_time', new_config_value)
            except:
                print('process_user_gold_balance_related_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_gold_balance_related_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_silver_balance_related_records():
    config_value = get_config_value(db, 'last_imported_og_gamecoin_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT tb.username     AS og_account,
                                                  (SELECT recdate
                                                   FROM   gamecoin_detail
                                                   WHERE  tb.username = username AND gamecoin < 0
                                                   ORDER  BY recdate ASC
                                                   LIMIT  1)      AS first_slots_time,
                                                  (SELECT recdate
                                                   FROM   gamecoin_detail
                                                   WHERE  tb.username = username AND gamecoin < 0
                                                   ORDER  BY recdate DESC
                                                   LIMIT  1)      AS last_slots_time,
                                                  (SELECT ( gamecoin + coin )
                                                   FROM   gamecoin_detail
                                                   WHERE  tb.username = username
                                                   ORDER  BY recdate DESC
                                                   LIMIT  1)      AS silver_balance,
                                                  Max(tb.recdate) AS max_recdate
                                           FROM   gamecoin_detail tb
                                           GROUP  BY og_account
                                           ORDER  BY max_recdate ASC
                                           """))
        return connection.execute(text("""
                                       SELECT tb.username     AS og_account,
                                              (SELECT recdate
                                               FROM   gamecoin_detail
                                               WHERE  tb.username = username AND gamecoin < 0
                                               ORDER  BY recdate ASC
                                               LIMIT  1)      AS first_slots_time,
                                              (SELECT recdate
                                               FROM   gamecoin_detail
                                               WHERE  tb.username = username AND gamecoin < 0
                                               ORDER  BY recdate DESC
                                               LIMIT  1)      AS last_slots_time,
                                              (SELECT ( gamecoin + coin )
                                               FROM   gamecoin_detail
                                               WHERE  tb.username = username
                                               ORDER  BY recdate DESC
                                               LIMIT  1)      AS silver_balance,
                                              Max(tb.recdate) AS max_recdate
                                       FROM   gamecoin_detail tb
                                       WHERE  tb.recdate >= :add_time
                                       GROUP  BY og_account
                                       ORDER  BY max_recdate ASC
                                       """), add_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_ods')

    rows = [{
        '_og_account': row['og_account'],
        'silver_balance': row['silver_balance'],
        'first_slots_time': row['first_slots_time'],
        'last_slots_time': row['last_slots_time'],
        'max_recdate': row['max_recdate']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_recdate']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.og_account == bindparam('_og_account')
            values = {
                'silver_balance': bindparam('silver_balance'),
                'first_slots_time': bindparam('first_slots_time'),
                'last_slots_time': bindparam('last_slots_time')
                }

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                set_config_value(connection, 'last_imported_og_gamecoin_add_time', new_config_value)
            except:
                print('process_user_silver_balance_related_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_silver_balance_related_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_reward_point_related_records():
    pass


def process_user_mall_order_newly_added_records():
    config_value = get_config_value(db, 'last_imported_user_mall_order_add_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT o.UserId,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_masterpoint_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_masterpoint_exchanged_for_gold,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_masterpoint_exchanged_for_gold,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_masterpoint_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_gold,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_gold,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_silver,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_silver,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_silver,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_silver,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_lucky_spin,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_lucky_spin,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_lucky_spin,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_lucky_spin,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_lucky_charm,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_lucky_charm,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_lucky_charm,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_lucky_charm,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_avatar,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_avatar,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_avatar,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_avatar,
                                                  SUM(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_emoji,
                                                  SUM(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_emoji,
                                                  MIN(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_emoji,
                                                  MAX(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_emoji,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_spin_booster,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_spin_booster,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_spin_booster,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_spin_booster,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_spin_ticket,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_spin_ticket,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_spin_ticket,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_spin_ticket,
                                                  Max(o.CDate) AS max_createtime
                                           FROM   Mall_tOrder o
                                                  LEFT JOIN Mall_tOrderProductLog op
                                                         ON op.OrderId = o.OrderId
                                                  LEFT JOIN Mall_tProduct p
                                                         ON op.ProductId = p.Id
                                                  LEFT JOIN Mall_tCurrency c
                                                         ON o.CurrencyCode = c.CurrencyCode
                                           WHERE  o.OrderStatus != 1 AND o.OrderStatus != 41 AND (o.PaymentMode IS NULL OR o.PaymentMode = 1)
                                           GROUP  BY o.UserId
                                           ORDER  BY max_createtime ASC
                                           """))
        return connection.execute(text("""
                                       SELECT o.UserId,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_masterpoint_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_masterpoint_exchanged_for_gold,
                                              MIN(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_masterpoint_exchanged_for_gold,
                                              MAX(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_masterpoint_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_gold,
                                              MIN(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_gold,
                                              MAX(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_silver,
                                              MIN(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_silver,
                                              MAX(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_silver,
                                              MIN(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_silver,
                                              MAX(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_lucky_spin,
                                              SUM(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  End)     AS amount_of_dollar_exchanged_for_lucky_spin,
                                              MIN(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_lucky_spin,
                                              MAX(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  End)     AS last_time_of_dollar_exchanged_for_lucky_spin,
                                              SUM(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_lucky_charm,
                                              SUM(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_lucky_charm,
                                              MIN(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_lucky_charm,
                                              MAX(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_lucky_charm,
                                              SUM(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_avatar,
                                              SUM(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_avatar,
                                              MIN(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_avatar,
                                              MAX(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_avatar,
                                              SUM(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_emoji,
                                              SUM(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_emoji,
                                              MIN(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_emoji,
                                              MAX(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_emoji,
                                              SUM(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_spin_booster,
                                              SUM(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_spin_booster,
                                              MIN(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_spin_booster,
                                              MAX(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_spin_booster,
                                              SUM(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_spin_ticket,
                                              SUM(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_spin_ticket,
                                              MIN(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_spin_ticket,
                                              MAX(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_spin_ticket,
                                              Max(o.CDate) AS max_createtime
                                       FROM   Mall_tOrder o
                                              LEFT JOIN Mall_tOrderProductLog op
                                                     ON op.OrderId = o.OrderId
                                              LEFT JOIN Mall_tProduct p
                                                     ON op.ProductId = p.Id
                                              LEFT JOIN Mall_tCurrency c
                                                     ON o.CurrencyCode = c.CurrencyCode
                                       WHERE  o.OrderStatus != 1 AND o.OrderStatus != 41 AND (o.PaymentMode IS NULL OR o.PaymentMode = 1)
                                              AND o.UserId IN (SELECT DISTINCT UserId
                                                               FROM   Mall_tOrder
                                                               WHERE  CDate >= :add_time)
                                       GROUP  BY o.UserId
                                       ORDER  BY max_createtime ASC
                                       """), add_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_mall')

    rows = [{
        '_user_id': row['UserId'],

        'count_of_masterpoint_exchanged_for_gold': row['count_of_masterpoint_exchanged_for_gold'],
        'amount_of_masterpoint_exchanged_for_gold': row['amount_of_masterpoint_exchanged_for_gold'],
        'first_time_of_masterpoint_exchanged_for_gold': row['first_time_of_masterpoint_exchanged_for_gold'],
        'last_time_of_masterpoint_exchanged_for_gold': row['last_time_of_masterpoint_exchanged_for_gold'],

        'count_of_dollar_exchanged_for_gold': row['count_of_dollar_exchanged_for_gold'],
        'amount_of_dollar_exchanged_for_gold': row['amount_of_dollar_exchanged_for_gold'],
        'first_time_of_dollar_exchanged_for_gold': row['first_time_of_dollar_exchanged_for_gold'],
        'last_time_of_dollar_exchanged_for_gold': row['last_time_of_dollar_exchanged_for_gold'],

        'count_of_gold_exchanged_for_silver': row['count_of_gold_exchanged_for_silver'],
        'amount_of_gold_exchanged_for_silver': row['amount_of_gold_exchanged_for_silver'],
        'first_time_of_gold_exchanged_for_silver': row['first_time_of_gold_exchanged_for_silver'],
        'last_time_of_gold_exchanged_for_silver': row['last_time_of_gold_exchanged_for_silver'],

        'count_of_dollar_exchanged_for_silver': row['count_of_dollar_exchanged_for_silver'],
        'amount_of_dollar_exchanged_for_silver': row['amount_of_dollar_exchanged_for_silver'],
        'first_time_of_dollar_exchanged_for_silver': row['first_time_of_dollar_exchanged_for_silver'],
        'last_time_of_dollar_exchanged_for_silver': row['last_time_of_dollar_exchanged_for_silver'],

        'count_of_dollar_exchanged_for_lucky_spin': row['count_of_dollar_exchanged_for_lucky_spin'],
        'amount_of_dollar_exchanged_for_lucky_spin': row['amount_of_dollar_exchanged_for_lucky_spin'],
        'first_time_of_dollar_exchanged_for_lucky_spin': row['first_time_of_dollar_exchanged_for_lucky_spin'],
        'last_time_of_dollar_exchanged_for_lucky_spin': row['last_time_of_dollar_exchanged_for_lucky_spin'],

        'count_of_gold_exchanged_for_lucky_charm': row['count_of_gold_exchanged_for_lucky_charm'],
        'amount_of_gold_exchanged_for_lucky_charm': row['amount_of_gold_exchanged_for_lucky_charm'],
        'first_time_of_gold_exchanged_for_lucky_charm': row['first_time_of_gold_exchanged_for_lucky_charm'],
        'last_time_of_gold_exchanged_for_lucky_charm': row['last_time_of_gold_exchanged_for_lucky_charm'],

        'count_of_gold_exchanged_for_avatar': row['count_of_gold_exchanged_for_avatar'],
        'amount_of_gold_exchanged_for_avatar': row['amount_of_gold_exchanged_for_avatar'],
        'first_time_of_gold_exchanged_for_avatar': row['first_time_of_gold_exchanged_for_avatar'],
        'last_time_of_gold_exchanged_for_avatar': row['last_time_of_gold_exchanged_for_avatar'],

        'count_of_gold_exchanged_for_emoji': row['count_of_gold_exchanged_for_emoji'],
        'amount_of_gold_exchanged_for_emoji': row['amount_of_gold_exchanged_for_emoji'],
        'first_time_of_gold_exchanged_for_emoji': row['first_time_of_gold_exchanged_for_emoji'],
        'last_time_of_gold_exchanged_for_emoji': row['last_time_of_gold_exchanged_for_emoji'],

        'count_of_dollar_exchanged_for_spin_booster': row['count_of_dollar_exchanged_for_spin_booster'],
        'amount_of_dollar_exchanged_for_spin_booster': row['amount_of_dollar_exchanged_for_spin_booster'],
        'first_time_of_dollar_exchanged_for_spin_booster': row['first_time_of_dollar_exchanged_for_spin_booster'],
        'last_time_of_dollar_exchanged_for_spin_booster': row['last_time_of_dollar_exchanged_for_spin_booster'],

        'count_of_dollar_exchanged_for_spin_ticket': row['count_of_dollar_exchanged_for_spin_ticket'],
        'amount_of_dollar_exchanged_for_spin_ticket': row['amount_of_dollar_exchanged_for_spin_ticket'],
        'first_time_of_dollar_exchanged_for_spin_ticket': row['first_time_of_dollar_exchanged_for_spin_ticket'],
        'last_time_of_dollar_exchanged_for_spin_ticket': row['last_time_of_dollar_exchanged_for_spin_ticket'],

        'max_createtime': row['max_createtime']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_createtime']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'count_of_masterpoint_exchanged_for_gold': bindparam('count_of_masterpoint_exchanged_for_gold'),
                'amount_of_masterpoint_exchanged_for_gold': bindparam('amount_of_masterpoint_exchanged_for_gold'),
                'last_time_of_masterpoint_exchanged_for_gold': bindparam('last_time_of_masterpoint_exchanged_for_gold'),
                
                'count_of_dollar_exchanged_for_gold': bindparam('count_of_dollar_exchanged_for_gold'),
                'amount_of_dollar_exchanged_for_gold': bindparam('amount_of_dollar_exchanged_for_gold'),
                'last_time_of_dollar_exchanged_for_gold': bindparam('last_time_of_dollar_exchanged_for_gold'),
                
                'count_of_gold_exchanged_for_silver': bindparam('count_of_gold_exchanged_for_silver'),
                'amount_of_gold_exchanged_for_silver': bindparam('amount_of_gold_exchanged_for_silver'),
                'last_time_of_gold_exchanged_for_silver': bindparam('last_time_of_gold_exchanged_for_silver'),
                
                'count_of_dollar_exchanged_for_silver': bindparam('count_of_dollar_exchanged_for_silver'),
                'amount_of_dollar_exchanged_for_silver': bindparam('amount_of_dollar_exchanged_for_silver'),
                'last_time_of_dollar_exchanged_for_silver': bindparam('last_time_of_dollar_exchanged_for_silver'),
                
                'count_of_dollar_exchanged_for_lucky_spin': bindparam('count_of_dollar_exchanged_for_lucky_spin'),
                'amount_of_dollar_exchanged_for_lucky_spin': bindparam('amount_of_dollar_exchanged_for_lucky_spin'),
                'last_time_of_dollar_exchanged_for_lucky_spin': bindparam('last_time_of_dollar_exchanged_for_lucky_spin'),
                
                'count_of_gold_exchanged_for_lucky_charm': bindparam('count_of_gold_exchanged_for_lucky_charm'),
                'amount_of_gold_exchanged_for_lucky_charm': bindparam('amount_of_gold_exchanged_for_lucky_charm'),
                'last_time_of_gold_exchanged_for_lucky_charm': bindparam('last_time_of_gold_exchanged_for_lucky_charm'),
                
                'count_of_gold_exchanged_for_avatar': bindparam('count_of_gold_exchanged_for_avatar'),
                'amount_of_gold_exchanged_for_avatar': bindparam('amount_of_gold_exchanged_for_avatar'),
                'last_time_of_gold_exchanged_for_avatar': bindparam('last_time_of_gold_exchanged_for_avatar'),
                
                'count_of_gold_exchanged_for_emoji': bindparam('count_of_gold_exchanged_for_emoji'),
                'amount_of_gold_exchanged_for_emoji': bindparam('amount_of_gold_exchanged_for_emoji'),
                'last_time_of_gold_exchanged_for_emoji': bindparam('last_time_of_gold_exchanged_for_emoji'),
                
                'count_of_dollar_exchanged_for_spin_booster': bindparam('count_of_dollar_exchanged_for_spin_booster'),
                'amount_of_dollar_exchanged_for_spin_booster': bindparam('amount_of_dollar_exchanged_for_spin_booster'),
                'last_time_of_dollar_exchanged_for_spin_booster': bindparam('last_time_of_dollar_exchanged_for_spin_booster'),
                
                'count_of_dollar_exchanged_for_spin_ticket': bindparam('count_of_dollar_exchanged_for_spin_ticket'),
                'amount_of_dollar_exchanged_for_spin_ticket': bindparam('amount_of_dollar_exchanged_for_spin_ticket'),
                'last_time_of_dollar_exchanged_for_spin_ticket': bindparam('last_time_of_dollar_exchanged_for_spin_ticket'),
                }

            column_keys = ['first_time_of_masterpoint_exchanged_for_gold',
                           'first_time_of_dollar_exchanged_for_gold',
                           'first_time_of_gold_exchanged_for_silver',
                           'first_time_of_dollar_exchanged_for_silver',
                           'first_time_of_dollar_exchanged_for_lucky_spin',
                           'first_time_of_gold_exchanged_for_lucky_charm',
                           'first_time_of_gold_exchanged_for_avatar',
                           'first_time_of_gold_exchanged_for_emoji',
                           'first_time_of_dollar_exchanged_for_spin_booster',
                           'first_time_of_dollar_exchanged_for_spin_ticket']
            column_params = {}
            for column in column_keys:
                column_params[column + '_where'] = and_(
                                                       BIUser.__table__.c.user_id == bindparam('_user_id'),
                                                       BIUser.__table__.c[column] == None
                                                   )
                column_params[column + '_values'] = {column: bindparam(column)}

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                for column in column_keys:
                    connection.execute(BIUser.__table__.update().where(column_params[column + '_where']).values(column_params[column + '_values']), rows)
                set_config_value(connection, 'last_imported_user_mall_order_add_time', new_config_value)
            except:
                print('process_user_mall_order_newly_added_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_mall_order_newly_added_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_mall_order_newly_updated_records():
    config_value = get_config_value(db, 'last_imported_user_mall_order_update_time')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT o.UserId,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_masterpoint_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_masterpoint_exchanged_for_gold,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_masterpoint_exchanged_for_gold,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_masterpoint_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_gold,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_gold,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_gold,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_silver,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_silver,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_silver,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_silver,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_silver,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_lucky_spin,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_lucky_spin,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_lucky_spin,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_lucky_spin,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_lucky_charm,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_lucky_charm,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_lucky_charm,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_lucky_charm,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_avatar,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_avatar,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_avatar,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_avatar,
                                                  SUM(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_gold_exchanged_for_emoji,
                                                  SUM(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_gold_exchanged_for_emoji,
                                                  MIN(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_gold_exchanged_for_emoji,
                                                  MAX(CASE
                                                        WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_gold_exchanged_for_emoji,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_spin_booster,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_spin_booster,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_spin_booster,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_spin_booster,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                        ELSE 0
                                                      END)     AS count_of_dollar_exchanged_for_spin_ticket,
                                                  SUM(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                        ELSE 0
                                                      END)     AS amount_of_dollar_exchanged_for_spin_ticket,
                                                  MIN(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS first_time_of_dollar_exchanged_for_spin_ticket,
                                                  MAX(CASE
                                                        WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                        ELSE NULL
                                                      END)     AS last_time_of_dollar_exchanged_for_spin_ticket,
                                                  Max(o.UDate) AS max_updatetime
                                           FROM   Mall_tOrder o
                                                  LEFT JOIN Mall_tOrderProductLog op
                                                         ON op.OrderId = o.OrderId
                                                  LEFT JOIN Mall_tProduct p
                                                         ON op.ProductId = p.Id
                                                  LEFT JOIN Mall_tCurrency c
                                                         ON o.CurrencyCode = c.CurrencyCode
                                           WHERE  o.OrderStatus != 1 AND o.OrderStatus != 41 AND (o.PaymentMode IS NULL OR o.PaymentMode = 1) AND o.udate IS NOT NULL
                                           GROUP  BY o.UserId
                                           ORDER  BY max_updatetime ASC
                                           """))
        return connection.execute(text("""
                                       SELECT o.UserId,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_masterpoint_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_masterpoint_exchanged_for_gold,
                                              MIN(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_masterpoint_exchanged_for_gold,
                                              MAX(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 106 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_masterpoint_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_gold,
                                              MIN(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_gold,
                                              MAX(CASE
                                                    WHEN ( p.Id = 1 OR p.ParentId = 1 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_gold,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  End)     AS amount_of_gold_exchanged_for_silver,
                                              MIN(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_silver,
                                              MAX(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_silver,
                                              MIN(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_silver,
                                              MAX(CASE
                                                    WHEN ( p.Id = 2 OR p.ParentId = 2 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_silver,
                                              SUM(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_lucky_spin,
                                              SUM(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_lucky_spin,
                                              MIN(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_lucky_spin,
                                              MAX(CASE
                                                    WHEN ( p.Id = 3 OR p.ParentId = 3 ) AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_lucky_spin,
                                              SUM(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_lucky_charm,
                                              SUM(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_lucky_charm,
                                              MIN(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_lucky_charm,
                                              MAX(CASE
                                                    WHEN ( p.Id = 5 OR p.ParentId = 5 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_lucky_charm,
                                              SUM(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_avatar,
                                              SUM(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_avatar,
                                              MIN(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_avatar,
                                              MAX(CASE
                                                    WHEN ( p.Id = 6 OR p.ParentId = 6 ) AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_avatar,
                                              SUM(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_gold_exchanged_for_emoji,
                                              SUM(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_gold_exchanged_for_emoji,
                                              MIN(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_gold_exchanged_for_emoji,
                                              MAX(CASE
                                                    WHEN p.ParentId = 35 AND o.CurrencyCode = 101 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_gold_exchanged_for_emoji,
                                              SUM(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_spin_booster,
                                              SUM(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_spin_booster,
                                              MIN(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_spin_booster,
                                              MAX(CASE
                                                    WHEN ( p.Id = 8 OR p.Id = 15 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_spin_booster,
                                              SUM(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN 1
                                                    ELSE 0
                                                  END)     AS count_of_dollar_exchanged_for_spin_ticket,
                                              SUM(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.TotalPrice
                                                    ELSE 0
                                                  END)     AS amount_of_dollar_exchanged_for_spin_ticket,
                                              MIN(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS first_time_of_dollar_exchanged_for_spin_ticket,
                                              MAX(CASE
                                                    WHEN ( p.Id = 9 OR p.Id = 16 ) AND p.ParentId = 3 AND o.CurrencyCode = 201 THEN o.CDate
                                                    ELSE NULL
                                                  END)     AS last_time_of_dollar_exchanged_for_spin_ticket,
                                              Max(o.UDate) AS max_updatetime
                                       FROM   Mall_tOrder o
                                              LEFT JOIN Mall_tOrderProductLog op
                                                     ON op.OrderId = o.OrderId
                                              LEFT JOIN Mall_tProduct p
                                                     ON op.ProductId = p.Id
                                              LEFT JOIN Mall_tCurrency c
                                                     ON o.CurrencyCode = c.CurrencyCode
                                       WHERE  o.OrderStatus != 1 AND o.OrderStatus != 41 AND (o.PaymentMode IS NULL OR o.PaymentMode = 1)
                                              AND o.UserId IN (SELECT DISTINCT UserId
                                                               FROM   Mall_tOrder
                                                               WHERE  UDate >= :update_time)
                                       GROUP  BY o.UserId
                                       ORDER  BY max_updatetime ASC
                                       """), update_time=config_value)

    result_proxy = with_db_context(db, collection, 'orig_wpt_mall')

    rows = [{
        '_user_id': row['UserId'],

        'count_of_masterpoint_exchanged_for_gold': row['count_of_masterpoint_exchanged_for_gold'],
        'amount_of_masterpoint_exchanged_for_gold': row['amount_of_masterpoint_exchanged_for_gold'],
        'first_time_of_masterpoint_exchanged_for_gold': row['first_time_of_masterpoint_exchanged_for_gold'],
        'last_time_of_masterpoint_exchanged_for_gold': row['last_time_of_masterpoint_exchanged_for_gold'],

        'count_of_dollar_exchanged_for_gold': row['count_of_dollar_exchanged_for_gold'],
        'amount_of_dollar_exchanged_for_gold': row['amount_of_dollar_exchanged_for_gold'],
        'first_time_of_dollar_exchanged_for_gold': row['first_time_of_dollar_exchanged_for_gold'],
        'last_time_of_dollar_exchanged_for_gold': row['last_time_of_dollar_exchanged_for_gold'],

        'count_of_gold_exchanged_for_silver': row['count_of_gold_exchanged_for_silver'],
        'amount_of_gold_exchanged_for_silver': row['amount_of_gold_exchanged_for_silver'],
        'first_time_of_gold_exchanged_for_silver': row['first_time_of_gold_exchanged_for_silver'],
        'last_time_of_gold_exchanged_for_silver': row['last_time_of_gold_exchanged_for_silver'],

        'count_of_dollar_exchanged_for_silver': row['count_of_dollar_exchanged_for_silver'],
        'amount_of_dollar_exchanged_for_silver': row['amount_of_dollar_exchanged_for_silver'],
        'first_time_of_dollar_exchanged_for_silver': row['first_time_of_dollar_exchanged_for_silver'],
        'last_time_of_dollar_exchanged_for_silver': row['last_time_of_dollar_exchanged_for_silver'],

        'count_of_dollar_exchanged_for_lucky_spin': row['count_of_dollar_exchanged_for_lucky_spin'],
        'amount_of_dollar_exchanged_for_lucky_spin': row['amount_of_dollar_exchanged_for_lucky_spin'],
        'first_time_of_dollar_exchanged_for_lucky_spin': row['first_time_of_dollar_exchanged_for_lucky_spin'],
        'last_time_of_dollar_exchanged_for_lucky_spin': row['last_time_of_dollar_exchanged_for_lucky_spin'],

        'count_of_gold_exchanged_for_lucky_charm': row['count_of_gold_exchanged_for_lucky_charm'],
        'amount_of_gold_exchanged_for_lucky_charm': row['amount_of_gold_exchanged_for_lucky_charm'],
        'first_time_of_gold_exchanged_for_lucky_charm': row['first_time_of_gold_exchanged_for_lucky_charm'],
        'last_time_of_gold_exchanged_for_lucky_charm': row['last_time_of_gold_exchanged_for_lucky_charm'],

        'count_of_gold_exchanged_for_avatar': row['count_of_gold_exchanged_for_avatar'],
        'amount_of_gold_exchanged_for_avatar': row['amount_of_gold_exchanged_for_avatar'],
        'first_time_of_gold_exchanged_for_avatar': row['first_time_of_gold_exchanged_for_avatar'],
        'last_time_of_gold_exchanged_for_avatar': row['last_time_of_gold_exchanged_for_avatar'],

        'count_of_gold_exchanged_for_emoji': row['count_of_gold_exchanged_for_emoji'],
        'amount_of_gold_exchanged_for_emoji': row['amount_of_gold_exchanged_for_emoji'],
        'first_time_of_gold_exchanged_for_emoji': row['first_time_of_gold_exchanged_for_emoji'],
        'last_time_of_gold_exchanged_for_emoji': row['last_time_of_gold_exchanged_for_emoji'],

        'count_of_dollar_exchanged_for_spin_booster': row['count_of_dollar_exchanged_for_spin_booster'],
        'amount_of_dollar_exchanged_for_spin_booster': row['amount_of_dollar_exchanged_for_spin_booster'],
        'first_time_of_dollar_exchanged_for_spin_booster': row['first_time_of_dollar_exchanged_for_spin_booster'],
        'last_time_of_dollar_exchanged_for_spin_booster': row['last_time_of_dollar_exchanged_for_spin_booster'],

        'count_of_dollar_exchanged_for_spin_ticket': row['count_of_dollar_exchanged_for_spin_ticket'],
        'amount_of_dollar_exchanged_for_spin_ticket': row['amount_of_dollar_exchanged_for_spin_ticket'],
        'first_time_of_dollar_exchanged_for_spin_ticket': row['first_time_of_dollar_exchanged_for_spin_ticket'],
        'last_time_of_dollar_exchanged_for_spin_ticket': row['last_time_of_dollar_exchanged_for_spin_ticket'],

        'max_updatetime': row['max_updatetime']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_updatetime']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'count_of_masterpoint_exchanged_for_gold': bindparam('count_of_masterpoint_exchanged_for_gold'),
                'amount_of_masterpoint_exchanged_for_gold': bindparam('amount_of_masterpoint_exchanged_for_gold'),
                'last_time_of_masterpoint_exchanged_for_gold': bindparam('last_time_of_masterpoint_exchanged_for_gold'),

                'count_of_dollar_exchanged_for_gold': bindparam('count_of_dollar_exchanged_for_gold'),
                'amount_of_dollar_exchanged_for_gold': bindparam('amount_of_dollar_exchanged_for_gold'),
                'last_time_of_dollar_exchanged_for_gold': bindparam('last_time_of_dollar_exchanged_for_gold'),

                'count_of_gold_exchanged_for_silver': bindparam('count_of_gold_exchanged_for_silver'),
                'amount_of_gold_exchanged_for_silver': bindparam('amount_of_gold_exchanged_for_silver'),
                'last_time_of_gold_exchanged_for_silver': bindparam('last_time_of_gold_exchanged_for_silver'),

                'count_of_dollar_exchanged_for_silver': bindparam('count_of_dollar_exchanged_for_silver'),
                'amount_of_dollar_exchanged_for_silver': bindparam('amount_of_dollar_exchanged_for_silver'),
                'last_time_of_dollar_exchanged_for_silver': bindparam('last_time_of_dollar_exchanged_for_silver'),

                'count_of_dollar_exchanged_for_lucky_spin': bindparam('count_of_dollar_exchanged_for_lucky_spin'),
                'amount_of_dollar_exchanged_for_lucky_spin': bindparam('amount_of_dollar_exchanged_for_lucky_spin'),
                'last_time_of_dollar_exchanged_for_lucky_spin': bindparam('last_time_of_dollar_exchanged_for_lucky_spin'),

                'count_of_gold_exchanged_for_lucky_charm': bindparam('count_of_gold_exchanged_for_lucky_charm'),
                'amount_of_gold_exchanged_for_lucky_charm': bindparam('amount_of_gold_exchanged_for_lucky_charm'),
                'last_time_of_gold_exchanged_for_lucky_charm': bindparam('last_time_of_gold_exchanged_for_lucky_charm'),

                'count_of_gold_exchanged_for_avatar': bindparam('count_of_gold_exchanged_for_avatar'),
                'amount_of_gold_exchanged_for_avatar': bindparam('amount_of_gold_exchanged_for_avatar'),
                'last_time_of_gold_exchanged_for_avatar': bindparam('last_time_of_gold_exchanged_for_avatar'),

                'count_of_gold_exchanged_for_emoji': bindparam('count_of_gold_exchanged_for_emoji'),
                'amount_of_gold_exchanged_for_emoji': bindparam('amount_of_gold_exchanged_for_emoji'),
                'last_time_of_gold_exchanged_for_emoji': bindparam('last_time_of_gold_exchanged_for_emoji'),

                'count_of_dollar_exchanged_for_spin_booster': bindparam('count_of_dollar_exchanged_for_spin_booster'),
                'amount_of_dollar_exchanged_for_spin_booster': bindparam('amount_of_dollar_exchanged_for_spin_booster'),
                'last_time_of_dollar_exchanged_for_spin_booster': bindparam('last_time_of_dollar_exchanged_for_spin_booster'),

                'count_of_dollar_exchanged_for_spin_ticket': bindparam('count_of_dollar_exchanged_for_spin_ticket'),
                'amount_of_dollar_exchanged_for_spin_ticket': bindparam('amount_of_dollar_exchanged_for_spin_ticket'),
                'last_time_of_dollar_exchanged_for_spin_ticket': bindparam('last_time_of_dollar_exchanged_for_spin_ticket'),
            }

            column_keys = ['first_time_of_masterpoint_exchanged_for_gold',
                           'first_time_of_dollar_exchanged_for_gold',
                           'first_time_of_gold_exchanged_for_silver',
                           'first_time_of_dollar_exchanged_for_silver',
                           'first_time_of_dollar_exchanged_for_lucky_spin',
                           'first_time_of_gold_exchanged_for_lucky_charm',
                           'first_time_of_gold_exchanged_for_avatar',
                           'first_time_of_gold_exchanged_for_emoji',
                           'first_time_of_dollar_exchanged_for_spin_booster',
                           'first_time_of_dollar_exchanged_for_spin_ticket']
            column_params = {}
            for column in column_keys:
                column_params[column + '_where'] = and_(
                    BIUser.__table__.c.user_id == bindparam('_user_id'),
                    BIUser.__table__.c[column] == None
                )
                column_params[column + '_values'] = {column: bindparam(column)}

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                for column in column_keys:
                    connection.execute(BIUser.__table__.update().where(column_params[column + '_where']).values(column_params[column + '_values']), rows)
                set_config_value(connection, 'last_imported_user_mall_order_update_time', new_config_value)
            except:
                print('process_user_mall_order_newly_updated_records transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_mall_order_newly_updated_records transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


def process_user_promotion_time():
    config_value = get_config_value(db, 'last_imported_promotion_history_id')

    def collection(connection, transaction):
        """ Get newly added. """
        if config_value is None:
            return connection.execute(text("""
                                           SELECT user_id,
                                                  MIN(CASE
                                                        WHEN push_type = 'fb_notification' THEN scheduled_at
                                                        ELSE NULL
                                                      END) AS first_promotion_fb_notification_time,
                                                  MIN(CASE
                                                        WHEN push_type = 'email' THEN scheduled_at
                                                        ELSE NULL
                                                      END) AS first_promotion_email_time,
                                                  MAX(CASE
                                                        WHEN push_type = 'fb_notification' THEN scheduled_at
                                                        ELSE NULL
                                                      END) AS last_promotion_fb_notification_time,
                                                  MAX(CASE
                                                        WHEN push_type = 'email' THEN scheduled_at
                                                        ELSE NULL
                                                      END) AS last_promotion_email_time,
                                                  MAX(id)  AS max_id
                                           FROM   promotion_push_history
                                           WHERE  status = 'success'
                                           GROUP  BY user_id
                                           ORDER  BY max_id ASC
                                           """))
        return connection.execute(text("""
                                       SELECT user_id,
                                              MIN(CASE
                                                    WHEN push_type = 'fb_notification' THEN scheduled_at
                                                    ELSE NULL
                                                  END) AS first_promotion_fb_notification_time,
                                              MIN(CASE
                                                    WHEN push_type = 'email' THEN scheduled_at
                                                    ELSE NULL
                                                  END) AS first_promotion_email_time,
                                              MAX(CASE
                                                    WHEN push_type = 'fb_notification' THEN scheduled_at
                                                    ELSE NULL
                                                  END) AS last_promotion_fb_notification_time,
                                              MAX(CASE
                                                    WHEN push_type = 'email' THEN scheduled_at
                                                    ELSE NULL
                                                  END) AS last_promotion_email_time,
                                              MAX(id)  AS max_id
                                       FROM   promotion_push_history
                                       WHERE  status = 'success'
                                              AND user_id IN (SELECT DISTINCT user_id
                                                              FROM   promotion_push_history
                                                              WHERE  id > :max_id)
                                       GROUP  BY user_id
                                       ORDER  BY max_id ASC
                                       """), max_id=config_value)

    result_proxy = with_db_context(db, collection)

    rows = [{
        '_user_id': row['user_id'],
        'first_promotion_fb_notification_time': row['first_promotion_fb_notification_time'],
        'first_promotion_email_time': row['first_promotion_email_time'],
        'last_promotion_fb_notification_time': row['last_promotion_fb_notification_time'],
        'last_promotion_email_time': row['last_promotion_email_time'],
        'max_id': row['max_id']
    } for row in result_proxy]

    if rows:
        new_config_value = rows[-1]['max_id']

        def sync_collection(connection, transaction):
            """ Sync newly added. """
            where = BIUser.__table__.c.user_id == bindparam('_user_id')
            values = {
                'last_promotion_email_time': bindparam('last_promotion_email_time'),
                'last_promotion_fb_notification_time': bindparam('last_promotion_fb_notification_time')
                }

            column_keys = ['first_promotion_fb_notification_time',
                           'first_promotion_email_time']
            column_params = {}
            for column in column_keys:
                column_params[column + '_where'] = and_(
                    BIUser.__table__.c.user_id == bindparam('_user_id'),
                    BIUser.__table__.c[column] == None
                )
                column_params[column + '_values'] = {column: bindparam(column)}

            try:
                connection.execute(BIUser.__table__.update().where(where).values(values), rows)
                for column in column_keys:
                    connection.execute(BIUser.__table__.update().where(column_params[column + '_where']).values(column_params[column + '_values']), rows)
                set_config_value(connection, 'last_imported_promotion_history_id', new_config_value)
            except:
                print('process_user_promotion_time transaction.rollback()')
                transaction.rollback()
                raise
            else:
                print('process_user_promotion_time transaction.commit()')
                transaction.commit()
            return

        with_db_context(db, sync_collection)

    return


@celery.task
def process_bi_user():

    process_user_newly_added_records()
    print('process_user_newly_added_records() done.')

    process_user_newly_updated_records()
    print('process_user_newly_updated_records() done.')

    process_user_billing_info_added_records()
    print('process_user_billing_info_added_records() done.')

    process_user_info_newly_added_records()
    print('process_user_info_newly_added_records() done.')

    process_user_info_newly_updated_records()
    print('process_user_info_newly_updated_records() done.')

    process_user_login_newly_added_records()
    print('process_user_login_newly_added_records() done.')

    process_user_ourgame_newly_added_records()
    print('process_user_ourgame_newly_added_records() done.')

    # process_user_ourgame_newly_updated_records()
    # print('process_user_ourgame_newly_updated_records() done.')

    # process_user_payment_newly_added_records()
    # print('process_user_payment_newly_added_records() done.')

    process_user_payment_spin_purchase_newly_added_records()
    print('process_user_payment_spin_purchase_newly_added_records() done.')

    process_user_payment_spin_purchase_newly_updated_records()
    print('process_user_payment_spin_purchase_newly_updated_records() done.')

    process_user_mall_order_newly_added_records()
    print('process_user_mall_order_newly_added_records() done.')

    process_user_mall_order_newly_updated_records()
    print('process_user_mall_order_newly_updated_records() done.')

    if app.config['ENV'] == 'prod':
        process_user_gold_balance_related_records()
        print('process_user_gold_balance_related_records() done.')

        process_user_silver_balance_related_records()
        print('process_user_silver_balance_related_records() done.')

        process_user_reward_point_related_records()
        print('process_user_reward_point_related_records() done.')

    process_user_promotion_time()
    print('process_user_promotion_time() done.')
