from datetime import date

import os
import pandas as pd
from flask_migrate import MigrateCommand
from flask_script import Manager, Server
from flask_script.commands import ShowUrls, Clean
from random import choice

from app import create_app
from app.constants import TRANSACTION_TYPES
from app.extensions import db
from app.models.bi import BIImportConfig, BIStatistic, BIUser, BIUserCurrency, BIUserBill, BIClubWPTUser, \
    BIUserStatistic
from app.models.main import AdminUser, AdminUserActivity, AdminUserQuery
from app.models.promotion import PromotionPush, PromotionPushHistory
from app.tasks.bi_clubwpt_user import process_bi_clubwpt_user
from app.tasks.bi_statistic import process_bi_statistic
from app.tasks.bi_user import process_bi_user
from app.tasks.bi_user_bill import process_bi_user_bill
from app.tasks.bi_user_currency import process_bi_user_currency
from app.tasks.bi_user_statistic import process_bi_user_statistic
from app.tasks.promotion import process_promotion_facebook_notification, process_promotion_email
from app.tasks.scheduled import process_bi

# default to dev config because no one should use this in
# production anyway
env = os.environ.get('WPT_DWH_ENV', 'dev')
app = create_app('app.settings.%sConfig' % env.capitalize())

manager = Manager(app)
manager.add_command("server", Server(host='0.0.0.0'))
manager.add_command("show-urls", ShowUrls())
manager.add_command("clean", Clean())
manager.add_command('db', MigrateCommand)


@manager.shell
def make_shell_context():
    """ Creates a python REPL with several default imports
        in the context of the app
    """

    return dict(app=app, db=db, AdminUser=AdminUser)


def init_bi_user_import_config():
    """ Init BIImportConfig Table """

    BIImportConfig.__table__.create(db.engine, checkfirst=True)

    variables = ['last_imported_user_id',
                 'last_imported_user_update_time',
                 'last_imported_user_billing_info_id',
                 'last_imported_user_info_add_time',
                 'last_imported_user_info_update_time',
                 'last_imported_user_login_time',
                 'last_imported_user_ourgame_add_time',
                 'last_imported_user_ourgame_update_time',
                 'last_imported_user_payment_spin_purchase_add_time',
                 'last_imported_user_payment_spin_purchase_update_time',
                 'last_imported_user_mall_order_add_time',
                 'last_imported_user_mall_order_update_time',
                 'last_imported_og_powergamecoin_add_time',
                 'last_imported_og_gamecoin_add_time',
                 'last_imported_promotion_history_id']

    for v in variables:
        db.session.query(BIImportConfig).filter_by(var=v).delete()
        db.session.add(BIImportConfig(var=v))

    db.session.commit()


def init_bi_user_bill_import_config():
    """ Init BIImportConfig Table """

    BIImportConfig.__table__.create(db.engine, checkfirst=True)

    variables = ['last_imported_user_bill_dollar_paid_add_time',
                 'last_imported_user_mall_bill_order_id',
                 'last_imported_user_mall_bill_order_update_time',
                 'last_imported_user_payment_bill_order_id']

    for v in variables:
        db.session.query(BIImportConfig).filter_by(var=v).delete()
        db.session.add(BIImportConfig(var=v))

    db.session.commit()


def init_bi_user_currency_import_config():
    """ Init BIImportConfig Table """

    BIImportConfig.__table__.create(db.engine, checkfirst=True)

    variables = ['last_imported_user_gold_currency_add_time',
                 'last_imported_user_silver_currency_add_time']

    for v in variables:
        db.session.query(BIImportConfig).filter_by(var=v).delete()
        db.session.add(BIImportConfig(var=v))

    db.session.commit()


def init_bi_clubwpt_user_import_config():
    """ Init BIImportConfig Table """

    BIImportConfig.__table__.create(db.engine, checkfirst=True)

    variables = ['last_imported_clubwpt_user_add_time',
                 'last_imported_clubwpt_user_update_time']

    for v in variables:
        db.session.query(BIImportConfig).filter_by(var=v).delete()
        db.session.add(BIImportConfig(var=v))

    db.session.commit()


@manager.command
def reset_bi():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        db.drop_all(bind=None)
        db.create_all(bind=None)

        init_bi_user_import_config()
        init_bi_user_bill_import_config()
        init_bi_user_currency_import_config()
        init_bi_clubwpt_user_import_config()

        user = AdminUser(email='admin@admin.com', password='password', timezone='EST')
        user.name = "Test Account"
        db.session.add(user)

        user = AdminUser(email='admin1@admin.com', password='password', timezone='EST')
        user.name = "Test1 Account"
        db.session.add(user)

        db.session.commit()


@manager.command
def reset_bi_admin():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        AdminUserActivity.__table__.drop(db.engine, checkfirst=True)
        AdminUserQuery.__table__.drop(db.engine, checkfirst=True)

        AdminUserActivity.__table__.create(db.engine, checkfirst=True)
        AdminUserQuery.__table__.create(db.engine, checkfirst=True)


@manager.command
def reset_bi_promotion():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        PromotionPush.__table__.drop(db.engine, checkfirst=True)
        PromotionPushHistory.__table__.drop(db.engine, checkfirst=True)

        PromotionPush.__table__.create(db.engine, checkfirst=True)
        PromotionPushHistory.__table__.create(db.engine, checkfirst=True)


@manager.command
def reset_bi_user():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        BIUser.__table__.drop(db.engine, checkfirst=True)

        BIUser.__table__.create(db.engine, checkfirst=True)

        init_bi_user_import_config()


@manager.command
def reset_bi_user_bill():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        BIUserBill.__table__.drop(db.engine, checkfirst=True)

        BIUserBill.__table__.create(db.engine, checkfirst=True)

        init_bi_user_bill_import_config()


@manager.command
def reset_bi_user_currency():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        BIUserCurrency.__table__.drop(db.engine, checkfirst=True)

        BIUserCurrency.__table__.create(db.engine, checkfirst=True)

        init_bi_user_currency_import_config()


@manager.command
def reset_bi_clubwpt_user():
    """ ReCreate Database and Seed """

    answer = input("Do you want? (yes/no) ")
    if answer == 'yes':
        BIClubWPTUser.__table__.drop(db.engine, checkfirst=True)

        BIClubWPTUser.__table__.create(db.engine, checkfirst=True)

        init_bi_clubwpt_user_import_config()


@manager.command
def reset_bi_statistic():
    """ ReCreate Database and Seed """

    BIStatistic.__table__.drop(db.engine, checkfirst=True)

    BIStatistic.__table__.create(db.engine, checkfirst=True)

    from datetime import date
    import pandas as pd
    for day in pd.date_range(date(2016, 6, 1), date(2017, 12, 31)):
        for game in ['All Game', 'TexasPoker', 'TimeSlots']:
            for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Web Mobile', 'Facebook Game']:
                # new_reg = randrange(1000, 2000)
                # dau = randrange(2000)
                # mau = randrange(4000)
                # wau = randrange(3000)
                # new_reg_game_dau = randrange(1000)
                db.session.add(BIStatistic(on_day=day.strftime("%Y-%m-%d"), game=game, platform=platform))
                # new_reg=new_reg,
                # dau=dau, mau=mau, wau=wau,
                # new_reg_game_dau=new_reg_game_dau))
    db.session.commit()


@manager.command
def reset_bi_user_statistic():
    """ ReCreate Database and Seed """

    BIUserStatistic.__table__.drop(db.engine, checkfirst=True)

    BIUserStatistic.__table__.create(db.engine, checkfirst=True)

    from datetime import date
    import pandas as pd
    for day in pd.date_range(date(2016, 6, 1), date(2017, 12, 31)):
        for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Web Mobile', 'Facebook Game']:
            db.session.add(BIUserStatistic(on_day=day.strftime("%Y-%m-%d"), platform=platform))
    db.session.commit()


# @manager.command
# def reset_bi_statistic():
#     """ ReCreate Database and Seed """
#
#     answer = input("Do you want? (yes/no) ")
#     if answer == 'yes':
#
#         BIStatistic.__table__.drop(db.engine, checkfirst=True)
#
#         BIStatistic.__table__.create(db.engine, checkfirst=True)
#
#         from datetime import date
#         import pandas as pd
#         for day in pd.date_range(date(2016, 6, 1), date(2017, 12, 31)):
#             for game in ['All Game', 'TexasPoker', 'TimeSlots']:
#                 for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Web Mobile', 'Facebook Game']:
#                     db.session.add(BIStatistic(on_day=day.strftime("%Y-%m-%d"), game=game, platform=platform))
#         db.session.commit()


@manager.command
def sync_bi():
    if app.config['ENV'] == 'prod':
        process_bi.delay()
    else:
        process_bi()


@manager.command
def sync_bi_user():
    if app.config['ENV'] == 'prod':
        process_bi_user.delay()
    else:
        process_bi_user()


@manager.command
def sync_bi_user_bill():
    if app.config['ENV'] == 'prod':
        process_bi_user_bill.delay()
    else:
        process_bi_user_bill()


@manager.command
def sync_bi_user_currency():
    if app.config['ENV'] == 'prod':
        process_bi_user_currency.delay()
    else:
        process_bi_user_currency()


@manager.command
def sync_bi_clubwpt_user():
    if app.config['ENV'] == 'prod':
        process_bi_clubwpt_user.delay()
    else:
        process_bi_clubwpt_user()


@manager.command
def sync_bi_statistic_for_lifetime():
    if app.config['ENV'] == 'prod':
        process_bi_statistic.delay('lifetime')
    else:
        process_bi_statistic('lifetime')


@manager.command
def sync_bi_statistic_for_yesterday():
    if app.config['ENV'] == 'prod':
        process_bi_statistic.delay('yesterday')
    else:
        process_bi_statistic('yesterday')


@manager.command
def sync_bi_statistic_for_today():
    if app.config['ENV'] == 'prod':
        process_bi_statistic.delay('today')
    else:
        process_bi_statistic('today')


@manager.command
def sync_bi_user_statistic_for_lifetime():
    if app.config['ENV'] == 'prod':
        process_bi_user_statistic.delay('lifetime')
    else:
        process_bi_user_statistic('lifetime')


@manager.command
def sync_bi_user_statistic_for_yesterday():
    if app.config['ENV'] == 'prod':
        process_bi_user_statistic.delay('yesterday')
    else:
        process_bi_user_statistic('yesterday')


@manager.command
def sync_bi_user_statistic_for_today():
    if app.config['ENV'] == 'prod':
        process_bi_user_statistic.delay('today')
    else:
        process_bi_user_statistic('today')


@manager.command
def sync_bi_statistic_for_someday(target):
    if app.config['ENV'] == 'prod':
        process_bi_statistic.delay(target)
    else:
        process_bi_statistic(target)


@manager.command
def process_promotion_push():
    if app.config['ENV'] == 'prod':
        process_promotion_facebook_notification.delay()
        process_promotion_email.delay()
    else:
        process_promotion_facebook_notification()
        process_promotion_email()


@manager.command
def create_bi_user_currency_mock_data():
    game_id_list = [0, 22083, 23118, 38880, 39990]
    transaction_type_list = list(TRANSACTION_TYPES)
    currency_type_list = ['Gold', 'Silver']

    user_id_list = db.session.query(BIUser.user_id).all()
    day_range_1 = pd.date_range(date(2016, 6, 1), date(2016, 8, 30))
    day_range_2 = pd.date_range(date(2016, 8, 30), date(2016, 12, 30))
    day_range_3 = pd.date_range(date(2016, 12, 30), date(2017, 4, 30))

    transaction_amount_list = range(10000)

    for i in range(1000):
        created_at = choice(day_range_1)
        user_id_tuple = choice(user_id_list)
        user_id = user_id_tuple[0]
        currency_type = choice(currency_type_list)
        transaction_type = choice(transaction_type_list)
        transaction_amount = choice(transaction_amount_list)
        game_id = choice(game_id_list)

        db.session.add(BIUserCurrency(created_at=created_at, game_id=game_id, currency_type=currency_type,
                                      user_id=user_id,
                                      transaction_type=transaction_type, transaction_amount=transaction_amount))
    db.session.commit()

    for i in range(2000):
        created_at = choice(day_range_2)
        user_id_tuple = choice(user_id_list)
        user_id = user_id_tuple[0]
        currency_type = choice(currency_type_list)
        transaction_type = choice(transaction_type_list)
        transaction_amount = choice(transaction_amount_list)
        game_id = choice(game_id_list)

        db.session.add(BIUserCurrency(created_at=created_at, game_id=game_id, currency_type=currency_type,
                                      user_id=user_id,
                                      transaction_type=transaction_type, transaction_amount=transaction_amount))
    db.session.commit()

    for i in range(3000):
        created_at = choice(day_range_3)
        user_id_tuple = choice(user_id_list)
        user_id = user_id_tuple[0]
        currency_type = choice(currency_type_list)
        transaction_type = choice(transaction_type_list)
        transaction_amount = choice(transaction_amount_list)
        game_id = choice(game_id_list)

        db.session.add(BIUserCurrency(created_at=created_at, game_id=game_id, currency_type=currency_type,
                                      user_id=user_id,
                                      transaction_type=transaction_type, transaction_amount=transaction_amount))
    db.session.commit()


if __name__ == "__main__":
    manager.run()
