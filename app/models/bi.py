from flask import current_app as app
from sqlalchemy import text
from sqlalchemy.schema import UniqueConstraint, Index

from app.constants import TRANSACTION_TYPES, GOLD_FREE_TRANSACTION_TYPES, SILVER_FREE_TRANSACTION_TYPES
from app.extensions import db
from app.libs.datetime_type import NaiveDateTime, AwareDateTime, OGInsertableAwareDateTime
from app.models.orig_wpt import WPTUserLoginLog
from app.utils import current_time


class BIImportConfig(db.Model):
    __tablename__ = 'bi_import_config'

    var = db.Column(db.String(255), unique=True, primary_key=True)
    value = db.Column(db.String(255))
    last_synced_at = db.Column(AwareDateTime)


class BIStatistic(db.Model):
    __tablename__ = 'bi_statistic'

    id = db.Column(db.Integer, primary_key=True)
    on_day = db.Column(db.Date, nullable=False, index=True)
    game = db.Column(db.String(255), nullable=False, index=True)
    platform = db.Column(db.String(255), nullable=False, index=True)

    new_reg = db.Column(db.Integer, nullable=False, default=0)
    guest_reg = db.Column(db.Integer, nullable=False, default=0)
    facebook_reg = db.Column(db.Integer, nullable=False, default=0)
    facebook_game_reg = db.Column(db.Integer, nullable=False, default=0)
    facebook_login_reg = db.Column(db.Integer, nullable=False, default=0)
    email_reg = db.Column(db.Integer, nullable=False, default=0)
    email_validated = db.Column(db.Integer, nullable=False, default=0)

    mtt_buy_ins = db.Column(db.Integer, nullable=False, default=0)
    ring_game_buy_ins = db.Column(db.Integer, nullable=False, default=0)
    sng_buy_ins = db.Column(db.Integer, nullable=False, default=0)

    mtt_rake = db.Column(db.Integer, nullable=False, default=0)
    sng_rake = db.Column(db.Integer, nullable=False, default=0)
    ring_game_rake = db.Column(db.Integer, nullable=False, default=0)

    mtt_winnings = db.Column(db.Integer, nullable=False, default=0)
    sng_winnings = db.Column(db.Integer, nullable=False, default=0)

    dau = db.Column(db.Integer, nullable=False, default=0)
    wau = db.Column(db.Integer, nullable=False, default=0)
    mau = db.Column(db.Integer, nullable=False, default=0)
    new_reg_game_dau = db.Column(db.Integer, nullable=False, default=0)

    paid_user_count = db.Column(db.Integer, nullable=False, default=0)
    paid_amount = db.Column(db.Float, nullable=False, default=0)
    paid_count = db.Column(db.Integer, nullable=False, default=0)
    revenue = db.Column(db.Float, default=0)

    free_gold = db.Column(db.Integer, nullable=False, default=0)
    free_silver = db.Column(db.Integer, nullable=False, default=0)

    one_day_retention = db.Column(db.Integer, nullable=False, default=0)
    seven_day_retention = db.Column(db.Integer, nullable=False, default=0)
    thirty_day_retention = db.Column(db.Integer, nullable=False, default=0)


    # count_of_masterpoint_exchanged_for_gold = db.Column(db.Integer, default=0)
    # amount_of_masterpoint_exchanged_for_gold = db.Column(db.Float, default=0)
    # count_of_dollar_exchanged_for_gold = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_gold = db.Column(db.Float, default=0)
    # count_of_gold_exchanged_for_silver = db.Column(db.Integer, default=0)
    # amount_of_gold_exchanged_for_silver = db.Column(db.Float, default=0)
    # count_of_dollar_exchanged_for_silver = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_silver = db.Column(db.Float, default=0)
    # count_of_dollar_exchanged_for_lucky_spin = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_lucky_spin = db.Column(db.Float, default=0)
    # count_of_gold_exchanged_for_lucky_charm = db.Column(db.Integer, default=0)
    # amount_of_gold_exchanged_for_lucky_charm = db.Column(db.Float, default=0)
    # count_of_gold_exchanged_for_avatar = db.Column(db.Integer, default=0)
    # amount_of_gold_exchanged_for_avatar = db.Column(db.Float, default=0)
    # count_of_gold_exchanged_for_emoji = db.Column(db.Integer, default=0)
    # amount_of_gold_exchanged_for_emoji = db.Column(db.Float, default=0)

    # count_of_dollar_exchanged_for_spin_purchase = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_spin_purchase = db.Column(db.Float, default=0)

    # count_of_dollar_exchanged_for_spin_ticket = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_spin_ticket = db.Column(db.Float, default=0)
    # count_of_dollar_exchanged_for_spin_booster = db.Column(db.Integer, default=0)
    # amount_of_dollar_exchanged_for_spin_booster = db.Column(db.Float, default=0)


class BIUser(db.Model):
    __tablename__ = 'bi_user'

    id = db.Column(db.BIGINT, primary_key=True)
    user_id = db.Column(db.BIGINT, unique=True, nullable=False, index=True)
    username = db.Column(db.String(255), index=True)
    og_account = db.Column(db.String(255), index=True)
    facebook_id = db.Column(db.String(255))
    email = db.Column(db.String(255), index=True)
    email_validate_time = db.Column(OGInsertableAwareDateTime, index=True)
    email_promotion_allowed = db.Column(db.Boolean)

    account_status = db.Column(db.String(255))
    account_status_orig = db.Column(db.Integer)
    last_login_ip = db.Column(db.String(255))
    # last_login_ip_location = db.Column(db.String(255))
    last_login_time = db.Column(OGInsertableAwareDateTime)

    reg_ip = db.Column(db.String(255))
    # reg_ip_location = db.Column(db.String(255))
    reg_time = db.Column(OGInsertableAwareDateTime, index=True)
    reg_source = db.Column(db.String(255), index=True)

    reg_platform = db.Column(db.String(255), index=True)
    reg_facebook_connect = db.Column(db.Boolean)

    reg_type_orig = db.Column(db.Integer)
    reg_platform_orig = db.Column(db.Integer)
    reg_device_orig = db.Column(db.Integer)
    reg_affiliate = db.Column(db.String(255))
    reg_affiliate_orig = db.Column(db.Integer)
    reg_campaign = db.Column(db.String(255))
    reg_campaign_orig = db.Column(db.Integer)

    reg_country = db.Column(db.String(255))
    reg_state = db.Column(db.String(255))
    reg_city = db.Column(db.String(255))

    first_name = db.Column(db.String(255))
    middle_name = db.Column(db.String(255))
    last_name = db.Column(db.String(255))
    address = db.Column(db.String(255))
    city = db.Column(db.String(255))
    state = db.Column(db.String(255))
    country = db.Column(db.String(255))
    zip_code = db.Column(db.String(255))
    phone = db.Column(db.String(255))
    birthday = db.Column(NaiveDateTime)
    gender = db.Column(db.String(255))
    gender_orig = db.Column(db.Integer)

    billing_contact = db.Column(db.String(255))
    billing_address = db.Column(db.String(255))
    billing_city = db.Column(db.String(255))
    billing_state = db.Column(db.String(255))
    billing_country = db.Column(db.String(255))
    billing_zip_code = db.Column(db.String(255))

    gold_balance = db.Column(db.Integer, index=True)
    silver_balance = db.Column(db.Integer, index=True)
    first_poker_time = db.Column(OGInsertableAwareDateTime)
    first_slots_time = db.Column(OGInsertableAwareDateTime)
    last_poker_time = db.Column(OGInsertableAwareDateTime, index=True)
    last_slots_time = db.Column(OGInsertableAwareDateTime, index=True)

    first_promotion_fb_notification_time = db.Column(AwareDateTime)
    first_promotion_email_time = db.Column(AwareDateTime)
    last_promotion_fb_notification_time = db.Column(AwareDateTime)
    last_promotion_email_time = db.Column(AwareDateTime)

    first_free_spin_time = db.Column(OGInsertableAwareDateTime)
    last_free_spin_time = db.Column(OGInsertableAwareDateTime, index=True)
    last_premium_spin_time = db.Column(OGInsertableAwareDateTime, index=True)

    reward_level = db.Column(db.Integer)
    reward_xp = db.Column(db.Integer)
    reward_point = db.Column(db.Integer)

    dollar_paid_amount = db.Column(db.Float, index=True)
    dollar_paid_count = db.Column(db.Integer)

    count_of_masterpoint_exchanged_for_gold = db.Column(db.Integer)
    amount_of_masterpoint_exchanged_for_gold = db.Column(db.Float)
    first_time_of_masterpoint_exchanged_for_gold = db.Column(OGInsertableAwareDateTime)
    last_time_of_masterpoint_exchanged_for_gold = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_gold = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_gold = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_gold = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_gold = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_gold_exchanged_for_silver = db.Column(db.Integer)
    amount_of_gold_exchanged_for_silver = db.Column(db.Float)
    first_time_of_gold_exchanged_for_silver = db.Column(OGInsertableAwareDateTime)
    last_time_of_gold_exchanged_for_silver = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_silver = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_silver = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_silver = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_silver = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_lucky_spin = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_lucky_spin = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_lucky_spin = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_lucky_spin = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_gold_exchanged_for_lucky_charm = db.Column(db.Integer)
    amount_of_gold_exchanged_for_lucky_charm = db.Column(db.Float)
    first_time_of_gold_exchanged_for_lucky_charm = db.Column(OGInsertableAwareDateTime)
    last_time_of_gold_exchanged_for_lucky_charm = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_gold_exchanged_for_avatar = db.Column(db.Integer)
    amount_of_gold_exchanged_for_avatar = db.Column(db.Float)
    first_time_of_gold_exchanged_for_avatar = db.Column(OGInsertableAwareDateTime)
    last_time_of_gold_exchanged_for_avatar = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_gold_exchanged_for_emoji = db.Column(db.Integer)
    amount_of_gold_exchanged_for_emoji = db.Column(db.Float)
    first_time_of_gold_exchanged_for_emoji = db.Column(OGInsertableAwareDateTime)
    last_time_of_gold_exchanged_for_emoji = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_spin_purchase = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_purchase = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_spin_purchase = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_spin_purchase = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_spin_ticket = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_ticket = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_spin_ticket = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_spin_ticket = db.Column(OGInsertableAwareDateTime, index=True)

    count_of_dollar_exchanged_for_spin_booster = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_booster = db.Column(db.Float)
    first_time_of_dollar_exchanged_for_spin_booster = db.Column(OGInsertableAwareDateTime)
    last_time_of_dollar_exchanged_for_spin_booster = db.Column(OGInsertableAwareDateTime, index=True)

    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)

    def attribute_pairs(self):
        return [[column.key.replace('_', ' ').title(), self.__dict__[column.key] or ''] for column in
                self.__table__.columns]

    def login_logs(self, limit=5):
        return WPTUserLoginLog.query.filter_by(user_id=self.user_id).order_by(text('id DESC')).limit(limit).all()

    def gold_activities(self, limit=5):
        return BIUserCurrency.query.filter_by(currency_type='Gold', user_id=self.user_id).order_by(
            text('created_at DESC')).limit(limit).all()

    def silver_activities(self, limit=5):
        return BIUserCurrency.query.filter_by(currency_type='Silver', user_id=self.user_id).order_by(
            text('created_at DESC')).limit(limit).all()

    def gold_free_currency(self, limit=5):
        return BIUserCurrency.query.filter_by(currency_type='Gold', user_id=self.user_id).filter(
            BIUserCurrency.transaction_type.in_(GOLD_FREE_TRANSACTION_TYPES)).order_by(text('created_at DESC')).limit(
            limit).all()

    def silver_free_currency(self, limit=5):
        return BIUserCurrency.query.filter_by(currency_type='Silver', user_id=self.user_id).filter(
            BIUserCurrency.transaction_type.in_(SILVER_FREE_TRANSACTION_TYPES)).order_by(text('created_at DESC')).limit(
            limit).all()

    def __repr__(self):
        return '<BITUser %r>' % self.email


class BIUserCurrency(db.Model):
    __tablename__ = 'bi_user_currency'

    id = db.Column(db.BIGINT, primary_key=True)
    orig_id = db.Column(db.BIGINT, nullable=False, index=True)
    user_id = db.Column(db.BIGINT, nullable=False, index=True)
    og_account = db.Column(db.String(255), nullable=False, index=True)
    game_id = db.Column(db.Integer, nullable=False, index=True)
    currency_type = db.Column(db.String(50), nullable=False, index=True)
    transaction_type = db.Column(db.Integer, nullable=False, index=True)
    transaction_amount = db.Column(db.BIGINT)
    balance = db.Column(db.BIGINT)
    user_id_updated = db.Column(db.Boolean, nullable=False, default=False, index=True)
    created_at = db.Column(OGInsertableAwareDateTime, nullable=False, default=current_time, index=True)

    # __table_args__ = (UniqueConstraint('currency_type', 'orig_id', name='ix_uniq_currency_type_and_orig_id'),)
    __table_args__ = (UniqueConstraint('currency_type', 'orig_id', name='ix_uniq_currency_type_and_orig_id'),
                      Index('ix_og_account_and_user_id_updated', 'og_account', 'user_id_updated'),)

    def transaction_type_display(self):
        value = TRANSACTION_TYPES[self.transaction_type]
        return '%s %s' % (self.transaction_type, value) if value is not None else self.transaction_type


class BIUserBill(db.Model):
    __tablename__ = 'bi_user_bill'

    id = db.Column(db.BIGINT, primary_key=True)

    orig_id = db.Column(db.String(255), nullable=False, index=True)
    orig_db = db.Column(db.String(50), nullable=False, index=True)

    user_id = db.Column(db.BIGINT, nullable=False, index=True)
    game_id = db.Column(db.Integer, nullable=False, index=True)

    platform = db.Column(db.String(255), nullable=False, index=True)
    platform_orig = db.Column(db.Integer, nullable=False, index=True)

    currency_type = db.Column(db.String(255), nullable=False, index=True)
    currency_type_orig = db.Column(db.Integer, nullable=False, index=True)

    currency_amount = db.Column(db.Float, nullable=False, default=0)

    # category = db.Column(db.String(255), nullable=False)
    # category_orig = db.Column(db.Integer)

    # product = db.Column(db.String(255), nullable=False)
    # product_orig = db.Column(db.Integer, nullable=False)

    goods = db.Column(db.String(255))
    goods_orig = db.Column(db.Integer, nullable=False)

    is_promotion = db.Column(db.Boolean)

    quantity = db.Column(db.Integer, nullable=False)

    created_at = db.Column(OGInsertableAwareDateTime, nullable=False, default=current_time, index=True)

    __table_args__ = (
        UniqueConstraint('orig_db', 'orig_id', 'goods_orig', name='ix_uniq_orig_db_and_orig_id_and_goods_orig'),)

    def bill_detail_products(self):
        return BIUserBillDetail.query.filter_by(orig_db=self.orig_db, orig_id=self.orig_id).all()


class BIUserBillDetail(db.Model):
    __tablename__ = 'bi_user_bill_detail'

    id = db.Column(db.BIGINT, primary_key=True)

    orig_id = db.Column(db.String(255), nullable=False, index=True)
    orig_db = db.Column(db.String(50), nullable=False, index=True)

    user_id = db.Column(db.BIGINT, nullable=False, index=True)
    game_id = db.Column(db.Integer, nullable=False, index=True)

    platform = db.Column(db.String(255), nullable=False, index=True)
    platform_orig = db.Column(db.Integer, nullable=False, index=True)

    currency_type = db.Column(db.String(255), nullable=False, index=True)
    currency_type_orig = db.Column(db.Integer, nullable=False, index=True)

    currency_amount = db.Column(db.Float, nullable=False, default=0)

    category = db.Column(db.String(255), nullable=False)
    category_orig = db.Column(db.Integer)

    product = db.Column(db.String(255), nullable=False)
    product_orig = db.Column(db.Integer, nullable=False)

    goods = db.Column(db.String(255))
    goods_orig = db.Column(db.Integer, nullable=False)

    is_promotion = db.Column(db.Boolean)

    quantity = db.Column(db.Integer, nullable=False)

    created_at = db.Column(OGInsertableAwareDateTime, nullable=False, default=current_time, index=True)

    __table_args__ = (
        UniqueConstraint('orig_db', 'orig_id', 'product_orig', name='ix_uniq_orig_db_and_orig_id_and_product_orig'),)


class BIUserStatistic(object):
    _mapper = {}

    @staticmethod
    def model(stats_date):

        from app.tasks import with_db_context

        table_index = stats_date
        class_name = 'BIUserStatistic_{}'.format(table_index)

        ModelClass = BIUserStatistic._mapper.get(class_name, None)
        if ModelClass is None:
            ModelClass = type(class_name, (db.Model,), {
                '__module__': __name__,
                '__name__': class_name,
                '__tablename__': 'bi_user_statistic_{}'.format(table_index),
                'id': db.Column(db.BIGINT, primary_key=True),
                'user_id': db.Column(db.BIGINT, index=True),
                'og_account': db.Column(db.BIGINT, index=True),
                'user_name': db.Column(db.String(255), nullable=False),
                'stats_date': db.Column(db.Date, index=True, default=stats_date),

                'new_reg': db.Column(db.Boolean, default=False),
                'sng_dau': db.Column(db.Boolean, default=False),
                'mtt_dau': db.Column(db.Boolean, default=False),
                'ring_dau': db.Column(db.Boolean, default=False),

                'slots_dau': db.Column(db.Boolean, default=False),
                'store_dau': db.Column(db.Boolean, default=False),

                'ring_rake': db.Column(db.Integer, nullable=False, default=0),
                'ring_hands': db.Column(db.Integer, nullable=False, default=0),

                'sng_entries': db.Column(db.Integer, nullable=False, default=0),
                'mtt_entries': db.Column(db.Integer, nullable=False, default=0),

                'mtt_rebuy_value': db.Column(db.Integer, nullable=False, default=0),
                'mtt_rebuy_count': db.Column(db.Integer, nullable=False, default=0),

                'sng_rake': db.Column(db.Integer, nullable=False, default=0),
                'mtt_rake': db.Column(db.Integer, nullable=False, default=0),

                'mtt_buyins': db.Column(db.Integer, nullable=False, default=0),
                'sng_buyins': db.Column(db.Integer, nullable=False, default=0),

                'sng_winnings': db.Column(db.Integer, nullable=False, default=0),
                'mtt_winnings': db.Column(db.Integer, nullable=False, default=0),
            })
            BIUserStatistic._mapper[class_name] = ModelClass

            with app.app_context():
                cls = ModelClass()
                cls.__table__.create(db.engine, checkfirst=True)

            def sync_all_users(connection, transaction):
                users = [dict(zip(('user_id', 'user_name', 'og_account'), u)) for u in
                         db.session.query(BIUser.user_id, BIUser.username, BIUser.og_account).all()]
                try:

                    connection.execute(ModelClass.__table__.insert(), users)

                except:

                    print(stats_date, 'users transaction.rollback()')

                    transaction.rollback()

                    raise

                else:

                    transaction.commit()

                print(stats_date, 'users transaction.commit()')

            with_db_context(db, sync_all_users)

            return ModelClass

        return ModelClass


class BIClubWPTUser(db.Model):
    __tablename__ = 'bi_clubwpt_user'

    id = db.Column(db.BIGINT, primary_key=True)

    orig_user_id = db.Column(db.Integer, nullable=False, index=True)

    email = db.Column(db.String(255), index=True)
    orig_email = db.Column(db.String(255), index=True)

    username = db.Column(db.String(255), index=True)
    orig_username = db.Column(db.String(255), index=True)

    gold_balance = db.Column(db.Integer, index=True)

    exchanged_at = db.Column(OGInsertableAwareDateTime, index=True)
    exchanged_user_id = db.Column(db.BIGINT, index=True)
