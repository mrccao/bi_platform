from sqlalchemy import text
from sqlalchemy.schema import UniqueConstraint

from app.constants import TRANSACTION_TYPES, BI_USER_SORTED_COLUMNS, GOLD_FREE_TRANSACTION_TYPES, \
    SILVER_FREE_TRANSACTION_TYPES
from app.extensions import db
from app.libs.datetime_type import NaiveDateTime, AwareDateTime, OGInsertableAwareDateTime
from app.models.orig_wpt import WPTUserLoginLog
from app.utils import current_time


class BIImportConfig(db.Model):
    __tablename__ = 'bi_import_config'

    var = db.Column(db.String(255), unique=True, primary_key=True)
    value = db.Column(db.String(255))
    last_synced_at = db.Column(AwareDateTime, onupdate=current_time)


class BIStatistic(db.Model):
    __tablename__ = 'bi_statistic'

    id = db.Column(db.Integer, primary_key=True)
    on_day = db.Column(NaiveDateTime, nullable=False, index=True)
    game = db.Column(db.String(255), nullable=False, index=True)
    platform = db.Column(db.String(255), nullable=False, index=True)

    new_registration = db.Column(db.Integer, default=0)
    dau = db.Column(db.Integer, default=0)
    wau = db.Column(db.Integer, default=0)
    mau = db.Column(db.Integer, default=0)
    new_registration_game_dau = db.Column(db.Integer, default=0)

    dollar_paid_user_count = db.Column(db.Integer, default=0)

    dollar_paid_amount = db.Column(db.Float, default=0)
    dollar_paid_count = db.Column(db.Integer, default=0)

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
    first_name = db.Column(db.String(255))
    middle_name = db.Column(db.String(255))
    last_name = db.Column(db.String(255))
    address = db.Column(db.String(255))
    city = db.Column(db.String(255))
    state = db.Column(db.String(255))
    country = db.Column(db.String(255))
    zip_code = db.Column(db.String(255))

    billing_contact = db.Column(db.String(255))
    billing_address = db.Column(db.String(255))
    billing_city = db.Column(db.String(255))
    billing_state = db.Column(db.String(255))
    billing_country = db.Column(db.String(255))
    billing_zip_code = db.Column(db.String(255))

    phone = db.Column(db.String(255))
    email = db.Column(db.String(255), index=True)
    email_validate_time = db.Column(OGInsertableAwareDateTime, index=True)
    email_promotion_allowed = db.Column(db.Boolean)
    birthday = db.Column(NaiveDateTime)
    reg_ip = db.Column(db.String(255))
    # reg_ip_location = db.Column(db.String(255))
    reg_time = db.Column(OGInsertableAwareDateTime, index=True)
    reg_source = db.Column(db.String(255), index=True)

    reg_platform = db.Column(db.String(255), index=True)
    reg_facebook_connect = db.Column(db.Boolean)

    facebook_id = db.Column(db.String(255))

    reg_type_orig = db.Column(db.Integer)
    reg_platform_orig = db.Column(db.Integer)
    reg_device_orig = db.Column(db.Integer)
    reg_affiliate = db.Column(db.String(255))
    reg_affiliate_orig = db.Column(db.Integer)
    reg_campaign = db.Column(db.String(255))
    reg_campaign_orig = db.Column(db.Integer)
    last_login_ip = db.Column(db.String(255))
    # last_login_ip_location = db.Column(db.String(255))
    last_login_time = db.Column(OGInsertableAwareDateTime)
    gender = db.Column(db.String(255))
    gender_orig = db.Column(db.Integer)
    account_status = db.Column(db.String(255))
    account_status_orig = db.Column(db.Integer)

    gold_balance = db.Column(db.Integer, index=True)
    silver_balance = db.Column(db.Integer, index=True)
    first_poker_time = db.Column(OGInsertableAwareDateTime)
    first_slots_time = db.Column(OGInsertableAwareDateTime)
    last_poker_time = db.Column(OGInsertableAwareDateTime, index=True)
    last_slots_time = db.Column(OGInsertableAwareDateTime, index=True)

    reward_level = db.Column(db.Integer)
    reward_xp = db.Column(db.Integer)
    reward_point = db.Column(db.Integer)

    dollar_paid_amount = db.Column(db.Float, index=True)
    dollar_paid_count = db.Column(db.Integer)

    count_of_masterpoint_exchanged_for_gold = db.Column(db.Integer)
    amount_of_masterpoint_exchanged_for_gold = db.Column(db.Float)
    count_of_dollar_exchanged_for_gold = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_gold = db.Column(db.Float)
    count_of_gold_exchanged_for_silver = db.Column(db.Integer)
    amount_of_gold_exchanged_for_silver = db.Column(db.Float)
    count_of_dollar_exchanged_for_silver = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_silver = db.Column(db.Float)
    count_of_dollar_exchanged_for_lucky_spin = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_lucky_spin = db.Column(db.Float)
    count_of_gold_exchanged_for_lucky_charm = db.Column(db.Integer)
    amount_of_gold_exchanged_for_lucky_charm = db.Column(db.Float)
    count_of_gold_exchanged_for_avatar = db.Column(db.Integer)
    amount_of_gold_exchanged_for_avatar = db.Column(db.Float)
    count_of_gold_exchanged_for_emoji = db.Column(db.Integer)
    amount_of_gold_exchanged_for_emoji = db.Column(db.Float)

    count_of_dollar_exchanged_for_spin_purchase = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_purchase = db.Column(db.Float)

    count_of_dollar_exchanged_for_spin_ticket = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_ticket = db.Column(db.Float)
    count_of_dollar_exchanged_for_spin_booster = db.Column(db.Integer)
    amount_of_dollar_exchanged_for_spin_booster = db.Column(db.Float)

    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)

    def attribute_pairs(self):
        return [[column.replace('_', ' ').title(), self.__dict__[column] or ''] for column in BI_USER_SORTED_COLUMNS]
        # return { column.replace('_', ' ').title(): self.__dict__[column] for column in sorted(self.__dict__) if not column.startswith('_sa_') }

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
    transaction_amount = db.Column(db.BIGINT, nullable=False, index=True)
    balance = db.Column(db.BIGINT, nullable=False, index=True)
    created_at = db.Column(OGInsertableAwareDateTime, nullable=False, default=current_time, index=True)

    __table_args__ = (UniqueConstraint('currency_type', 'orig_id', name='ix_uniq_currency_type_and_orig_id'),)

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

    currency_amount = db.Column(db.Float, nullable=False)

    category = db.Column(db.String(255), nullable=False)
    category_orig = db.Column(db.Integer)

    product = db.Column(db.String(255), nullable=False)
    product_orig = db.Column(db.Integer, nullable=False)

    goods = db.Column(db.String(255), nullable=False)
    goods_orig = db.Column(db.Integer, nullable=False)

    quantity = db.Column(db.Integer, nullable=False)

    created_at = db.Column(OGInsertableAwareDateTime, nullable=False, default=current_time, index=True)

    __table_args__ = (UniqueConstraint('orig_db', 'orig_id', name='ix_uniq_orig_db_and_orig_id'),)


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
