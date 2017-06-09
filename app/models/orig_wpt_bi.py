from app.extensions import db
from app.libs.datetime_type import UTCToESTDateTime


class WPTBIUserStatistic(db.Model):
    __bind_key__ = 'orig_wpt_bi'
    __tablename__ = 'user_statistic'

    user_id = db.Column('user_id', db.BIGINT, primary_key=True)
    reg_time = db.Column('reg_time', UTCToESTDateTime)
    reward_level = db.Column('reward_level', db.Integer)
    gold_balance = db.Column('gold_balance', db.BIGINT)
    silver_balance = db.Column('silver_balance', db.BIGINT)
    dollar_paid_amount = db.Column(db.Numeric(9,2))
    dollar_paid_count = db.Column('dollar_paid_count', db.Integer)
    last_purchase_gold_time = db.Column('last_purchase_gold_time', UTCToESTDateTime)
    count_of_gold_purchase = db.Column('count_of_gold_purchase', db.Integer)
    last_purchase_avatar_time = db.Column('last_purchase_avatar_time', UTCToESTDateTime)
    count_of_avatar_purchase = db.Column('count_of_avatar_purchase', db.Integer)
    last_purchase_charms_time = db.Column('last_purchase_charms_time', UTCToESTDateTime)
    count_of_charms_purchase = db.Column('count_of_charms_purchase', db.Integer)
    last_free_spin_time = db.Column('last_free_spin_time', UTCToESTDateTime)
    last_premium_spin_time = db.Column('last_premium_spin_time', UTCToESTDateTime)
    last_poker_time = db.Column('last_poker_time', UTCToESTDateTime)
    last_slots_time = db.Column('last_slots_time', UTCToESTDateTime)
    updated_at = db.Column('updated_at', UTCToESTDateTime)
