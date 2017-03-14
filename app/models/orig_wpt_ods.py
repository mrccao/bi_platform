from app.constants import TRANSACTION_TYPES
from app.extensions import db
from app.libs.datetime_type import OGReadableAwareDateTime


class WPTODSGoldActivity(db.Model):
    __bind_key__ = 'orig_wpt_ods'
    __tablename__ = 'powergamecoin_detail'

    id = db.Column('id', db.Integer, primary_key=True)
    og_account = db.Column('username', db.String)
    game_id = db.Column('gameid', db.Integer)
    transaction_type = db.Column('producttype', db.Integer)
    transaction_amount = db.Column('gamecoin', db.Integer)
    balance = db.Column('coin', db.Integer)
    ip = db.Column('userip', db.Integer)
    created_at = db.Column('recdate', OGReadableAwareDateTime)

    def transaction_type_display(self):
        value = TRANSACTION_TYPES[self.transaction_type]
        return '%s %s' % (self.transaction_type, value) if value is not None else self.transaction_type

    def balance_display(self):
        return self.transaction_amount + self.balance

    def game_id_display(self):
        return self.game_id


class WPTODSSilverActivity(db.Model):
    __bind_key__ = 'orig_wpt_ods'
    __tablename__ = 'gamecoin_detail'

    id = db.Column('id', db.Integer, primary_key=True)
    og_account = db.Column('username', db.String)
    game_id = db.Column('gameid', db.Integer)
    transaction_type = db.Column('producttype', db.Integer)
    transaction_amount = db.Column('gamecoin', db.Integer)
    balance = db.Column('coin', db.Integer)
    ip = db.Column('userip', db.Integer)
    created_at = db.Column('recdate', OGReadableAwareDateTime)

    def transaction_type_display(self):
        value = TRANSACTION_TYPES[self.transaction_type]
        return '%s %s' % (self.transaction_type, value) if value is not None else self.transaction_type

    def balance_display(self):
        return self.transaction_amount + self.balance

    def game_id_display(self):
        return self.game_id
