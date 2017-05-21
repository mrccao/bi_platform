import arrow
from flask import current_app as app
from sqlalchemy import text
from sqlalchemy.schema import Index

from app.constants import PROMOTION_PUSH_STATUSES
from app.extensions import db
from app.libs.datetime_type import AwareDateTime
from app.models.main import AdminUser, AdminUserQuery
from app.utils import current_time


class PromotionPush(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    admin_user_id = db.Column(db.Integer, nullable=False, index=True)
    based_query_id = db.Column(db.Integer)
    push_type = db.Column(db.String(50), nullable=False, index=True)
    status = db.Column(db.String(50))
    message = db.Column(db.Text, nullable=False)
    message_key = db.Column(db.String(255), nullable=False, index=True)
    scheduled_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)
    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)
    created_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)

    def based_query_sql(self):
        if self.based_query_id is None:
            return None

        query = db.session.query(AdminUserQuery).filter_by(id=self.based_query_id).first()
        if query:
            return query.sql

        return None

    def admin_user_name(self):
        if self.admin_user_id is None:
            return None

        query = db.session.query(AdminUser).filter_by(id=self.admin_user_id).first()
        if query:
            return query.name

        return None

    def to_dict(self):
        if self.status == PROMOTION_PUSH_STATUSES.SCHEDULED.value:
            total_count = db.engine.execute(
                text("SELECT COUNT(*) FROM promotion_push_history WHERE push_id = :push_id"), push_id=self.id).scalar()
            running_count = db.engine.execute(
                text("SELECT COUNT(*) FROM promotion_push_history WHERE push_id = :push_id AND status='running'"),
                push_id=self.id).scalar()
            successed_count = db.engine.execute(
                text("SELECT COUNT(*) FROM promotion_push_history WHERE push_id = :push_id AND status='success'"),
                push_id=self.id).scalar()
            request_failed_count = db.engine.execute(text(
                "SELECT COUNT(*) FROM promotion_push_history WHERE push_id = :push_id AND status='request_failed'"),
                push_id=self.id).scalar()
            failed_count = db.engine.execute(
                text("SELECT COUNT(*) FROM promotion_push_history WHERE push_id = :push_id AND status='failed'"),
                push_id=self.id).scalar()

        return {
            'id': self.id,
            'requested_by': self.admin_user_name(),
            'based_query_id': self.based_query_id,
            'based_query_sql': self.based_query_sql(),
            'push_type': self.push_type,
            'scheduled_at': arrow.get(self.scheduled_at).to(app.config['APP_TIMEZONE']).format(),
            'status': self.status if self.status != PROMOTION_PUSH_STATUSES.SCHEDULED.value else {
                'total_count': total_count,
                'running_count': running_count,
                'successed_count': successed_count,
                'request_failed_count': request_failed_count,
                'failed_count': failed_count
            },
            'message': self.message,
            'created_at': self.created_at
        }


class PromotionPushHistory(db.Model):
    id = db.Column(db.BIGINT, primary_key=True)
    push_id = db.Column(db.Integer, nullable=False, index=True)
    push_type = db.Column(db.String(50), nullable=False)
    user_id = db.Column(db.BIGINT, index=True)
    target = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)
    scheduled_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)
    created_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)

    __table_args__ = (
        Index('ix_push_id_and_status', 'push_id', 'status'), Index('ix_push_type_and_status', 'push_type', 'status'))


class BasicProperty(object):
    @staticmethod
    def reg_time(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def reg_state(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def reg_source(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def reg_source(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def current_gold_balance(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)


class GeneralUsers(object):
    @staticmethod
    def last_login(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def game_frequency(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)


class PaidUsers(object):
    @staticmethod
    def total_purchase(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def count_of_purchase(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def last_purchase(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def last_purchase_gold(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def last_purchase_avatar(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def count_of_avatar_purchase(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def last_purchase_charms(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def count_of_charms_purchase(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def reward_level(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)

    @staticmethod
    def last_free_spin(start_time, end_time):
        user_id = db.engine.execute(text("""
        
                                        """), start_time=start_time, end_time=end_time)

        return set(user_id)


class UsersGrouping(BasicProperty, GeneralUsers, PaidUsers):
    pass
