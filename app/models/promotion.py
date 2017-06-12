import arrow
from flask import current_app as app
from functools import reduce
from operator import iand, ior
from sqlalchemy import text
from sqlalchemy.schema import Index

from app.constants import PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES
from app.extensions import db
from app.libs.datetime_type import AwareDateTime
from app.models.main import AdminUser, AdminUserQuery
from app.tasks import with_db_context
from app.utils import current_time


class PromotionPush(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    admin_user_id = db.Column(db.Integer, nullable=False, index=True)
    based_query_id = db.Column(db.Integer)
    based_query_rules = db.Column(db.String(50))
    push_type = db.Column(db.String(50), nullable=False, index=True)
    status = db.Column(db.String(50))
    message = db.Column(db.Text, nullable=False)
    message_key = db.Column(db.String(255), nullable=False, index=True)
    scheduled_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)
    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)
    created_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)

    def based_query_sql(self):
        if not self.based_query_id:
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
            succeed_count = db.engine.execute(
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
            'based_query_rules': self.based_query_rules,
            'push_type': self.push_type,
            'scheduled_at': arrow.get(self.scheduled_at).to(app.config['APP_TIMEZONE']).format(),
            'status': self.status if self.status != PROMOTION_PUSH_STATUSES.SCHEDULED.value else {
                'total_count': total_count,
                'running_count': running_count,
                'succeed_count': succeed_count,
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
    @classmethod
    def reg_time(cls, field, operator, value):
        sql = """ SELECT user_id FROM( SELECT user_id, DATE_FORMAT(reg_time,'%Y-%m-%d') AS reg_time FROM bi_user )t """

        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def birthday(cls, field, operator, value):
        sql = 'SELECT user_id FROM bi_user WHERE birthday'

        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def reg_state(cls, field, operator, value):
        sql = "SELECT user_id FROM bi_user WHERE reg_state = :value"

        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id


class GameBehaviour(BasicProperty):
    @classmethod
    def x_days_inactive(cls, field, operator, value):
        sql = """
                SELECT DISTINCT  user_id 
                FROM   bi_user
                WHERE  user_id NOT IN (SELECT DISTINCT user_id
                                       FROM   bi_user_currency
                                       WHERE  DATE(created_at)> DATE_ADD(CURDATE(), INTERVAL - :value DAY)
                                              AND transaction_type NOT IN (20132001, 999998301, 925011306, 
                                              925011307, 925011410, 925011411, 30007777, 925011311, 923118301, 
                                              923118302, 923118303, 923118304, 923118311, 923118312, 923118313, 
                                              923118314))
              """

        result_proxy = list(db.engine.execute(text(sql), value=value))
        user_id = [item[0] for item in result_proxy]

        return user_id

    @classmethod
    def average_active_days_weekly(cls, field, operator, value):
        sql = """

                SELECT user_id,  average_active_days_weekly FROM (
                SELECT c.user_id AS user_id, Count(DISTINCT DATE(c.created_at)) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 7) AS average_active_days_weekly
                FROM   bi_user u
                       INNER JOIN bi_user_currency c
                               ON u.user_id = c.user_id
                GROUP BY c.user_id 
                )t
             """
        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def average_active_days_monthly(cls, field, operator, value):
        sql = """
                SELECT user_id , average_active_days_monthly FROM (
                SELECT c.user_id AS user_id, Count(DISTINCT DATE(c.created_at)) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 30) AS average_active_days_monthly
                FROM   bi_user u
                       INNER JOIN bi_user_currency c
                               ON u.user_id = c.user_id

                GROUP BY c.user_id 
                ) t 
             """
        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id


class PaidBehaviour(GameBehaviour):
    @classmethod
    def never_purchased_users(cls, field, operator, value):
        sql = """
                SELECT user_id
                FROM   bi_user
                WHERE  user_id NOT IN (SELECT DISTINCT user_id
                                       FROM   bi_user_bill) 
              """

        result_proxy = list(db.engine.execute(text(sql)))
        user_id = [item[0] for item in result_proxy]

        return set(user_id)

    @classmethod
    def purchased_users(cls, field, operator, value):
        sql = """ SELECT DISTINCT user_id FROM bi_user_bill """

        result_proxy = db.engine.execute(text(sql))
        user_id = [item[0] for item in result_proxy]

        return user_id

    @classmethod
    def x_days_not_purchase(cls, field, operator, value):
        sql = """
                SELECT DISTINCT  user_id 
                FROM   bi_user_bill
                WHERE  user_id NOT IN (SELECT DISTINCT user_id
                                       FROM   bi_user_bill
                                       WHERE  DATE(created_at) > :value
                                              AND currency_type = 'Dollar') 
               """

        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def average_purchase_amount_monthly(cls, field, operator, value):
        sql = """ 
                SELECT user_id , average_purchase_amount_monthly FROM(
                SELECT b.user_id AS user_id, SUM(currency_amount) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 30) AS average_purchase_amount_monthly
                FROM   bi_user u
                       INNER JOIN bi_user_bill b
                               ON u.user_id = b.user_id
                WHERE b.currency_type = 'Dollar'
                GROUP BY b.user_id 
                ) t
                
               """

        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def average_purchase_amount_weekly(cls, field, operator, value):
        sql = """
                SELECT user_id ,average_purchase_amount_weekly FROM(
                SELECT b.user_id AS user_id, SUM(currency_amount) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 7) AS average_purchase_amount_weekly
                FROM   bi_user u
                       INNER JOIN bi_user_bill b
                               ON u.user_id = b.user_id
                WHERE b.currency_type = 'Dollar'
                GROUP BY b.user_id 
                ) t
              """
        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def average_purchase_count_monthly(cls, field, operator, value):
        sql = """
                SELECT user_id, average_purchase_count_monthly FROM (
                SELECT b.user_id AS user_id, Count(DISTINCT DATE(b.created_at)) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 30) AS average_purchase_count_monthly
                FROM   bi_user u
                       INNER JOIN bi_user_bill b
                               ON u.user_id = b.user_id
                WHERE b.currency_type = 'Dollar'
                GROUP BY b.user_id 
                ) t
         """
        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id

    @classmethod
    def average_purchase_count_weekly(cls, field, operator, value):
        sql = """
                SELECT  average_purchase_count_weekly FROM (
                SELECT b.user_id, Count(DISTINCT DATE(b.created_at)) / (DATEDIFF(CURDATE(), DATE
                (u.reg_time)) / 7) AS average_purchase_count_weekly
                FROM   bi_user u
                       INNER JOIN bi_user_bill b
                               ON u.user_id = b.user_id
                WHERE b.currency_type = 'Dollar'
                GROUP BY b.user_id ) t
         """
        user_id = with_db_context(db, sql_filter_option, sql=sql, field=field, operator=operator, value=value)

        return user_id


class UsersGrouping(PaidBehaviour):
    @classmethod
    def parse_query_rules(cls, rules):

        field = rules["field"]
        operator = rules["operator"]
        value = rules["value"]

        if field == 'purchased' and value == '1':
            field = "purchased_users"
        if field == 'purchased' and value == '0':
            field = "never_purchased_users"

        fields = {"reg_time": super(UsersGrouping, cls).reg_time, "reg_state": super(UsersGrouping, cls).reg_state,
                  "birthday": super(UsersGrouping, cls).birthday,
                  "x_days_inactive": super(UsersGrouping, cls).x_days_inactive,
                  "average_active_days_weekly": super(UsersGrouping, cls).average_active_days_weekly,
                  "average_active_days_monthly": super(UsersGrouping, cls).average_active_days_monthly,
                  "purchased_users": super(UsersGrouping, cls).purchased_users,
                  "never_purchased_users": super(UsersGrouping, cls).never_purchased_users,
                  "X_days_not_purchase": super(UsersGrouping, cls).x_days_inactive,
                  "average_purchase_count_monthly": super(UsersGrouping, cls).average_purchase_count_monthly,
                  "average_purchase_count_weekly": super(UsersGrouping, cls).average_purchase_count_weekly,
                  "average_purchase_amount_monthly": super(UsersGrouping, cls).average_purchase_amount_monthly,
                  "average_purchase_amount_weekly": super(UsersGrouping, cls).average_purchase_amount_weekly}

        user_id = fields[field](field, operator, value)

        return set(user_id)

    @classmethod
    def get_user_id(cls, query_rules):

        condition = query_rules["condition"]
        rules = query_rules["rules"]

        def get_child_query_rules(rules):

            if "condition" in rules:

                cls.get_user_id(rules)

            else:

                every_child_query_user_id = cls.parse_query_rules(rules)

                return every_child_query_user_id

        user_id = list(map(get_child_query_rules, rules))

        if condition == "AND":

            query_result = list(reduce(iand, user_id))

        else:

            query_result = list(reduce(ior, user_id))

        return query_result

    @classmethod
    def generate_recipients(cls, query_rules, notification_type):

        def get_user(query_rules, notification_type):

            user_ids = cls.get_user_id(query_rules)

            if user_ids:

                if notification_type == PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value:
                    result_proxy = db.get_engine(app, bind='orig_wpt').execute(text(
                        """ SELECT u_id AS user_id  ,pu_id  AS platform_user_id FROM  tb_platform_user_info WHERE u_id IN :user_ids """),
                        user_ids=tuple(user_ids))

                    return result_proxy

                elif notification_type == PROMOTION_PUSH_TYPES.EMAIL.value:
                    result_proxy = db.engine.execute(text(
                        """ SELECT user_id,  username,reg_country,reg_state,email  FROM bi_user WHERE user_id IN :user_ids """),
                        user_ids=tuple(user_ids))
                    return result_proxy

                else:

                    return []
            else:
                return []

        if notification_type == PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value:
            result_proxy = get_user(query_rules=query_rules, notification_type=notification_type)

            recipients = [[row['user_id'], row['platform_user_id']] for row in result_proxy]

            return recipients

        if notification_type == PROMOTION_PUSH_TYPES.EMAIL.value:
            result_proxy = get_user(query_rules=query_rules, notification_type=notification_type)

            recipients = [{'user_id': row['user_id'], 'username': row['username'], 'country': row['reg_country'],
                           'email': row['email']} for row in result_proxy]

            return recipients


def sql_filter_option(connection, transaction, sql, field, operator, value):
    if operator == "less":

        result_proxy = connection.execute(text(sql + 'WHERE ' + field + '< :value'), value=value)

    elif operator == "equal":

        result_proxy = connection.execute(text(sql + 'WHERE ' + field + '= :value'), value=value)

    elif operator == "greater":

        result_proxy = connection.execute(text(sql + 'WHERE ' + field + '> :value'),
                                          value=value)
    else:

        result_proxy = connection.execute(text(sql + 'WHERE ' + field + ' BETWEEN :value1 AND :value2'),
                                          value1=value[0], value2=value[1])

    user_ids = list([item[0] for item in result_proxy])

    return user_ids
