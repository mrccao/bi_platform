import arrow
from flask import current_app as app
from functools import reduce
from operator import iand, ior
from sqlalchemy import text
from sqlalchemy.schema import Index

from app.constants import PROMOTION_PUSH_STATUSES
from app.extensions import db
from app.libs.datetime_type import AwareDateTime
from app.models.main import AdminUser, AdminUserQuery
from app.tasks import with_db_context
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
    @classmethod
    def sql_execute(cls, sql, field, operator, value):

        def collection(connection, transaction, field=field):

            if field in ["reg_time", "last_active_time", " last_purchase", "last_purchase_avatar", "last_purchase_charms",
                         "last_free_spin"]: field = "DATE_FORMAT(on_day,'%Y-%m-%d')"

            if operator == "less":

                user_id = db.engine.execute(text(sql + ' WHERE' + field + ' < :value'), value=value, field=field)

            elif operator == "equal":

                user_id = db.engine.execute(text(sql + 'WHERE' + field + ' = :value'), value=value, field=field)

            elif operator == "greater":

                user_id = db.engine.execute(text(sql + 'WHERE' + field + ' > :value'), value=value, field=field)

            else:

                user_id = db.engine.execute(text(sql + 'WHERE' + field + ' BETWEEN :value1 AND :value2'),
                                            value1=value[0],
                                            value2=value[1], field=field)

            return set(user_id)

        result_proxy = with_db_context(db, collection)

        result_proxy = set(list(result_proxy))

        return result_proxy

    @classmethod
    def reg_time(cls, field, operator, value):

        sql = 'SELECT user_id FROM bi_user'

        return cls.sql_execute(sql, field, operator, value)

    @classmethod
    def reg_state(cls, field, operator, value):

        sql = "SELECT user_id FROM bi_user"

        return cls.sql_execute(sql, field, operator, value)

    @classmethod
    def reg_source(cls, operator, value):

        sql = "SELECT user_id FROM bi_user"

        return cls.sql_execute(sql, operator, value)

    # TODO
    # @classmethod
    # def current_gold_balance(cls, operator, value):

    # sql = "SELECT user_id FROM bi_user"
    #
    # return cls.sql_execute(sql, operator, value)


class GameBehaviour(BasicProperty):
    @classmethod
    def last_active_time(cls, operator, value):
        sql = ""

        return cls.sql_execute(sql, operator, value)

    @classmethod
    def active_days_of_the_month(cls, operator, value):
        sql = """
        
        SELECT user_id
FROM (
       SELECT DISTINCT
         DATE(created_at),
         user_id
       FROM bi_user_currency
       WHERE DATE(created_at) > '2017-04-14'
     ) t
GROUP BY user_id
HAVING count(*) > 5;
        
        
        """


        return cls.sql_execute(sql, operator, value)


    @classmethod
    def consecutive_active_day_of_the_month(cls, operator, value):

        sql ="""
        
        
SELECT *
FROM (SELECT *
      FROM (
             SELECT
               user_id,
               max(days)      lianxu_days,
               min(login_day) start_date,
               max(login_day) end_date
             FROM (


                    SELECT
                      user_id,
                      @cont_day :=
                      (CASE
                       WHEN (@last_uid = user_id AND DATEDIFF(created_at, @last_dt) = 1)
                         THEN
                           (@cont_day + 1)
                       WHEN (@last_uid = user_id AND DATEDIFF(created_at, @last_dt) < 1)
                         THEN
                           (@cont_day + 0)
                       ELSE
                         1
                       END)                                              AS days,
                      (@cont_ix := (@cont_ix + IF(@cont_day = 1, 1, 0))) AS cont_ix,
                      @last_uid := user_id,
                      @last_dt := created_at                                login_day
                    FROM


                      (


                        SELECT DISTINCT
                          user_id,
                          DATE(created_at) created_at
                        FROM bi_user_currency
                        WHERE user_id != 0
                        ORDER BY user_id, created_at) AS t,


                      (SELECT
                         @last_uid := '',
                         @last_dt := '',
                         @cont_ix := 0,
                         @cont_day := 0) AS t1
                  ) AS t2
             GROUP BY user_id, cont_ix
             HAVING lianxu_days > 50
           )


           tmp
      ORDER BY lianxu_days DESC) ntmp
GROUP BY user_id
ORDER BY lianxu_days DESC
        
        
        
        """


        return cls.sql_execute(sql, operator, value)
#
# class PaidBehaviour(GameBehaviour):
#     @classmethod
#     def total_purchase(cls, operator, value):
#
#         sql = """
#             SELECT user_id,total_purchase
#             FROM (
#
#                 SELECT
#             user_id,
#             sum(currency_amount) AS total_purchase
#             FROM bi_user_bill
#             WHERE currency_type = 'Dollar'
#             GROUP BY user_id
#
#             ) t
#             ORDER BY  total_purchase DESC
#          """
#         return cls.sql_execute(sql, operator, value)
#
#
#     @classmethod
#     def count_of_purchase(cls, operator, value):
#
#         sql = """
#          SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          count(*) AS purchase_count
#        FROM bi_user_bill
#        WHERE currency_type = 'gold'
#              AND category_orig = 6
#        GROUP BY user_id
#      ) t
# WHERE purchase_count > 40;
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def last_purchase(cls, operator, value):
#
#         sql = """
#
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def last_purchase_gold(cls, operator, value):
#
#         sql = """
#            SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          substring_index(group_concat(date(created_at) ORDER BY date(created_at) DESC SEPARATOR ','), ',',
#                          1) AS last_purchase
#        FROM bi_user_bill
#        WHERE currency_type = 'dollar'
#              AND category = 'Gold'
#        GROUP BY user_id
#      ) t
# WHERE last_purchase = '2017-04-10';
#
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def last_purchase_avatar(cls, operator, value):
#         sql = """
#            SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          substring_index(group_concat(date(created_at) ORDER BY date(created_at) DESC SEPARATOR ','), ',',
#                          1) AS last_purchase
#        FROM bi_user_bill
#        WHERE currency_type = 'gold'
#              AND category_orig = 6
#        GROUP BY user_id
#      ) t
# WHERE last_purchase = '2017-04-10';
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def count_of_avatar_purchase(cls, operator, value):
#
#         sql = """
# SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          count(*) AS purchase_count
#        FROM bi_user_bill
#        WHERE currency_type = 'gold'
#              AND category_orig = 6
#        GROUP BY user_id
#      ) t
# WHERE purchase_count > 40;
#
#
#          """
#
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def last_purchase_charms(cls, operator, value):
#
#         sql = """
#
# SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          substring_index(group_concat(date(created_at) ORDER BY date(created_at) DESC SEPARATOR ','), ',',
#                          1) AS last_purchase
#        FROM bi_user_bill
#        WHERE currency_type = 'gold'
#              AND category_orig = 5
#        GROUP BY user_id
#      ) t
# WHERE last_purchase = '2017-04-10';
#
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def count_of_charms_purchase(cls, operator, value):
#
#         sql = """
#
#
# SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          count(*) AS purchase_count
#        FROM bi_user_bill
#        WHERE currency_type = 'gold'
#              AND category_orig = 6
#        GROUP BY user_id
#      ) t
# WHERE purchase_count > 40;
#
#
#
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def reward_level(cls, operator, value):
#
#         sql = """
#
#          """
#
#         return cls.sql_execute(sql, operator, value)
#
#     @classmethod
#     def last_free_spin(cls, operator, value):   sql = """
#
# SELECT user_id
# FROM (
#        SELECT
#          user_id,
#          substring_index(group_concat(date(created_at) ORDER BY date(created_at) DESC SEPARATOR ','), ',',
#                          1) AS last_purchase
#        FROM bi_user_bill
#        WHERE currency_type = 'dollar'
#              AND category_orig = 3
#        GROUP BY user_id
#      ) t
# WHERE last_purchase = '2017-04-10';
#
#          """
#
#
#
#         return cls.sql_execute(sql, operator, value)
#
#
# class UsersGrouping(PaidBehaviour):
#     fields = {"reg_time": super().reg_time, "reg_state": super().reg_state,
#               "reg_source": super().reg_source, "current_gold_balance": super().current_gold_balance,
#               "last_active_time": super().last_active_time, "game_frequency": super().game_frequency,
#               "total_purchase": super().total_purchase, "count_of_purchase": super().count_of_purchase,
#               " last_purchase": super().last_purchase, "last_purchase_gold": super().last_purchase_gold,
#               "last_purchase_avatar": super().last_purchase_avatar,
#               "last_purchase_charms": super().last_purchase_charms,
#               "last_free_spin": super().last_free_spin, "count_of_avatar_purchase": super().count_of_avatar_purchase,
#               "count_of_charms_purchase": super().count_of_charms_purchase, "reward_level": super().reward_level}
#
#     @classmethod
#     def parse_query_rules(cls, rules):
#         field = rules["field"]
#         operator = rules["operator"]
#         value = rules["value"]
#         value = list(map(lambda datetime: arrow.Arrow.strptime(datetime, "YYYY-MM-DD"), value))
#         if isinstance(value, str):
#             value = arrow.Arrow.strptime(value, "YYYY-MM-DD")
#
#         user_id_set = cls.fields[field](field, operator, value)
#
#         return user_id_set
#
#     @classmethod
#     def get_user_id(cls, query_rules):
#         condition = query_rules["condition"]
#         rules = query_rules["rules"]
#
#         def get_child_query_rules(rules):
#             if "condition" in rules:
#                 cls.get_user_id(rules)
#             else:
#                 cls.parse_query_rules(rules)
#
#         user_id_list = list(map(get_child_query_rules, rules))
#
#         if condition == "AND":
#             result = list(reduce(iand, user_id_list))
#         else:
#             result = list(reduce(ior, user_id_list))
#
#         return result
#
#
#
#         # @classmethod
#         # def parse_query_rules(cls, rules):
#         #     field = rules["field"]
#         #     operator = rules["operator"]
#         #     value = rules["value"]
#         #
#         #     filter_str_dict = {"less": lambda value: '< {}'.format(value), "equal": lambda value: '= {}'.format(value),
#         #                        "greater": lambda value: '> {}'.format(value),
#         #                        "between": lambda value: 'between {} AND {}'.format(*value)}
#         #
#         #     filter_str = filter_str_dict[operator](value)
#         #
#         #     user_id_set = cls.fields[field](filter_str)
#         #
#         #     return user_id_set
#         #
