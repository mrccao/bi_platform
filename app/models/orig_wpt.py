from jinja2 import Markup

from app.extensions import db
from app.libs.datetime_type import NaiveDateTime, AwareDateTime, OGInsertableAwareDateTime, OGReadableAwareDateTime

class WPTPlatform(db.Model):
    __bind_key__ = 'orig_wpt'
    __tablename__ = 'tb_platform_info'

    id = db.Column('p_id', db.Integer, primary_key=True)
    name = db.Column('p_name', db.String)
    display_name = db.Column('p_display_name', db.String)


class WPTPlatformUser(db.Model):
    __bind_key__ = 'orig_wpt'
    __tablename__ = 'tb_platform_user_info'

    platform_id = db.Column('p_id', db.Integer, primary_key=True)
    user_id = db.Column('u_id', db.Integer, primary_key=True)


class WPTUser(db.Model):
    __bind_key__ = 'orig_wpt'
    __tablename__ = 'tb_user_base'

    id = db.Column('u_id', db.Integer, primary_key=True)
    email = db.Column('u_email', db.String)
    status = db.Column('u_status', db.String)
    platform_id = db.Column('p_id', db.Integer)
    reg_source = db.Column('u_type', db.String)
    reg_ip = db.Column('reg_ip', db.String)
    reg_device = db.Column('reg_device', db.String)
    reg_time = db.Column('reg_time', db.String)

    # def login_logs(self, limit=5):
    #     return WPTUserLoginLog.query.filter_by(user_id=self.id).order_by('id DESC').limit(limit).all()

    def __repr__(self):
        return '<WPTUser %r>' % self.email


class WPTUserDetail(db.Model):
    __bind_key__ = 'orig_wpt'
    __tablename__ = 'tb_user_info'

    uesr_id = db.Column('u_id', db.Integer, primary_key=True)
    gender = db.Column('u_sex', db.Integer)
    first_name = db.Column('name_first', db.String)
    middle_name = db.Column('name_middle', db.String)
    last_name = db.Column('name_last', db.String)
    country = db.Column('addr_country', db.String)
    state = db.Column('addr_state', db.String)
    city = db.Column('addr_city', db.String)
    address = db.Column('addr_detail', db.String)
    postal_code = db.Column('addr_post_code', db.String)
    phone = db.Column('u_phone', db.String)
    birthday = db.Column('u_birth', NaiveDateTime)
    update_time = db.Column('update_time', OGReadableAwareDateTime)


# class WPTUserChangeLog(db.Model):
    # __bind_key__ = 'orig_wpt'
#     __tablename__ = 'tb_user_info_change'


class WPTUserLoginLog(db.Model):
    __bind_key__ = 'orig_wpt'
    __tablename__ = 'tb_user_login_log'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column('u_id', db.Integer)
    login_ip = db.Column('login_ip', db.String)
    login_time = db.Column('login_time', OGReadableAwareDateTime)
    login_result = db.Column('remark', db.String)

    def login_result_type(self):
        s = self.login_result
        r = '<span class="label label-danger">Unknown</span>'
        if s.find('True~') >= 0:
            r = '<span class="label label-success">Passed</span>'
        if s.find('validate') >= 0:
            r = '<span class="label label-warning">Failed</span>'
        if s.find('banned') >= 0:
            r = '<span class="label label-primary">Failed</span>'
        if s.find('correct') >= 0:
            r = '<span class="label label-danger">Failed</span>'

        return Markup(r)

    def login_result_text(self):
        try:
            s = self.login_result.split('~')[-1]
            if s is None:
                return self.login_result
            return s
        except:
            return 'Exception'

