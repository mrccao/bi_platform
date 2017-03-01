import hashlib
from flask_login import UserMixin, AnonymousUserMixin
from werkzeug.security import generate_password_hash, check_password_hash

from app.extensions import db
from app.utils import current_time
from app.constants import ADMIN_USER_QUERY_STATUSES, ADMIN_USER_ROLES
from app.libs.datetime_type import NaiveDateTime, AwareDateTime, OGInsertableAwareDateTime, OGReadableAwareDateTime


class AdminUser(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), nullable=False, unique=True, index=True)
    encrypt_password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(255), nullable=False, default=ADMIN_USER_ROLES.MANAGER.value)
    timezone = db.Column(db.String(255), nullable=False, default='EST')

    failed_attempts = db.Column(db.Integer, nullable=False, default=0)
    active = db.Column(db.Boolean, default=True, index=True)

    sign_in_count = db.Column(db.Integer, nullable=False, default=0)
    current_sign_in_at = db.Column(AwareDateTime)
    current_sign_in_ip = db.Column(db.String(255))

    created_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)
    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)

    def has_role(self, role_name):
        return self.role == role_name

    def get_password(self):
        return self.encrypt_password

    def set_password(self, password):
        self.encrypt_password = generate_password_hash(password,
                                                       method='pbkdf2:sha512',
                                                       salt_length=64)

    password = property(get_password, set_password)

    def check_password(self, value):
        return check_password_hash(self.password, value)

    def track_sign_in_success(self, ip):
        if self.failed_attempts > 0:
            self.failed_attempts = 0
        self.sign_in_count += 1
        self.current_sign_in_at = current_time()
        self.current_sign_in_ip = ip
        db.session.commit()

    def track_sign_in_failure(self):
        if self.active:
            self.failed_attempts += 1
            if self.failed_attempts >= 3:
                self.failed_attempts = 0
                self.active = False
            db.session.commit()

    @property
    def is_authenticated(self):
        if isinstance(self, AnonymousUserMixin):
            return False
        else:
            return True

    def is_active(self):
        return self.active

    def is_anonymous(self):
        if isinstance(self, AnonymousUserMixin):
            return True
        else:
            return False

    def get_id(self):
        return self.id

    def __repr__(self):
        return '<AdminUser %r>' % self.email


class AdminUserActivity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    admin_user_id = db.Column(db.Integer, nullable=False, index=True)
    action = db.Column(db.String(255), nullable=False, index=True)
    raw_data = db.Column(db.Text, nullable=False)
    created_at = db.Column(AwareDateTime, nullable=False, default=current_time, index=True)

    def __init__(self, admin_user_id, action, raw_data):
        self.admin_user_id = admin_user_id
        self.action = action
        self.raw_data = raw_data

    @classmethod
    def record(admin_user_id, action, raw_data):
        db.session.add(AdminUserActivity(admin_user_id, action, raw_data))
        db.session.commit()


class AdminUserQuery(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    admin_user_id = db.Column(db.Integer, nullable=False, index=True)
    sql = db.Column(db.Text)
    status = db.Column(db.String(255))
    rows = db.Column(db.Integer)
    error_message = db.Column(db.Text)
    sql_key = db.Column(db.String(255), nullable=False, index=True)
    run_time = db.Column(db.Numeric(precision=20, scale=6))
    created_at = db.Column(AwareDateTime, default=current_time, nullable=False, index=True)
    updated_at = db.Column(AwareDateTime, onupdate=current_time, index=True)

    def to_dict(self):
        return {
            'id': self.id,
            'sql': self.sql,
            'status': self.status,
            'rows': self.rows,
            'error_message': self.error_message,
            'run_time': self.run_time,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def query_key(self):
        return hashlib.md5('%s_%s_%s' % (self.id, self.sql_key, self.upadted_at)).hexdigest()
