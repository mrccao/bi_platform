from flask import Flask, render_template
from flask_admin import Admin

from app.assets import assets
from app.extensions import cache, db, debug_toolbar, login, gravatar, mail, migrate
from app.libs.json_encoder import FlaskJSONEncoder
from app.models.bi import (BIUser,
                           BIUserCurrency,
                           BIUserBill,
                           BIClubWPTUser,
                           WPTUserLoginLog)
from app.models.main import AdminUser, AdminUserActivity
from app.models.orig_wpt import WPTUserLoginLog
from app.views import (AdminBaseIndexView,
                       AdminBaseModelView,
                       AdminUserModelView,
                       AdminBIUserModelView,
                       AdminBIUserCurrencyModelView,
                       AdminBIUserBillModelView,
                       AdminBIClubWPTUserModelView,
                       AdminWPTUserLoginLogModelView)


def create_app(object_name, register_blueprint=True):
    """
    An flask application factory, as explained here:
    http://flask.pocoo.org/docs/patterns/appfactories/

    Arguments:
        object_name: the python path of the config object,
                     e.g. app.settings.ProdConfig
    """

    app = Flask(__name__, static_folder='public', static_url_path='')
    app.config.from_object(object_name)
    app.json_encoder = FlaskJSONEncoder

    register_errorhandlers(app)
    register_extensions(app)

    if register_blueprint:
        register_blueprints(app)

    return app


def register_extensions(app):
    """Register Flask extensions."""

    cache.init_app(app)

    debug_toolbar.init_app(app)

    db.init_app(app)

    migrate.init_app(app, db)

    mail.init_app(app)

    gravatar.init_app(app)

    assets.init_app(app)

    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', 'Fields missing from ruleset', UserWarning)
        admin = Admin(app, index_view=AdminBaseIndexView(), url='/data')
        admin.add_view(AdminUserModelView(AdminUser, db.session, menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='admin_user'))
        admin.add_view(AdminBIUserModelView(BIUser, db.session, name='BI User', menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='bi_user'))
        admin.add_view(AdminBIUserCurrencyModelView(BIUserCurrency, db.session, name='BI User Currency', menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='bi_user_currency'))
        admin.add_view(AdminBIUserBillModelView(BIUserBill, db.session, name='BI User Bill', menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='bi_user_bill'))
        admin.add_view(AdminBIClubWPTUserModelView(BIClubWPTUser, db.session, name='BI ClubWPT User', menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='bi_clubwpt_user'))
        admin.add_view(AdminWPTUserLoginLogModelView(WPTUserLoginLog, db.session, name='User Login Log', menu_icon_type='fa', menu_icon_value='fa-circle-o', endpoint='user_login_log'))

    login.init_app(app)
    login.login_view = "account.sign_in"
    login.login_message_category = "warning"

    @login.user_loader
    def load_user(user_id):
        return AdminUser.query.get(user_id)


def register_blueprints(app):
    """Register Flask blueprints."""

    from app.controllers.account import account
    from app.controllers.dashboard import dashboard
    from app.controllers.sql_lab import sql_lab
    from app.controllers.page import page
    from app.controllers.promotion import promotion

    app.register_blueprint(account)
    app.register_blueprint(dashboard)
    app.register_blueprint(sql_lab)
    app.register_blueprint(page)
    app.register_blueprint(promotion)

    from app.libs.context_processor import global_processor
    app.context_processor(global_processor)

    return None


def register_errorhandlers(app):
    """Register error handlers."""

    def render_error(error):
        """Render error template."""

        error_code = getattr(error, 'code', 500)
        return render_template('error/{0}.html'.format(error_code)), error_code

    for errcode in [401, 404, 500]:
        app.errorhandler(errcode)(render_error)

    return None
