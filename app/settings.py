import os
import tempfile

from datetime import timedelta

db_file = tempfile.NamedTemporaryFile()


class Config(object):
    SECRET_KEY = 'REPLACE ME'


class ProdConfig(Config):
    ENV = 'prod'
    DEBUG = False
    ASSETS_DEBUG = False

    USE_X_SENDFILE = False
    WTF_CSRF_ENABLED = True

    APP_HOST = 'https://bi.playwpt.com'
    APP_TIMEZONE = 'EST'

    REPORT_FILE_EXTENSION = 'csv.gz'
    REPORT_FILE_COMPRESSION = 'gzip'
    REPORT_FILE_CONTENT_TYPE = 'application/gzip'
    REPORT_FILE_FOLDER = '/data/data/app_exported'

    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_POOL_SIZE = 20
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://x:x@127.0.0.1/bi_prod'
    SQLALCHEMY_BINDS = {
        'orig_wpt':            'mysql+pymysql://x:x@10.2.1.75/wpt',
        'orig_wpt_mall':       'mysql+pymysql://x:x@10.2.1.75/wptmall',
        'orig_wpt_payment':    'mysql+pymysql://x:x@10.2.1.75/wptpayment',
        'orig_wpt_ods':        'mysql+pymysql://x:x@192.168.1.233/wpt_ods',
    }

    CELERY_BROKER_URL = 'amqp://localhost:5672'
    CELERY_TIMEZONE = 'UTC'
    CELERY_IMPORTS = (
        'app.tasks.bi_user',
        'app.tasks.bi_user_bill',
        'app.tasks.bi_user_currency',
        'app.tasks.bi_statistic',
        'app.tasks.bi_clubwpt_user',
        'app.tasks.scheduled',
        'app.tasks.sql_lab',
    )
    CELERYBEAT_SCHEDULE = {
        'process_scheduled': {
            'task': 'app.tasks.scheduled.process_bi',
            'schedule': timedelta(seconds=8 * 60)
        }
    }

    MAIL_SUBJECT_PREFIX = '[BI System] '
    MAIL_SERVER = '192.168.1.122'
    MAIL_PORT = 25
    MAIL_USERNAME = 'wpt_report'
    MAIL_PASSWORD = 'eo9ee3It9EWf3CG867Tc'
    MAIL_DEFAULT_SENDER = 'WPT Report <wpt_report@ourgame.com>'

    CACHE_TYPE = 'simple'


class DevConfig(Config):
    ENV = 'dev'
    DEBUG = True
    ASSETS_DEBUG = False
    DEBUG_TB_INTERCEPT_REDIRECTS = False

    USE_X_SENDFILE = False
    WTF_CSRF_ENABLED = True

    APP_HOST = 'http://localhost:5000'
    APP_TIMEZONE = 'EST'

    REPORT_FILE_EXTENSION = 'csv.gz'
    REPORT_FILE_COMPRESSION = 'gzip'
    REPORT_FILE_CONTENT_TYPE = 'application/gzip'
    REPORT_FILE_FOLDER = '/Users/Lei/Workspace/Projects/wpt-dwh-static'

    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_DATABASE_URI =  'mysql+pymysql://root:@127.0.0.1/bi_dev'
    SQLALCHEMY_BINDS = {
        'orig_wpt':            'mysql+pymysql://wpt_read:wxoselctWxjJnv0PhpQz@172.28.14.115/wpt',
        'orig_wpt_mall':       'mysql+pymysql://wpt_read:wxoselctWxjJnv0PhpQz@172.28.14.115/wptmall',
        'orig_wpt_payment':    'mysql+pymysql://wpt_read:wxoselctWxjJnv0PhpQz@172.28.14.115/wpt_payment',
        'orig_wpt_ods':        'mysql+pymysql://localhost/orig_wpt_ods',
    }

    CELERY_BROKER_URL = 'amqp://localhost:5672'
    CELERY_TIMEZONE = 'UTC'
    CELERY_IMPORTS = (
        'app.tasks.bi_user',
        'app.tasks.bi_user_bill',
        'app.tasks.bi_user_currency',
        'app.tasks.bi_statistic',
        'app.tasks.bi_clubwpt_user',
        'app.tasks.scheduled',
        'app.tasks.sql_lab',
    )
    CELERYBEAT_SCHEDULE = {
        'process_scheduled': {
            'task': 'app.tasks.scheduled.process_bi',
            'schedule': timedelta(seconds=8 * 60)
        }
    }

    MAIL_SUBJECT_PREFIX = '[BI System] '
    MAIL_SERVER = 'mailtrap.io'
    MAIL_PORT = 2525
    MAIL_USERNAME = '7d57fceb668eef'
    MAIL_PASSWORD = '4bce2f066aaa28'
    MAIL_DEFAULT_SENDER = 'WPT Report <wpt_report@ourgame.com>'

    CACHE_TYPE = 'simple'


class TestConfig(Config):
    ENV = 'test'
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    USE_X_SENDFILE = False
    WTF_CSRF_ENABLED = False

    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + db_file.name
    # SQLALCHEMY_BINDS = {
    #     'orig_wpt':            'mysql+pymysql://localhost/orig_wpt',
    #     'orig_wpt_mall':       'mysql+pymysql://localhost/orig_wpt_mall',
    #     'orig_wpt_payment':    'mysql+pymysql://localhost/orig_wpt_payment',

    #     'orig_slots':          'mysql+pymysql://localhost/orig_slots',
    #     'orig_poker':          'mysql+pymysql://localhost/orig_pokers',

    #     'orig_wpt_ods':        'mysql+pymysql://localhost/orig_wpt_ods',
    # }
    SQLALCHEMY_ECHO = True

    CELERY_BROKER_URL = 'amqp://localhost:5672'
    CELERY_TIMEZONE = 'UTC'
    CELERY_IMPORTS = (
        'app.tasks.bi_user',
        'app.tasks.bi_user_bill',
        'app.tasks.bi_user_currency',
        'app.tasks.bi_statistic',
        'app.tasks.bi_clubwpt_user',
        'app.tasks.scheduled',
        'app.tasks.sql_lab',
    )
    CELERYBEAT_SCHEDULE = {
        'process_scheduled': {
            'task': 'app.tasks.scheduled.process_bi',
            'schedule': timedelta(seconds=8 * 60)
        }
    }

    CACHE_TYPE = 'simple'
