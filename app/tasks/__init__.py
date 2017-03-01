import os

from celery import Celery, Task
from sqlalchemy import text

from app import create_app
from app.utils import current_time


def get_engine(db_instance, bind=None):
    """ Get one db engine from multi engines. """
    return db_instance.get_engine(db_instance.get_app(), bind=bind)


def with_db_context(db_instance, func=None, bind=None):
    """ Exec with db context. """
    with get_engine(db_instance, bind=bind).connect() as connection:
        with connection.begin() as transaction:
            if func:
                return func(connection, transaction)
            return


def get_config_value(db_instance, key):
    "" "Get import config with key. """
    def func(connection, transation):
        """ Nested func """
        data = connection.execute(text('SELECT value FROM bi_import_config WHERE var = :key'), key=key).first()
        return data[0]
    return with_db_context(db_instance, func)


def set_config_value(connection, key, value):
    """ Set import config with key. """
    connection.execute(text('UPDATE bi_import_config SET value = :value WHERE var = :key'), key=key, value=value)
    return


def get_wpt_og_user_mapping(db, og_accounts):
    def func(connection, transaction):
        return connection.execute(text('SELECT user_id, og_account FROM bi_user WHERE og_account IN :og_accounts'), og_accounts=tuple(og_accounts))
    result_proxy = with_db_context(db, func)
    data = {row['og_account']: row['user_id'] for row in result_proxy}
    return data


def create_celery_app(flask_app):
    """ Create a Celery application. """
    celery_app = Celery(flask_app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery_app.conf.update(flask_app.config)
    celery_app.flask_app = flask_app
    celery_app.Task = AppContextTask
    return celery_app


class AppContextTask(Task):
    """Celery task running within a Flask application context.
    Expects the associated Flask application to be set on the bound
    Celery application.
    """

    abstract = True

    def __call__(self, *args, **kwargs):
        """Execute task."""
        with self.app.flask_app.app_context():
            return Task.__call__(self, *args, **kwargs)


env = os.environ.get('WPT_DWH_ENV', 'dev')
app = create_app('app.settings.%sConfig' % env.capitalize(), register_blueprint=False)
celery = create_celery_app(app)
