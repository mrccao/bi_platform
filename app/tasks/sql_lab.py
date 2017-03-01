import os
import sqlparse
import pandas as pd

from flask import current_app as app
from sqlalchemy import text
from sqlalchemy.sql.expression import bindparam

from app.libs.dataframe import DataFrame
from app.tasks import celery, get_config_value, set_config_value, with_db_context
from app.tasks.mail import send_mail
from app.extensions import db
from app.models.main import AdminUser, AdminUserQuery
from app.constants import ADMIN_USER_QUERY_STATUSES, SQL_RESULT_STRATEGIES
from app.utils import current_time, current_time_as_float, error_msg_from_exception, dedup

@celery.task
def get_sql_results(query_id, strategy=SQL_RESULT_STRATEGIES.RENDER_JSON.value):
    """Executes the sql query returns the results."""

    query = db.session.query(AdminUserQuery).filter_by(id=query_id).one()

    def handle_error(msg):
        """Local method handling error while processing the SQL"""
        query.error_message = msg
        query.status = ADMIN_USER_QUERY_STATUSES.FAILED.value
        db.session.commit()
        raise Exception(msg)

    try:
        parsed_sql = sqlparse.parse(query.sql)[0]

        if str(parsed_sql.tokens[0]).upper() != 'SELECT':
            handle_error("Only `SELECT` statements are allowed against this database")

        start_time = current_time_as_float()

        result_proxy = db.engine.execute(text(str(parsed_sql)))

        query.status = ADMIN_USER_QUERY_STATUSES.RUNNING.value
        db.session.flush()

        result = None
        if result_proxy.cursor:
            column_names = dedup([col[0] for col in result_proxy.cursor.description])
            data = result_proxy.fetchall()
            result = DataFrame(pd.DataFrame(data, columns=column_names))

        # counting rows
        query.rows = result_proxy.rowcount
        if query.rows == -1 and result:
            query.rows = result.size

        if strategy == SQL_RESULT_STRATEGIES.RENDER_JSON.value and query.rows > 2000:
            handle_error("The query exceeded the maximum record limit: 2000. You may want to run your query with a LIMIT.")
        else:
            query.run_time = round(current_time_as_float() - start_time, 3)
            query.status = ADMIN_USER_QUERY_STATUSES.SUCCESS.value

            db.session.flush()
            db.session.commit()

        #########

        now = current_time().to(app.config['APP_TIMEZONE'])
        sql_key = query.sql_key
        current_user_id = query.admin_user_id

        if strategy == SQL_RESULT_STRATEGIES.RENDER_JSON.value:
            return {
                'columns': result.columns if result else [],
                'data': result.data if result else [],
                'rows': query.rows,
                'run_time': query.run_time
            }
        elif strategy == SQL_RESULT_STRATEGIES.SEND_TO_MAIL.value:
            admin_user = db.session.query(AdminUser).filter_by(id=current_user_id).one()

            filename = '%s_%s.%s' % (sql_key, now.timestamp, app.config['REPORT_FILE_EXTENSION'])
            path = os.path.join(app.config['REPORT_FILE_FOLDER'], filename)
            result.dateframe.to_csv(path, compression=app.config['REPORT_FILE_COMPRESSION'], encoding='utf-8')

            send_mail(admin_user.email,
                      'SQL Lab result - %s' % sql_key,
                      'sql_result_report',
                      path,
                      app.config['REPORT_FILE_CONTENT_TYPE'],
                      username=admin_user.name,
                      sql=str(parsed_sql),
                      filename=filename,
                      generated_at=now.format())
        elif strategy == SQL_RESULT_STRATEGIES.GENERATE_DOWNLOAD_LINK.value:
            # filename = '%s.%s' % (sql_key, app.config['REPORT_FILE_EXTENSION'])
            filename = '%s_%s.%s' % (sql_key, now.timestamp, app.config['REPORT_FILE_EXTENSION'])
            path = os.path.join(app.config['REPORT_FILE_FOLDER'], filename)
            result.dateframe.to_csv(path, compression=app.config['REPORT_FILE_COMPRESSION'], encoding='utf-8')
            print('%s/sql_lab/download?key=%s&ts=%s' % (app.config['APP_HOST'], sql_key, now.timestamp))

            return {
                'download_link': '%s/sql_lab/download?key=%s&ts=%s' % (app.config['APP_HOST'], sql_key, now.timestamp)
            }
        return None
    except Exception as e:
        handle_error(error_msg_from_exception(e))
