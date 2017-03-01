import sqlparse
import hashlib
import os

from flask import Blueprint, render_template, request, jsonify, send_from_directory
from flask_login import login_required, current_user
from flask import current_app as app

from app.tasks.sql_lab import get_sql_results
from app.extensions import db
from app.utils import error_msg_from_exception, timeout
from app.models.main import AdminUserQuery
from app.constants import ADMIN_USER_QUERY_STATUSES, SQL_RESULT_STRATEGIES, ADMIN_USER_ROLES

sql_lab = Blueprint('sql_lab', __name__)


@sql_lab.route("/sql_lab", methods=["GET"])
@login_required
def index():
    return render_template('sql_lab/index.html')


@sql_lab.route("/sql_lab/query_histories", methods=["GET"])
@login_required
def query_histories():
    queries = db.session.query(AdminUserQuery).filter_by(admin_user_id=current_user.id).order_by(AdminUserQuery.updated_at.desc()).limit(10)
    return jsonify(data=[i.to_dict() for i in queries.all()])


@sql_lab.route("/sql_lab/format_sql", methods=["POST"])
@login_required
def format_sql():
    """ format given SQL query """
    sql = request.form.get('sql')
    data = sqlparse.format(sql.strip(), reindent=True, keyword_case='upper')
    return jsonify(data=data)


@sql_lab.route("/sql_lab/execute_sql", methods=["POST"])
@login_required
def execute_sql():
    """Executes the sql query returns the results."""

    sql = request.form.get('sql')
    formatted_sql = sqlparse.format(sql.strip().strip(';'), reindent=True, keyword_case='upper')
    strategy = request.form.get('strategy')

    pending_digest = (str(current_user.id) + '_' + formatted_sql).encode('utf-8')
    sql_key = hashlib.md5(pending_digest).hexdigest()

    query = db.session.query(AdminUserQuery).filter_by(sql_key=sql_key).first()
    if query is None:
        query = AdminUserQuery(
            sql=formatted_sql,
            sql_key=sql_key,
            status=ADMIN_USER_QUERY_STATUSES.PENDING.value,
            admin_user_id=current_user.id
        )

        db.session.add(query)
        db.session.commit()
    else:
        query.status = ADMIN_USER_QUERY_STATUSES.PENDING.value
        query.rows = None
        query.error_message = None
        query.run_time = None

        db.session.flush()
        db.session.commit()

    query_id = query.id

    permission = current_user.has_role(ADMIN_USER_ROLES.ROOT.value) or current_user.has_role(ADMIN_USER_ROLES.ADMIN.value)

    try:
        if strategy == SQL_RESULT_STRATEGIES.RENDER_JSON.value:
            with timeout(
                seconds=10,
                error_message="The query exceeded the 10 seconds timeout."):
                result = get_sql_results(query_id, strategy=strategy)
            return jsonify(result)
        elif strategy == SQL_RESULT_STRATEGIES.SEND_TO_MAIL.value:
            if permission:
                get_sql_results.delay(query_id, strategy=strategy)
                return jsonify(query_id=query_id), 202
            else:
                return jsonify(error="You don't have permission to access this funtion"), 403
        elif strategy == SQL_RESULT_STRATEGIES.GENERATE_DOWNLOAD_LINK.value:
            if permission:
                result = get_sql_results(query_id, strategy=strategy)
                return jsonify(result)
            else:
                return jsonify(error="You don't have permission to access this funtion"), 403
    except Exception as e:
        return jsonify(error=error_msg_from_exception(e)), 500


@sql_lab.route("/sql_lab/download", methods=["GET"])
@login_required
def download_result():
    """ download given SQL query result """
    permission = current_user.has_role(ADMIN_USER_ROLES.ROOT.value) or current_user.has_role(ADMIN_USER_ROLES.ADMIN.value)

    if permission:
        sql_key = request.args.get('key')
        ts = request.args.get('ts')
        query = db.session.query(AdminUserQuery).filter_by(sql_key=sql_key).first()
        if query is not None:
            filename = "%s_%s.%s" % (sql_key, ts, app.config['REPORT_FILE_EXTENSION'])
            return send_from_directory(app.config['REPORT_FILE_FOLDER'], filename, as_attachment=True)
