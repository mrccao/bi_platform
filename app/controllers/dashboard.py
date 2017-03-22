import arrow
from flask import Blueprint, render_template, request, jsonify
from flask import current_app as app
from flask_login import login_required
from sqlalchemy import text

from app.extensions import db
from app.utils import current_time

dashboard = Blueprint('dashboard', __name__)


@dashboard.route("/", methods=["GET"])
@login_required
def index():
    return render_template('dashboard/index.html')


@dashboard.route("/dashboard/visualization/summary_data", methods=["GET"])
@login_required
def visualization_summary_data():
    now = current_time(app.config['APP_TIMEZONE'])

    if request.args.get('day') and request.args.get('day') == 'yday':
        day = now.replace(days=-1).format('YYYY-MM-DD')
    else:
        day = now.format('YYYY-MM-DD')

    new_registration = db.engine.execute(text("""
                                              SELECT SUM(new_registration)
                                              FROM   bi_statistic
                                              WHERE  platform = 'All Platform'
                                              AND    on_day = :day
                                              """), day=day).scalar()

    revenue = db.engine.execute(text("""
                                     SELECT ROUND(SUM(currency_amount), 2)
                                     FROM   bi_user_bill
                                     WHERE  currency_type = 'Dollar'
                                            AND DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) = :day
                                     """), day=day).scalar()

    game_dau = db.engine.execute(text("""
                                      SELECT SUM(dau)
                                      FROM   bi_statistic
                                      WHERE  game ='All Game'
                                      AND    on_day = :day
                                      """), day=day).scalar()

    new_registration_game_dau = db.engine.execute(text("""
                                                       SELECT SUM(new_registration_game_dau)
                                                       FROM   bi_statistic
                                                       WHERE  on_day = :day
                                                       """), day=day).scalar()

    payload = {
        'new_registration': new_registration,
        'revenue': revenue or 0,
        'game_dau': game_dau,
        'new_registration_game_dau': new_registration_game_dau
    }

    return jsonify(payload)


@dashboard.route("/dashboard/visualization/executive_data", methods=["GET"])
@login_required
def visualization_executive_data():
    days_ago = request.args.get('days_ago')

    if days_ago is None:
        start_time, end_time = request.args.get('date_range').split('  -  ')
    else:
        now = current_time(app.config['APP_TIMEZONE'])
        end_time = now.replace(days=-1).format('YYYY-MM-DD')
        start_time = now.replace(days=-int(days_ago)).format('YYYY-MM-DD')

    game = request.args.get('game')
    platform = request.args.get('platform')
    report_type = request.args.get('report_type')
    proxy = []

    if report_type == 'New Registration':

        proxy = db.engine.execute(text("""
                                       SELECT DATE(on_day),
                                              new_registration
                                       FROM   bi_statistic
                                       WHERE  on_day BETWEEN :start_time AND :end_time
                                              AND game = :game
                                              AND platform = :platform
                                       """), start_time=start_time, end_time=end_time, game='All Game',
                                  platform=platform)

    elif report_type == 'Game DAU':

        proxy = db.engine.execute(text("""
                                       SELECT DATE(on_day),
                                              dau
                                       FROM   bi_statistic
                                       WHERE  on_day BETWEEN :start_time AND :end_time
                                              AND game = :game
                                              AND platform = :platform
                                       """), start_time=start_time, end_time=end_time, game=game,
                                  platform='All Platform')

    elif report_type == 'WAU':

        proxy = db.engine.execute(text("""
                                       SELECT DATE(on_day),
                                              wau
                                       FROM   bi_statistic
                                       WHERE  on_day BETWEEN :start_time AND :end_time
                                              AND game = :game
                                              AND platform = :platform
                                       """), start_time=start_time, end_time=end_time, game=game,
                                  platform='All Platform')

    elif report_type == 'MAU':

        proxy = db.engine.execute(text("""
                                       SELECT DATE(on_day),
                                              mau
                                       FROM   bi_statistic
                                       WHERE  on_day BETWEEN :start_time AND :end_time
                                              AND game = :game
                                              AND platform = :platform
                                        """), start_time=start_time, end_time=end_time, game=game,
                                  platform='All Platform')

    elif report_type == 'New Reg Game DAU':

        proxy = db.engine.execute(text("""
                                       SELECT DATE(on_day),
                                              new_registration_game_dau
                                       FROM   bi_statistic
                                       WHERE  on_day BETWEEN :start_time AND :end_time
                                              AND game = :game
                                              AND platform = :platform
                                        """), start_time=start_time, end_time=end_time, game='All Game',
                                  platform='All Platform')

    elif report_type == 'Revenue':
        proxy = db.engine.execute(text("""
                                       SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                              ROUND(SUM(currency_amount), 2)
                                       FROM   bi_user_bill
                                       WHERE  currency_type = 'Dollar'
                                              AND DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) BETWEEN
                                                  :start_time AND :end_time
                                       GROUP  BY on_day
                                       """), start_time=start_time, end_time=end_time)

    labels = []
    data = []

    for row in proxy:
        labels.append(arrow.get(row[0]).format('YYYY-MM-DD'))
        data.append(row[1])

    # if request.args.file_type == 'excel':
    #     output = BytesIO()
    #     column_names = ['Datetime', report_type]
    #
    #     book = xlsxwriter.Workbook(output, {'in_memory': True})
    #     sheet = book.add_worksheet("result")
    #     sheet.write_row('A1', column_names)
    #     sheet.write_column('A2', labels)
    #     sheet.write_column('B2', data)
    #     book.close()
    #
    #     output.seek(0)
    #
    #     return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #                      as_attachment=True,
    #                      attachment_filename="{0}/{1}-{2}.xlsx".format(start_time, end_time, report_type))

    json_response = jsonify(labels=labels, data=data, game=game, platform=platform)
    return json_response
