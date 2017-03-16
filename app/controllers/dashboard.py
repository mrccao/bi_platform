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
                                                 SELECT new_registration
                                                 FROM   bi_statistic
                                                 WHERE  platform = 'All Platform'
                                                 AND on_day = :day"""), day=day).scalar()

    revenue = db.engine.execute(text( """
                                         SELECT revenue
                                         FROM bi_statistic
                                         WHERE on_day= :day"""), day=day).scalar()

    game_dau = db.engine.execute(text("""
                                          SELECT dau
                                          FROM   bi_statistic
                                          WHERE game='All Game'
                                          AND on_day = :day  """), day=day).scalar()

    new_registration_game_dau = db.engine.execute(text("""
                                                          SELECT new_registration_game_dau
                                                          FROM   bi_statistic
                                                          WHERE  on_day= :day """), day=day).scalar()

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
               SELECT on_day,
                      new_registration
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game='All Game', platform=platform)

    elif report_type == 'Game DAU':

        proxy = db.engine.execute(text("""
               SELECT on_day,
                      dau
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game=game, platform='All Platform')

    elif report_type == 'WAU':

        proxy = db.engine.execute(text("""
               SELECT on_day,
                      wau
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game=game, platform='All Platform')

    elif report_type == 'MAU':

        proxy = db.engine.execute(text("""
               SELECT on_day,
                      mau
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game=game, platform='All Platform')

    elif report_type == 'New Reg Game DAU':

        proxy = db.engine.execute(text("""
               SELECT on_day,
                      new_registration_game_dau
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game='All Game', platform='All Platform')

    elif report_type == 'Revenue':

        proxy = db.engine.execute(text("""
               SELECT on_day,
                      dollar_paid_amount
               FROM   bi_statistic
               WHERE  DATE(on_day) BETWEEN :start_time AND :end_time
                      AND game = :game
                      AND platform = :platform
                """), start_time=start_time, end_time=end_time, game='All Game', platform='All Platform')

    labels = []
    data = []

    for row in proxy:
        labels.append(arrow.get(row[0]).format('YYYY-MM-DD'))
        data.append(row[1])

    return jsonify(labels=labels, data=data, game=game, platform=platform)
