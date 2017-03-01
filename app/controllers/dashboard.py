import arrow

from flask import current_app as app
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from sqlalchemy import text, and_, or_

from app.utils import current_time, get_last_day_of_prev_month
from app.extensions import db
from app.constants import FREE_TRANSACTION_TYPES

dashboard = Blueprint('dashboard', __name__)

@dashboard.route("/", methods=["GET"])
@login_required
def index():
    return render_template('dashboard/index.html')


@dashboard.route("/dashboard/visualization/summary_data", methods=["GET"])
@login_required
def visualization_summary_data():
    now = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')
    index_time = current_time().replace(days=-2).format('YYYY-MM-DD')

    new_registration = db.engine.execute(text("SELECT COUNT(*) FROM bi_user WHERE DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) = :now"), now=now).scalar()
    revenue = db.engine.execute(text("SELECT ROUND(SUM(currency_amount), 2) FROM bi_user_bill WHERE currency_type = 'Dollar' AND DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) = :now"), now=now).scalar()
    # game_dau = db.engine.execute(text("""
    #                                   SELECT Count(DISTINCT user_id)
    #                                   FROM   bi_user_currency
    #                                   WHERE  created_at > :index_time
    #                                          AND transaction_type NOT IN :transaction_type
    #                                          AND Date(Convert_tz(created_at, '+00:00', '-05:00')) = :now 
    #                                   """), now=now, index_time=index_time, transaction_type=tuple(FREE_TRANSACTION_TYPES)).scalar()
    # new_registration_game_dau = db.engine.execute(text("""
    #                                                    SELECT COUNT(DISTINCT uc.user_id)
    #                                                    FROM   bi_user u
    #                                                           LEFT JOIN bi_user_currency uc
    #                                                             ON u.user_id = uc.user_id
    #                                                    WHERE  uc.transaction_type NOT IN :transaction_type
    #                                                           AND DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) = :now 
    #                                                    """), now=now, transaction_type=tuple(FREE_TRANSACTION_TYPES)).scalar()

    game_dau = db.engine.execute(text("""
                                      SELECT Count(DISTINCT user_id)
                                      FROM   bi_user_currency
                                      WHERE  created_at > :index_time
                                             AND Date(Convert_tz(created_at, '+00:00', '-05:00')) = :now 
                                      """), now=now, index_time=index_time).scalar()
    new_registration_game_dau = db.engine.execute(text("""
                                                       SELECT COUNT(DISTINCT uc.user_id)
                                                       FROM   bi_user u
                                                              LEFT JOIN bi_user_currency uc
                                                                ON u.user_id = uc.user_id
                                                       WHERE  DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) = :now 
                                                       """), now=now).scalar()

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
        now = current_time().to(app.config['APP_TIMEZONE'])
        end_time = now.replace(days=-1).format('YYYY-MM-DD')
        start_time = now.replace(days=-int(days_ago)).format('YYYY-MM-DD')

    game = request.args.get('game')
    platform = request.args.get('platform')

    report_type = request.args.get('report_type')

    proxy = []
    if report_type == 'New Registration':
        proxy = db.engine.execute(text("""
                                       SELECT DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) AS on_day,
                                              COUNT(*)
                                       FROM   bi_user
                                       WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', '-05:00')) BETWEEN
                                              :start_time AND :end_time
                                       GROUP  BY on_day
                                       """), start_time=start_time, end_time=end_time)
        # game = 'All Game'
        # proxy = db.engine.execute(text("""
        #                                SELECT day, new_registration
        #                                FROM   bi_statistic
        #                                WHERE  DATE(day) BETWEEN :start_time AND :end_time AND game = :game AND platform = :platform
        #                                """), start_time=start_time, end_time=end_time, game=game, platform=platform)
    elif report_type == 'Game DAU':
        index_time = arrow.get(start_time).replace(days=-2).format('YYYY-MM-DD')
        # proxy = db.engine.execute(text("""
        #                                SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
        #                                       COUNT(DISTINCT user_id)
        #                                FROM   bi_user_currency
        #                                WHERE  created_at > :index_time
        #                                       AND transaction_type NOT IN :transaction_type
        #                                       AND DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) BETWEEN :start_time AND :end_time
        #                                GROUP  BY on_day
        #                                """), start_time=start_time, end_time=end_time, index_time=index_time, transaction_type=tuple(FREE_TRANSACTION_TYPES))
        proxy = db.engine.execute(text("""
                               SELECT DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) AS on_day,
                                      COUNT(DISTINCT user_id)
                               FROM   bi_user_currency
                               WHERE  created_at > :index_time
                                      AND DATE(CONVERT_TZ(created_at, '+00:00', '-05:00')) BETWEEN :start_time AND :end_time
                               GROUP  BY on_day
                               """), start_time=start_time, end_time=end_time, index_time=index_time)
        # platform = 'All Platform'
        # proxy = db.engine.execute(text("""
        #                                SELECT day, dau
        #                                FROM   bi_statistic
        #                                WHERE  DATE(day) BETWEEN :start_time AND :end_time AND game = :game AND platform = :platform
        #                                """), start_time=start_time, end_time=end_time, game=game, platform=platform)
    # elif report_type == 'WAU':
    #     proxy = db.engine.execute(text("""
    #                                    SELECT CONCAT(DATE_FORMAT(DATE_ADD(CONVERT_TZ(created_at, '+00:00', '-05:00'), INTERVAL(1-DAYOFWEEK(CONVERT_TZ(created_at, '+00:00', '-05:00'))) DAY), '%Y-%m-%d'), 
    #                                                  '  -  ', 
    #                                                  DATE_FORMAT(DATE_ADD(CONVERT_TZ(created_at, '+00:00', '-05:00'), INTERVAL(7-DAYOFWEEK(CONVERT_TZ(created_at, '+00:00', '-05:00'))) DAY), '%Y-%m-%d')) as on_week,
    #                                           COUNT(DISTINCT user_id)
    #                                    FROM   bi_user_currency
    #                                    GROUP  BY on_week
    #                                    """))
    elif report_type == 'MAU':
        proxy = db.engine.execute(text("""
                                       SELECT DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '-05:00'), '%Y-%m') AS on_month,
                                              COUNT(DISTINCT user_id)
                                       FROM   bi_user_currency
                                       GROUP  BY on_month
                                       """))
        # platform = 'All Platform'
        # today = current_time().to(app.config['APP_TIMEZONE']).format('YYYY-MM-DD')
        # last_day_of_prev_month = get_last_day_of_prev_month().format('YYYY-MM-DD')
        # proxy = db.engine.execute(text("""
        #                                SELECT DISTINCT(LAST_DAY(day)), mau
        #                                FROM   bi_statistic
        #                                WHERE  game = :game AND platform = :platform AND LAST_DAY(day) <= :last_day_of_prev_month
        #                                UNION
        #                                SELECT day, mau
        #                                FROM   bi_statistic
        #                                WHERE  game = :game AND platform = :platform AND day = :today
        #                                """), last_day_of_prev_month=last_day_of_prev_month, today=today, game=game, platform=platform)
    elif report_type == 'New Reg Game DAU':
        # proxy = db.engine.execute(text("""
        #                                SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
        #                                       COUNT(DISTINCT uc.user_id)
        #                                FROM   bi_user u
        #                                       LEFT JOIN bi_user_currency uc
        #                                              ON u.user_id = uc.user_id
        #                                WHERE  uc.transaction_type NOT IN :transaction_type
        #                                       AND DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) BETWEEN :start_time AND :end_time
        #                                GROUP  BY on_day 
        #                                """), start_time=start_time, end_time=end_time, transaction_type=tuple(FREE_TRANSACTION_TYPES))
        proxy = db.engine.execute(text("""
                                       SELECT DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) AS on_day,
                                              COUNT(DISTINCT uc.user_id)
                                       FROM   bi_user u
                                              LEFT JOIN bi_user_currency uc
                                                     ON u.user_id = uc.user_id
                                       WHERE  DATE(CONVERT_TZ(u.reg_time, '+00:00', '-05:00')) BETWEEN :start_time AND :end_time
                                       GROUP  BY on_day 
                                       """), start_time=start_time, end_time=end_time)
        # platform = 'All Platform'
        # proxy = db.engine.execute(text("""
        #                                SELECT day, new_registration_dau
        #                                FROM   bi_statistic
        #                                WHERE  DATE(day) BETWEEN :start_time AND :end_time AND game = :game AND platform = :platform
        #                                """), start_time=start_time, end_time=end_time, game=game, platform=platform)
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
        # game = 'All Game'
        # platform = 'All Platform'
        # proxy = db.engine.execute(text("""
        #                                SELECT day, dollar_paid_succeeded_amount
        #                                FROM   bi_statistic
        #                                WHERE  DATE(day) BETWEEN :start_time AND :end_time AND game = :game AND platform = :platform
        #                                """), start_time=start_time, end_time=end_time, game=game, platform=platform)

    labels = []
    data = []
    for row in proxy:
        labels.append(arrow.get(row[0]).format('YYYY-MM-DD'))
        data.append(row[1])
    return jsonify(labels=labels, data=data, game=game, platform=platform)
