from decimal import Decimal

from flask import Blueprint, render_template, jsonify, request
from flask import current_app as app
from flask_login import login_required
from numpy import array
from sqlalchemy import and_, func, text

from app.extensions import db
from app.models.bi import BIStatistic
from app.utils import current_time
from app.utils import generate_date_range_group_by_daily_or_weekly_or_monthly

report = Blueprint('report', __name__)


@report.route("/report/metrics_summary", methods=["GET"])
@login_required
def metrics_summary():
    return render_template('report/metrics_summary.html')


@report.route("/report/metrics_summary_data", methods=["GET"])
@login_required
def metrics_summary_data():
    # get custom date range
    days_ago = request.args.get('days_ago')
    game = request.args.get("game")
    platform = request.args.get("platform")
    group_type = request.args.get("group")
    # get default date range
    start_time, end_time = request.args.get('date_range').split('  -  ')

    if days_ago:
        now = current_time(app.config['APP_TIMEZONE'])
        start_time = now.replace(days=-int(days_ago)).format('YYYY-MM-DD')
        end_time = now.replace(days=-1).format('YYYY-MM-DD')
        start_time, end_time = generate_date_range_group_by_daily_or_weekly_or_monthly(start_time, end_time, group_type)

    start_time, end_time = generate_date_range_group_by_daily_or_weekly_or_monthly(start_time, end_time, group_type)

    if group_type == 'Weekly':
        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%u') AS  week,
                                             SUM(dau)/7,
                                             SUM(wau)/7,
                                             SUM(mau)/7,
                                             SUM(facebook_game_reg)/7,
                                             SUM(facebook_login_reg)/7,
                                             SUM(guest_reg)/7,
                                             SUM(email_reg)/7,
                                             SUM(new_reg_game_dau)/7,
                                             SUM(paid_user_count)/7,
                                             SUM(paid_count)/7,
                                             SUM(revenue)/7,
                                             SUM(email_validated)/7,
                                             SUM(mtt_buy_ins)/7,
                                             SUM(sng_buy_ins)/7,
                                             SUM(mtt_rake)/7,
                                             SUM(sng_rake)/7,
                                             SUM(ring_game_rake)/7,
                                             SUM(mtt_winnings)/7,
                                             SUM(sng_winnings)/7
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                      GROUP  BY  week
                                      HAVING week BETWEEN :start_time AND :end_time
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    elif group_type == 'Monthly':
        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%m') AS  month,
                                             SUM(dau)/30,
                                             SUM(wau)/30,
                                             SUM(mau)/30,
                                             SUM(facebook_game_reg)/30,
                                             SUM(facebook_login_reg)/30,
                                             SUM(guest_reg)/30,
                                             SUM(email_reg)/30,
                                             SUM(new_reg_game_dau)/30,
                                             SUM(paid_user_count)/30,
                                             SUM(paid_count)/30,
                                             SUM(revenue)/30,
                                             SUM(email_validated)/30,
                                             SUM(mtt_buy_ins)/30,
                                             SUM(sng_buy_ins)/30,
                                             SUM(mtt_rake)/30,
                                             SUM(sng_rake)/30,
                                             SUM(ring_game_rake)/30,
                                             SUM(mtt_winnings)/30,
                                             SUM(sng_winnings)/30
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                      GROUP  BY month 
                                      HAVING month BETWEEN :start_time AND :end_time
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    else:
        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%m-%d') AS  on_day,
                                             dau,
                                             wau,
                                             mau,
                                             facebook_game_reg,
                                             facebook_login_reg,
                                             guest_reg,
                                             email_reg,
                                             new_reg_game_dau,
                                             paid_user_count,
                                             paid_count,
                                             revenue,
                                             one_day_retention,
                                             seven_day_retention,
                                             thirty_day_retention ,
                                             email_validated,
                                             mtt_buy_ins,
                                             sng_buy_ins,
                                             mtt_rake,
                                             sng_rake,
                                             ring_game_rake,
                                             mtt_winnings,
                                             sng_winnings
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                             AND on_day BETWEEN :start_time AND :end_time
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    query_result = list(query_result)
    transpose_query_result = list(map(list, zip(*query_result)))
    charts_data = transpose_query_result

    if group_type == 'Daily':

        column_names = ['dau', 'wau', 'mau', 'facebook_game_reg', 'facebook_login_reg', 'guest_reg', 'email_reg',
                        'new_reg_game_dau', 'paid_user_count', 'paid_count', 'revenue', 'one_day_retention(%)',
                        'seven_day_retention(%)', 'thirty_day_retention(%)', 'email_validated', 'mtt_buy_ins',
                        'sng_buy_ins', 'mtt_rake', 'sng_rake', 'ring_game_rake', 'mtt_winnings', 'sng_winnings',
                        'stickiness_weekly', 'stickiness_monthly', 'ARPDAU', 'ARPPU']

        new_reg_game_dau = transpose_query_result[8]
        one_day_retention_count = transpose_query_result[12]
        seven_day_retention_count = transpose_query_result[13]
        thirty_day_retention_count = transpose_query_result[14]

        try:
            one_day_retention = [int(round(i, 2) * 100) for i in
                                 array(one_day_retention_count) / array(new_reg_game_dau)]
            seven_day_retention = [int(round(i, 2) * 100) for i in
                                   array(seven_day_retention_count) / array(new_reg_game_dau)]
            thirty_day_retention = [int(round(i, 2) * 100) for i in
                                    array(thirty_day_retention_count) / array(new_reg_game_dau)]

        except Exception:

            one_day_retention = [0 for i in range(len(one_day_retention_count))]
            seven_day_retention = [0 for i in range(len(seven_day_retention_count))]
            thirty_day_retention = [0 for i in range(len(thirty_day_retention_count))]

        charts_data[12:15] = [one_day_retention, seven_day_retention, thirty_day_retention]

    else:
        column_names = ['dau', 'wau', 'mau', 'facebook_game_reg', 'facebook_login_reg', 'guest_reg', 'email_reg',
                        'new_reg_game_dau', 'paid_user_count', 'paid_count', 'revenue',
                        'email_validated', 'mtt_buy_ins', 'sng_buy_ins', 'mtt_rake', 'sng_rake',
                        'ring_game_rake', 'mtt_winnings', 'sng_winnings', 'stickiness_weekly', 'stickiness_monthly',
                        'ARPDAU', 'ARPPU']

    dau = transpose_query_result[1]
    wau = transpose_query_result[2]
    mau = transpose_query_result[3]
    paid_user_count = transpose_query_result[9]
    revenue = [Decimal(row) for row in transpose_query_result[11]]

    # calculate  Compound metrics

    try:
        stickiness_weekly = array(dau) / array(wau)
        stickiness_monthly = array(dau) / array(mau)
        ARPDAU = [int(round(i, 2)) for i in array(revenue) / array(dau)]
        ARPPU = [int(round(i, 2)) for i in array(revenue) / array(paid_user_count)]
    except Exception:
        stickiness_weekly = [0 for i in range(len(dau))]
        stickiness_monthly = [0 for i in range(len(mau))]
        ARPDAU = [0 for i in range(len(dau))]
        ARPPU = [0 for i in range(len(dau))]

    compound_metrics = [stickiness_weekly, stickiness_monthly, ARPDAU, ARPPU]
    # process charts
    charts_data.extend(compound_metrics)
    charts_labels = charts_data[0]
    charts_data = charts_data[1:]
    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=column_names)

    # process tables

    tables_title = [{'title': column_name} for column_name in column_names]
    transpose_query_result[0] = charts_labels
    tables_data = list(map(list, zip(*transpose_query_result)))
    tables_result = dict(tables_title=tables_title, tables_data=tables_data)
    return jsonify(charts_result=charts_result, tables_result=tables_result)


@report.route("/report/reg_platform", methods=["GET"])
@login_required
def reg_platform():
    return render_template('report/reg_platform_distributed.html')


@report.route("/report/reg_platform_data", methods=["GET"])
@login_required
def reg_platform_data():
    now = current_time(app.config['APP_TIMEZONE'])
    start_time = now.replace(days=-30).format('YYYY-MM-DD')
    end_time = now.format('YYYY-MM-DD')

    pie_result = db.session.query(func.DATE_FORMAT(BIStatistic.on_day, '%Y-%m-%d'),
                                  BIStatistic.email_reg, BIStatistic.guest_reg,
                                  BIStatistic.facebook_game_reg, BIStatistic.facebook_login_reg).filter(
        and_(BIStatistic.on_day >= start_time, BIStatistic.on_day < end_time, BIStatistic.game == 'All Game',
             BIStatistic.platform != 'Web Mobile'))

    transpose_query_result = list(map(list, zip(*pie_result)))

    platform = ["iOS", "Android", "Web", "Facebook Game", "All Platform"]
    reg_methods = ['email_reg', 'guest_reg', 'facebook_game_reg', 'facebook_login_reg']

    day_range_duplicated = transpose_query_result[0]
    all_reg_methods = transpose_query_result[1:5]
    day_range = sorted(set(day_range_duplicated), key=day_range_duplicated.index)

    def reg_method_count_every_platform_every_day(reg_method):
        def split_query_result(reg_method):
            return [reg_method[i:i + len(platform)] for i in range(0, len(reg_method), len(platform))]

        return dict(zip(day_range, split_query_result(reg_method)))

    every_reg_method_result = map(reg_method_count_every_platform_every_day, all_reg_methods)
    result = dict(zip(reg_methods, every_reg_method_result))
    result['day_range'] = day_range

    return jsonify(result)


@report.route("/report/user_reg_data", methods=["GET"])
@login_required
def user_reg_data():
    now = current_time(app.config['APP_TIMEZONE'])
    start_time = now.replace(days=-30).format('YYYY-MM-DD')
    end_time = now.format('YYYY-MM-DD')
    platform = request.args.get("platform")

    line_result = db.session.query(func.DATE_FORMAT(BIStatistic.on_day, '%Y-%m-%d'),
                                   BIStatistic.email_reg, BIStatistic.guest_reg,
                                   BIStatistic.facebook_game_reg, BIStatistic.facebook_login_reg).filter(
        and_(BIStatistic.on_day >= start_time, BIStatistic.on_day < end_time, BIStatistic.game == 'All Game',
             BIStatistic.platform != 'Web Mobile', BIStatistic.platform == platform))

    transpose_query_result = list(map(list, zip(*line_result)))
    line_data = transpose_query_result[1:]

    return jsonify(day_range=transpose_query_result[0], line_result=line_data)
