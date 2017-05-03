import datetime
from decimal import Decimal

import arrow
from flask import Blueprint, render_template, jsonify, request
from flask import current_app as app
from numpy import array
from sqlalchemy import and_, func, text

from app.extensions import db
from app.models.bi import BIStatistic
from app.utils import current_time
from app.utils import generate_date_range_group_by_daily_or_weekly_or_monthly
from  app.utils import get_day_range_of_month

report = Blueprint('report', __name__)


@report.route("/report/metrics_summary", methods=["GET"])
# @login_required
def metrics_summary():
    return render_template('report/metrics_summary.html')


@report.route("/report/metrics_summary_data", methods=["GET"])
# @login_required
def metrics_summary_data():
    # get custom date range
    days_ago = request.args.get('days_ago')
    game = request.args.get("game")
    platform = request.args.get("platform")
    group_type = request.args.get("group")
    # get default date range
    start_time, end_time = request.args.get('date_range').split('  -  ')

    if group_type == 'Weekly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

    if group_type == 'Monthly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

    if days_ago:
        now = current_time(app.config['APP_TIMEZONE'])
        start_time = now.replace(days=-int(days_ago)).format('YYYY-MM-DD')
        end_time = now.replace(days=-1).format('YYYY-MM-DD')
        start_time, end_time = generate_date_range_group_by_daily_or_weekly_or_monthly(start_time, end_time, group_type)

    start_time, end_time = generate_date_range_group_by_daily_or_weekly_or_monthly(start_time, end_time, group_type)

    if group_type == 'Weekly':
        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%u') AS  week,
                                             AVG(dau),
                                             AVG(wau),
                                             AVG(mau),
                                             AVG(facebook_game_reg),
                                             AVG(facebook_login_reg),
                                             AVG(guest_reg),
                                             AVG(email_reg),
                                             AVG(new_reg_game_dau),
                                             AVG(paid_user_count),
                                             AVG(paid_count),
                                             AVG(revenue),
                                             AVG(email_validated),
                                             AVG(mtt_buy_ins),
                                             AVG(sng_buy_ins),
                                             AVG(mtt_rake),
                                             AVG(sng_rake),
                                             AVG(ring_game_rake),
                                             AVG(mtt_winnings),
                                             AVG(sng_winnings)
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                      GROUP  BY  week
                                      HAVING week BETWEEN :start_time AND :end_time
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    elif group_type == 'Monthly':
        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%m') AS  month,
                                             AVG(dau),
                                             AVG(wau),
                                             AVG(mau),
                                             AVG(facebook_game_reg),
                                             AVG(facebook_login_reg),
                                             AVG(guest_reg),
                                             AVG(email_reg),
                                             AVG(new_reg_game_dau),
                                             AVG(paid_user_count),
                                             AVG(paid_count),
                                             AVG(revenue),
                                             AVG(email_validated),
                                             AVG(mtt_buy_ins),
                                             AVG(sng_buy_ins),
                                             AVG(mtt_rake),
                                             AVG(sng_rake),
                                             AVG(ring_game_rake),
                                             AVG(mtt_winnings),
                                             AVG(sng_winnings)
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
# @login_required
def reg_platform():
    return render_template('report/reg_platform_distributed.html')


@report.route("/report/reg_platform_data", methods=["GET"])
# @login_required
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
# @login_required
def user_reg_data():
    now = current_time(app.config['APP_TIMEZONE'])
    previous_month_start_day, previous_month_end_day, current_month_start_day, current_month_end_day = get_day_range_of_month(
        now)
    platform = request.args.get('platform', 'All Platform')

    def process_user_reg(start_time, end_time):

        query_result = db.engine.execute(text("""
                                          SELECT DATE_FORMAT(on_day,'%Y-%m-%d') AS  on_day,
                                                 email_reg,
                                                 guest_reg,
                                                 facebook_game_reg,
                                                 facebook_login_reg
                                          FROM   bi_statistic
                                          WHERE  platform = :platform
                                                 AND  game = 'All Game'
                                                 AND on_day BETWEEN :start_time AND :end_time
                                          """), platform=platform, start_time=start_time,
                                         end_time=end_time)

        result_line = list(map(list, zip(*query_result)))
        data = result_line[1:]
        day_range = result_line[0]

        query_result = db.engine.execute(text("""
                                          SELECT platform                    AS  name,
                                                 SUM(new_reg)                AS  value
                                          FROM   bi_statistic
                                          WHERE  game = 'All Game'
                                                 AND platform !='All Platform'
                                                 AND on_day BETWEEN :start_time AND :end_time
                                          GROUP  BY platform, 
                                                 DATE_FORMAT(on_day,'%Y-%m') 
                                          """), start_time=start_time,
                                         end_time=end_time)

        each_platform_reg_count_proxy = []

        for row in query_result:
            row_as_dict = dict(row)
            each_platform_reg_count_proxy.append(row_as_dict)

        query_result = db.engine.execute(text("""
                                        SELECT 'email_reg'    AS name,
                                               SUM(email_reg) AS value
                                        FROM   bi_statistic
                                        WHERE  game = 'All Game'
                                               AND platform = 'All Platform'
                                               AND on_day BETWEEN :start_time AND :end_time
                                        GROUP  BY DATE_FORMAT(on_day, '%Y-%m')
                                        UNION
                                        SELECT 'guest_reg'    AS name,
                                               SUM(guest_reg) AS value
                                        FROM   bi_statistic
                                        WHERE  game = 'All Game'
                                               AND platform = 'All Platform'
                                               AND on_day BETWEEN :start_time AND :end_time
                                        GROUP  BY DATE_FORMAT(on_day, '%Y-%m')
                                        UNION
                                        SELECT 'facebook_login_reg' AS name,
                                               SUM(email_reg)       AS value
                                        FROM   bi_statistic
                                        WHERE  game = 'All Game'
                                               AND platform = 'All Platform'
                                               AND on_day BETWEEN :start_time AND :end_time
                                        GROUP  BY DATE_FORMAT(on_day, '%Y-%m')
                                        UNION
                                        SELECT 'facebook_game_reg' AS name,
                                               SUM(email_reg)      AS value
                                        FROM   bi_statistic
                                        WHERE  game = 'All Game'
                                               AND platform = 'All Platform'
                                               AND on_day BETWEEN :start_time AND :end_time
                                        GROUP  BY DATE_FORMAT(on_day, '%Y-%m')  
                                          """), start_time=start_time,
                                         end_time=start_time)

        each_reg_method_count_proxy = []
        for row in query_result:
            row_as_dict = dict(row)
            each_reg_method_count_proxy.append(row_as_dict)

        return data, day_range, each_platform_reg_count_proxy, each_reg_method_count_proxy

    current_month_data, current_month_day_range, current_month_each_platform_reg_count_proxy, \
    current_month_each_reg_method_count_proxy = process_user_reg(previous_month_start_day, previous_month_end_day)

    previous_month_data, previous_month_day_range, previous_each_platform_reg_count_proxy, \
    previous_each_reg_method_count_proxy = process_user_reg(current_month_start_day, current_month_end_day)

    return jsonify(current_month_data=current_month_data, current_month_day_range=current_month_day_range,
                   current_month_each_platform_reg_count_proxy=current_month_each_platform_reg_count_proxy,
                   current_month_each_reg_method_count_proxy=current_month_each_reg_method_count_proxy,
                   previous_month_data=previous_month_data, previous_month_day_range=previous_month_day_range,
                   previous_each_platform_reg_count_proxy=previous_each_platform_reg_count_proxy,
                   previous_each_reg_method_count_proxy=previous_each_reg_method_count_proxy)

