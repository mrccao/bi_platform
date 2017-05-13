import datetime
from copy import copy
from decimal import Decimal

import arrow
from flask import current_app as app
from flask import render_template, jsonify, request
from flask_login import login_required
from numpy import array, isnan
from sqlalchemy import text

from app.extensions import db
from app.utils import current_time
from . import report


@report.route("/report/daily_summary", methods=["GET"])
@login_required
def daily_summary():
    return render_template('report/daily_summary.html')


@report.route("/report/daily_summary_data", methods=["GET"])
@login_required
def daily_summary_data():
    game = request.args.get("game")
    platform = request.args.get("platform")
    group_type = request.args.get("group")
    now = current_time(app.config['APP_TIMEZONE'])

    start_time = now.replace(days=-int(60)).format('YYYY-MM-DD')
    end_time = now.replace(days=-1).format('YYYY-MM-DD')

    if group_type == 'Weekly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%u') AS  week,
                                             ROUND(AVG(dau),0),
                                             ROUND(AVG(wau),0),
                                             ROUND(AVG(mau),0),
                                             ROUND(AVG(facebook_game_reg),0),
                                             ROUND(AVG(facebook_login_reg),0),
                                             ROUND(AVG(guest_reg),0),
                                             ROUND(AVG(email_reg),0),
                                             ROUND(AVG(email_validated),0),
                                             ROUND(AVG(new_reg),0),
                                             ROUND(AVG(new_reg_game_dau),0),
                                             ROUND(AVG(paid_user_count),0),
                                             ROUND(AVG(paid_count),0),
                                             ROUND(AVG(revenue),0),
                                             ROUND(AVG(one_day_retention),0),
                                             ROUND(AVG(seven_day_retention),0),
                                             ROUND(AVG(thirty_day_retention),0),
                                             ROUND(AVG(mtt_buy_ins),0),
                                             ROUND(AVG(sng_buy_ins),0),
                                             ROUND(AVG(mtt_rake),0),
                                             ROUND(AVG(sng_rake),0),
                                             ROUND(AVG(ring_game_rake),0),
                                             ROUND(AVG(mtt_winnings),0),
                                             ROUND(AVG(sng_winnings),0),
                                             ROUND(AVG(free_gold),0),
                                             ROUND(AVG(free_silver),0)
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                             AND on_day BETWEEN :start_time AND :end_time
                                      GROUP  BY  week
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    elif group_type == 'Monthly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        query_result = db.engine.execute(text("""
                                      SELECT DATE_FORMAT(on_day,'%Y-%m') AS  month,
                                             ROUND(AVG(dau),0),
                                             ROUND(AVG(wau),0),
                                             ROUND(AVG(mau),0),
                                             ROUND(AVG(facebook_game_reg),0),
                                             ROUND(AVG(facebook_login_reg),0),
                                             ROUND(AVG(guest_reg),0),
                                             ROUND(AVG(email_reg),0),
                                             ROUND(AVG(email_validated),0),
                                             ROUND(AVG(new_reg),0),
                                             ROUND(AVG(new_reg_game_dau),0),
                                             ROUND(AVG(paid_user_count),0),
                                             ROUND(AVG(paid_count),0),
                                             ROUND(AVG(revenue),0),
                                             ROUND(AVG(one_day_retention),0),
                                             ROUND(AVG(seven_day_retention),0),
                                             ROUND(AVG(thirty_day_retention),0),
                                             ROUND(AVG(mtt_buy_ins),0),
                                             ROUND(AVG(sng_buy_ins),0),
                                             ROUND(AVG(mtt_rake),0),
                                             ROUND(AVG(sng_rake),0),
                                             ROUND(AVG(ring_game_rake),0),
                                             ROUND(AVG(mtt_winnings),0),
                                             ROUND(AVG(sng_winnings),0),
                                             ROUND(AVG(free_gold),0),
                                             ROUND(AVG(free_silver),0)
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                             AND on_day BETWEEN :start_time AND :end_time
                                      GROUP  BY month 
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
                                             email_validated,
                                             new_reg,
                                             new_reg_game_dau,
                                             paid_user_count,
                                             paid_count,
                                             revenue,
                                             one_day_retention,
                                             seven_day_retention,
                                             thirty_day_retention ,
                                             mtt_buy_ins,
                                             sng_buy_ins,
                                             mtt_rake,
                                             sng_rake,
                                             ring_game_rake,
                                             mtt_winnings,
                                             sng_winnings,
                                             free_gold,
                                             free_silver
                                      FROM   bi_statistic
                                      WHERE  platform = :platform
                                             AND game = :game
                                             AND on_day BETWEEN :start_time AND :end_time
                                      """), platform=platform, game=game, start_time=start_time, end_time=end_time)

    query_result = list(query_result)
    transpose_query_result = list(map(list, zip(*query_result)))
    charts_data = transpose_query_result

    column_names = ['dau', 'wau', 'mau', 'facebook_game_reg', 'facebook_login_reg', 'guest_reg', 'email_reg',
                    'email_validated', 'new_reg', 'new_reg_game_dau', 'paid_user_count', 'paid_count', 'revenue',
                    'one_day_retention(%)', 'seven_day_retention(%)', 'thirty_day_retention(%)', 'mtt_buy_ins',
                    'sng_buy_ins', 'mtt_rake', 'sng_rake', 'ring_game_rake', 'mtt_winnings', 'sng_winnings',
                    'free_gold', 'free_silver', 'stickiness_weekly', 'stickiness_monthly', 'ARPPU']

    tables_columns_names = copy(column_names)
    tables_columns_names.insert(0, 'date')

    new_reg_game_dau = transpose_query_result[10]
    one_day_retention_count = transpose_query_result[14]
    seven_day_retention_count = transpose_query_result[15]
    thirty_day_retention_count = transpose_query_result[16]

    # calculate  retention

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

    charts_data[14:17] = [one_day_retention, seven_day_retention, thirty_day_retention]

    dau = transpose_query_result[1]
    wau = transpose_query_result[2]
    mau = transpose_query_result[3]
    paid_user_count = transpose_query_result[11]
    revenue = [Decimal(row) for row in transpose_query_result[13]]

    # calculate  Compound metrics

    try:

        stickiness_weekly = list(map(lambda i: 0 if isnan(i)  else round(i, 2), array(dau) / array(wau)))
        stickiness_monthly = list(map(lambda i: 0 if isnan(i)  else round(i, 2), array(dau) / array(mau)))
        # ARPDAU = [int(round(i, 2)) for i in array(revenue) / array(dau)]
        ARPPU = [int(round(i, 2)) for i in array(revenue) / array(paid_user_count)]
    except Exception:
        stickiness_weekly = [0 for i in range(len(dau))]
        stickiness_monthly = [0 for i in range(len(mau))]
        # ARPDAU = [0 for i in range(len(dau))]
        ARPPU = [0 for i in range(len(dau))]

    compound_metrics = [stickiness_weekly, stickiness_monthly, ARPPU]
    # process charts
    charts_data.extend(compound_metrics)
    charts_labels = charts_data[0]
    charts_data = charts_data[1:]
    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=column_names)

    # process tables


    tables_title = [{'title': column_name} for column_name in tables_columns_names]

    tables_data = list(map(list, zip(*transpose_query_result)))

    tables_result = dict(tables_title=tables_title, tables_data=tables_data)
    return jsonify(charts_result=charts_result, tables_result=tables_result)
