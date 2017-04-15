from datetime import datetime
from operator import attrgetter

from flask import Blueprint, render_template, jsonify
from flask import current_app as app
from flask_login import login_required
from numpy import array
from sqlalchemy import and_
from sqlalchemy.sql import text

from app.extensions import db
from app.models.bi import BIStatistic
from app.utils import current_time

report = Blueprint('report', __name__)


@report.route("/report/daily_summary", methods=["GET"])
@login_required
def daily_summary():
    return render_template('report/daily_summary.html')


@report.route("/report/daily_summary_data", methods=["GET"])
@login_required
def daily_summary_data():
    now = current_time(app.config['APP_TIMEZONE'])
    start_time = now.replace(days=-30).format('YYYY-MM-DD')
    end_time = now.format('YYYY-MM-DD')

    get_metrics = attrgetter('on_day', 'dau', 'wau', 'mau', 'one_day_retention', 'seven_day_retention',
                             'thirty_day_retention', 'paid_user_count', 'paid_count', 'revenue',
                             'new_reg', 'email_reg', 'email_validate', 'new_reg_game_dau', 'free_silver', 'free_gold')

    metrics = get_metrics(BIStatistic)
    query = db.session.query(BIStatistic).with_entities(*metrics)

    query_result = query.filter(and_(BIStatistic.on_day >= start_time,
                                     BIStatistic.on_day <= end_time,
                                     BIStatistic.game == 'All Game',
                                     BIStatistic.platform == 'All Platform'))

    transpose_query_result = list(map(list, zip(*query_result)))

    dau = transpose_query_result[1]
    one_day_retention_count = transpose_query_result[4]
    seven_day_retention_count = transpose_query_result[5]
    thirty_day_retention_count = transpose_query_result[6]
    paid_user_count = transpose_query_result[7]
    revenue = transpose_query_result[9]
    new_reg = transpose_query_result[10]
    email_reg = transpose_query_result[11]
    new_reg_game_dau = transpose_query_result[13]

    cumulative_revenue_sum = db.session.query('cumulative_revenue_sum').from_statement(
        text(""" 
            SELECT (SELECT Sum(x.revenue)
                    FROM   bi_statistic x
                    WHERE  x.on_day < t.on_day
                           AND x.game = 'All Game'
                           AND x.platform = 'All Platform') AS cumulative_revenue_sum
            FROM   bi_statistic t
            WHERE  t.game = 'All Game'
                   AND t.platform = 'All Platform'
                   AND t.on_day >= :start_time
                   AND on_day <= :end_time 
                         """
             )).params(start_time=start_time, end_time=end_time).all()

    cumulative_revenue_sum = [int(each_cumulative_revenue_sum[0]) for each_cumulative_revenue_sum in
                              cumulative_revenue_sum]

    cumulative_user_sum = db.session.query('cumulative_user_sum').from_statement(
        text(""" 
            SELECT (SELECT Sum(x.new_reg)
                    FROM   bi_statistic x
                    WHERE  x.on_day < t.on_day
                           AND x.game = 'All Game'
                           AND x.platform = 'All Platform') AS cumulative_user_sum
            FROM   bi_statistic t
            WHERE  t.game = 'All Game'
                   AND t.platform = 'All Platform'
                   AND t.on_day >= :start_time
                   AND on_day <= :end_time 
             """
             )).params(start_time=start_time, end_time=end_time).all()

    cumulative_user_sum = [int(each_cumulative_user_sum[0]) for each_cumulative_user_sum in cumulative_user_sum]

    #  calculate_extra_metrics

    facebook_reg = array(new_reg) - array(email_reg)
    reg_retention = array(new_reg_game_dau) / array(new_reg)
    ARPDAU = array(revenue) / array(dau)
    ARPPU = array(revenue) / array(paid_user_count)
    ARPU = array(cumulative_revenue_sum) / array(cumulative_user_sum)
    one_day_retention = array(one_day_retention_count) / array(new_reg_game_dau)
    seven_day_retention = array(seven_day_retention_count) / array(new_reg_game_dau)
    thirty_day_retention = array(thirty_day_retention_count) / array(new_reg_game_dau)

    extra_metrics = [facebook_reg, reg_retention, ARPDAU, ARPPU, ARPU]
    transpose_extra_operational_metrics = list(map(list, zip(*extra_metrics)))

    # process charts


    column_names = [column["name"] for column in query.column_descriptions]
    column_names.extend(['facebook_reg', 'reg_retention', 'ARPDAU', 'ARPPU', 'ARPU'])
    charts_legend = column_names[1:]

    charts_data = transpose_query_result
    charts_labels = [datetime.strftime(day, "%Y-%m-%d") for day in charts_data[0]]
    charts_data[4:7] = [one_day_retention, seven_day_retention, thirty_day_retention]
    charts_data = charts_data[1:]
    charts_data.extend(extra_metrics)

    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)

    # process tables

    tables_title = [{'title': column_name} for column_name in column_names]

    tables_data = [row for row in query_result]
    transpose_tables_data = list(map(list, zip(*tables_data)))
    transpose_tables_data[0] = charts_labels
    transpose_tables_data[4:7] = [one_day_retention, seven_day_retention_count, thirty_day_retention_count]
    transpose_tables_data.extend(extra_metrics)

    tables_data = list(map(list, zip(*transpose_tables_data)))

    tables_result = dict(tables_title=tables_title, tables_data=tables_data)

    return jsonify(charts_result=charts_result, tables_result=tables_result)
