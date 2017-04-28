from datetime import datetime

from flask import Blueprint, render_template, jsonify
from flask import current_app as app
from flask_login import login_required
from numpy import array, isnan
from operator import attrgetter
from sqlalchemy import and_

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
    start_time = now.replace(days=-60).format('YYYY-MM-DD')
    end_time = now.format('YYYY-MM-DD')

    get_metrics = attrgetter('on_day', 'dau', 'wau', 'mau', 'new_reg', 'facebook_game_reg', 'facebook_login_reg',
                             'guest_reg', 'new_reg_game_dau', 'paid_user_count', 'paid_count', 'revenue',
                             'one_day_retention', 'seven_day_retention', 'thirty_day_retention', 'free_gold',
                             'free_silver', 'ring_game_rake', 'sng_winnings', 'mtt_winnings')

    metrics = get_metrics(BIStatistic)
    query = db.session.query(BIStatistic).with_entities(*metrics)

    query_result = query.filter(and_(BIStatistic.on_day >= start_time,
                                     BIStatistic.on_day < end_time,
                                     BIStatistic.game == 'All Game',
                                     BIStatistic.platform == 'All Platform'))

    transpose_query_result = list(map(list, zip(*query_result)))

    dau = transpose_query_result[1]
    wau = transpose_query_result[2]
    mau = transpose_query_result[3]
    new_reg_game_dau = transpose_query_result[8]
    paid_user_count = transpose_query_result[9]
    revenue = transpose_query_result[11]
    one_day_retention_count = transpose_query_result[12]
    seven_day_retention_count = transpose_query_result[13]
    thirty_day_retention_count = transpose_query_result[14]

    # calculate  Compound metrics


    # email_reg = array(new_reg) - array(facebook_game_reg)- array(facebook_login_reg) -array(guest_reg)
    one_day_retention = list(map(lambda i: 0 if isnan(i)  else int(round(i, 2) * 100),
                                 array(one_day_retention_count) / array(new_reg_game_dau)))
    seven_day_retention = list(map(lambda i: 0 if isnan(i) else int(round(i, 2) * 100),
                                   array(seven_day_retention_count) / array(new_reg_game_dau)))
    thirty_day_retention = list(map(lambda i: 0 if isnan(i) else int(round(i, 2) * 100),
                                    array(thirty_day_retention_count) / array(new_reg_game_dau)))

    stickiness_weekly = list(map(lambda i: 0 if isnan(i) else int(round(i, 2) * 100), array(dau) / array(wau)))
    stickiness_monthly = list(map(lambda i: 0 if isnan(i) else int(round(i, 2) * 100), array(dau) / array(mau)))

    ARPDAU = list(map(lambda i: 0 if isnan(i) else int(round(i, 2)), array(revenue) / array(dau)))
    ARPPU = list(map(lambda i: 0 if isnan(i) else int(round(i, 2)), array(revenue) / array(paid_user_count)))

    compound_metrics = [stickiness_weekly, stickiness_monthly, ARPPU]

    # process charts

    column_names = [column["name"] for column in query.column_descriptions]
    column_names[12:15] = ['one_day_retention(%)', 'seven_day_retention(%)', 'thirty_day_retention(%)']
    column_names.extend(['stickiness_weekly(%)', 'stickiness_monthly(%)', 'ARPPU'])

    charts_legend = column_names[1:]

    charts_data = transpose_query_result
    charts_labels = [datetime.strftime(day, "%Y-%m-%d") for day in charts_data[0]]
    charts_data[12:15] = [one_day_retention, seven_day_retention, thirty_day_retention]
    charts_data = charts_data[1:]
    charts_data.extend(compound_metrics)

    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)

    # process tables

    tables_title = [{'title': column_name} for column_name in column_names]

    tables_data = [row for row in query_result]
    transpose_tables_data = list(map(list, zip(*tables_data)))
    transpose_tables_data[0] = charts_labels

    transpose_tables_data[12:15] = [one_day_retention, seven_day_retention, thirty_day_retention]
    transpose_tables_data.extend(compound_metrics)

    tables_data = list(map(list, zip(*transpose_tables_data)))

    tables_result = dict(tables_title=tables_title, tables_data=tables_data)

    return jsonify(charts_result=charts_result, tables_result=tables_result)
