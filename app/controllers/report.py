from flask import Blueprint, render_template, jsonify
from flask import current_app as app
from flask_login import login_required
from sqlalchemy import and_, func

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
    charts_indications = (
        func.date_format(BIStatistic.on_day, '%Y-%m-%d'), BIStatistic.dau,
        BIStatistic.wau, BIStatistic.mau, BIStatistic.new_reg_game_dau, BIStatistic.new_reg)
    charts_query = db.session.query(BIStatistic).with_entities(*charts_indications)
    charts_query_result = charts_query.filter(and_(BIStatistic.on_day > start_time, BIStatistic.on_day < end_time,
                                                   BIStatistic.game == 'All Game',
                                                   BIStatistic.platform == 'All Platform'))
    transpose_charts_query_result = list(map(list, zip(*charts_query_result)))
    # retention_rate = array(transpose_charts_query_result[4]) / array(transpose_charts_query_result[5])
    # retention_rate_percentage = ["{:.2%}".format(i) for i in retention_rate]
    charts_labels = transpose_charts_query_result[0]
    charts_legend = [column["name"] for column in charts_query.column_descriptions][1:]
    # charts_legend.append('retention_rate')
    charts_data = transpose_charts_query_result[1:]
    # charts_data.append(retention_rate)
    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)
    ###################
    tables_indications = (BIStatistic.dau, BIStatistic.wau, BIStatistic.mau,
                          BIStatistic.new_reg_game_dau, BIStatistic.new_reg)
    tables_query = db.session.query(BIStatistic).with_entities(*tables_indications)
    column_names = [column["name"] for column in tables_query.column_descriptions]
    tables_title = [{'title': column_name} for column_name in column_names]
    tables_title.insert(0, {'title': 'day'})
    tables_query_result = tables_query.filter(
        and_(BIStatistic.on_day > start_time, BIStatistic.on_day < end_time, BIStatistic.game == 'All Game',
             BIStatistic.platform == 'All Platform'))
    transpose_tables_data_query_result = list(map(list, zip(*tables_query_result)))
    transpose_tables_data_query_result.insert(0, charts_labels)
    tables_data = list(map(list, zip(*transpose_charts_query_result)))
    tables_result = dict(tables_title=tables_title, tables_data=tables_data)
    return jsonify(charts_result=charts_result, tables_result=tables_result)

#
# from datetime import datetime
#
# from flask import Blueprint, render_template, jsonify
# from flask import current_app as app
# from flask_login import login_required
# from numpy import array
# from operator import attrgetter
# from sqlalchemy import and_
#
# from app.extensions import db
# from app.models.bi import BIStatistic
# from app.utils import current_time
#
# report = Blueprint('report', __name__)
#
#
# @report.route("/report/daily_summary", methods=["GET"])
# @login_required
# def daily_summary():
#     return render_template('report/daily_summary.html')
#
#
# @report.route("/report/daily_summary_data", methods=["GET"])
# @login_required
# def daily_summary_data():
#     now = current_time(app.config['APP_TIMEZONE'])
#     start_time = now.replace(days=-60).format('YYYY-MM-DD')
#     end_time = now.format('YYYY-MM-DD')
#
#     get_metrics = attrgetter('on_day', 'dau', 'wau', 'mau', 'one_day_retention', 'seven_day_retention',
#                              'thirty_day_retention', 'paid_user_count', 'paid_count', 'revenue',
#                              'new_reg', 'email_reg', 'email_validated', 'new_reg_game_dau')
#
#     metrics = get_metrics(BIStatistic)
#     query = db.session.query(BIStatistic).with_entities(*metrics)
#
#     query_result = query.filter(and_(BIStatistic.on_day >= start_time,
#                                      BIStatistic.on_day <= end_time,
#                                      BIStatistic.game == 'All Game',
#                                      BIStatistic.platform == 'All Platform'))
#
#     transpose_query_result = list(map(list, zip(*query_result)))
#
#     one_day_retention_count = transpose_query_result[4]
#     seven_day_retention_count = transpose_query_result[5]
#     thirty_day_retention_count = transpose_query_result[6]
#
#     new_reg = transpose_query_result[10]
#     email_reg = transpose_query_result[11]
#     new_reg_game_dau = transpose_query_result[13]
#
#     facebook_reg = array(new_reg) - array(email_reg)
#
#     one_day_retention = ['{:.2f}'.format(i) for i in array(one_day_retention_count) / array(new_reg_game_dau)]
#     seven_day_retention = ['{:.2f}'.format(i) for i in array(seven_day_retention_count) / array(new_reg_game_dau)]
#     thirty_day_retention = ['{:.2f}'.format(i) for i in array(thirty_day_retention_count) / array(new_reg_game_dau)]
#
#     # process charts
#
#     column_names = [column["name"] for column in query.column_descriptions]
#     column_names.insert(13, 'facebook_reg')
#     charts_legend = column_names[1:]
#
#     charts_data = transpose_query_result
#     charts_labels = [datetime.strftime(day, "%Y-%m-%d") for day in charts_data[0]]
#     charts_data[4:7] = [one_day_retention, seven_day_retention, thirty_day_retention]
#     charts_data = charts_data[1:]
#     charts_data.insert(13, facebook_reg)
#
#     charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)
#
#     # process tables
#
#     tables_title = [{'title': column_name} for column_name in column_names]
#
#     tables_data = [row for row in query_result]
#     transpose_tables_data = list(map(list, zip(*tables_data)))
#     transpose_tables_data[0] = charts_labels
#     transpose_tables_data[4:7] = [one_day_retention, seven_day_retention_count, thirty_day_retention_count]
#     transpose_tables_data.insert(13, facebook_reg)
#
#     tables_data = list(map(list, zip(*transpose_tables_data)))
#
#     tables_result = dict(tables_title=tables_title, tables_data=tables_data)
#
#     return jsonify(charts_result=charts_result, tables_result=tables_result)

# from datetime import datetime
#
# from flask import Blueprint, render_template, jsonify
# from flask import current_app as app
# from flask_login import login_required
# from numpy import array
# from operator import attrgetter
# from sqlalchemy import and_
#
# from app.extensions import db
# from app.models.bi import BIStatistic
# from app.utils import current_time
#
# report = Blueprint('report', __name__)
#
#
# @report.route("/report/daily_summary", methods=["GET"])
# @login_required
# def daily_summary():
#     return render_template('report/daily_summary.html')
#
#
# @report.route("/report/daily_summary_data", methods=["GET"])
# @login_required
# def daily_summary_data():
#     now = current_time(app.config['APP_TIMEZONE'])
#     start_time = now.replace(days=-60).format('YYYY-MM-DD')
#     end_time = now.format('YYYY-MM-DD')
#
#     get_metrics = attrgetter('on_day', 'dau', 'wau', 'mau', 'one_day_retention', 'seven_day_retention',
#                              'thirty_day_retention', 'paid_user_count', 'paid_count', 'revenue',
#                              'new_reg', 'email_reg', 'email_validated', 'new_reg_game_dau')
#
#     metrics = get_metrics(BIStatistic)
#     query = db.session.query(BIStatistic).with_entities(*metrics)
#
#     query_result = query.filter(and_(BIStatistic.on_day >= start_time,
#                                      BIStatistic.on_day <= end_time,
#                                      BIStatistic.game == 'All Game',
#                                      BIStatistic.platform == 'All Platform'))
#
#     transpose_query_result = list(map(list, zip(*query_result)))
#
#     dau = transpose_query_result[1]
#     wau = transpose_query_result[2]
#     mau = transpose_query_result[3]
#
#     one_day_retention_count = transpose_query_result[4]
#     seven_day_retention_count = transpose_query_result[5]
#     thirty_day_retention_count = transpose_query_result[6]
#
#     paid_user_count = transpose_query_result[7]
#     revenue = transpose_query_result[9]
#
#     new_reg = transpose_query_result[10]
#     email_reg = transpose_query_result[11]
#     new_reg_game_dau = transpose_query_result[13]
#
#     facebook_reg = array(new_reg) - array(email_reg)
#
#     one_day_retention = ['{:.2f}'.format(i) for i in array(one_day_retention_count) / array(new_reg_game_dau)]
#     seven_day_retention = ['{:.2f}'.format(i) for i in array(seven_day_retention_count) / array(new_reg_game_dau)]
#     thirty_day_retention = ['{:.2f}'.format(i) for i in array(thirty_day_retention_count) / array(new_reg_game_dau)]
#
#     stickiness_weekly = array(dau) / array(wau)
#     stickiness_monthly = array(dau) / array(mau)
#     ARPDAU = ['{:.2f}'.format(i) for i in array(revenue) / array(dau)]
#     ARPPU = ['{:.2f}'.format(i) for i in array(revenue) / array(paid_user_count)]
#
#     extra_metrics = [stickiness_weekly, stickiness_monthly, ARPDAU, ARPPU]
#
#     # process charts
#
#     column_names = [column["name"] for column in query.column_descriptions]
#     column_names.insert(13, 'facebook_reg')
#     column_names.extend(['stickiness_weekly', 'stickiness_monthly', 'ARPDAU', 'ARPPU'])
#     charts_legend = column_names[1:]
#
#     charts_data = transpose_query_result
#     charts_labels = [datetime.strftime(day, "%Y-%m-%d") for day in charts_data[0]]
#     charts_data[4:7] = [one_day_retention, seven_day_retention, thirty_day_retention]
#     charts_data = charts_data[1:]
#     charts_data.insert(13, facebook_reg)
#     charts_data.extend(extra_metrics)
#
#     charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)
#
#     # process tables
#
#     tables_title = [{'title': column_name} for column_name in column_names]
#
#     tables_data = [row for row in query_result]
#     transpose_tables_data = list(map(list, zip(*tables_data)))
#     transpose_tables_data[0] = charts_labels
#     transpose_tables_data[4:7] = [one_day_retention, seven_day_retention_count, thirty_day_retention_count]
#     transpose_tables_data.insert(13, facebook_reg)
#     transpose_tables_data.extend(extra_metrics)
#
#     tables_data = list(map(list, zip(*transpose_tables_data)))
#
#     tables_result = dict(tables_title=tables_title, tables_data=tables_data)
#
#     return jsonify(charts_result=charts_result, tables_result=tables_result)
