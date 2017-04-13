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

    charts_indications = (func.date_format(BIStatistic.on_day, '%Y-%m-%d'),
                          BIStatistic.dau,
                          BIStatistic.wau, BIStatistic.mau,
                          BIStatistic.new_reg,
                          BIStatistic.new_reg_game_dau,
                          BIStatistic.email_reg,
                          BIStatistic.email_validate,
                          BIStatistic.revenue,
                          BIStatistic.dollar_paid_amount,
                          BIStatistic.dollar_paid_count,
                          BIStatistic.dollar_paid_user_count,
                          BIStatistic.free_gold,
                          BIStatistic.free_silver,
                          BIStatistic.one_day_retention,
                          BIStatistic.seven_day_retention,
                          BIStatistic.thirty_day_retention)

    charts_query = db.session.query(BIStatistic).with_entities(*charts_indications)

    charts_query_result = charts_query.filter(and_(BIStatistic.on_day > start_time,
                                                   BIStatistic.on_day < end_time,
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

    tables_indications = (BIStatistic.dau,
                          BIStatistic.wau,
                          BIStatistic.mau,
                          BIStatistic.new_reg,
                          BIStatistic.new_reg_game_dau,
                          BIStatistic.email_reg,
                          BIStatistic.email_validate,
                          BIStatistic.new_reg,
                          BIStatistic.revenue,
                          BIStatistic.dollar_paid_amount,
                          BIStatistic.dollar_paid_count,
                          BIStatistic.dollar_paid_user_count,
                          BIStatistic.free_gold,
                          BIStatistic.free_silver,
                          BIStatistic.one_day_retention,
                          BIStatistic.seven_day_retention,
                          BIStatistic.thirty_day_retention)

    tables_query = db.session.query(BIStatistic).with_entities(*tables_indications)

    column_names = [column["name"] for column in tables_query.column_descriptions]
    tables_title = [{'title': column_name} for column_name in column_names]
    tables_title.insert(0, {'title': 'day'})

    tables_query_result = tables_query.filter(and_(BIStatistic.on_day > start_time,
                                                   BIStatistic.on_day < end_time,
                                                   BIStatistic.game == 'All Game',
                                                   BIStatistic.platform == 'All Platform'))

    transpose_tables_data_query_result = list(map(list, zip(*tables_query_result)))
    transpose_tables_data_query_result.insert(0, charts_labels)
    tables_data = list(map(list, zip(*transpose_charts_query_result)))

    tables_result = dict(tables_title=tables_title, tables_data=tables_data)

    return jsonify(charts_result=charts_result, tables_result=tables_result)
