from flask import Blueprint, render_template, jsonify
from flask import current_app as app
from flask_login import login_required
from operator import attrgetter
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

    start_time = now.replace(days=-100).format('YYYY-MM-DD')
    end_time = now.format('YYYY-MM-DD')

    get_metrics = attrgetter('dau', 'wau', 'mau', 'new_reg', 'email_reg', 'email_validate', 'new_reg_game_dau',
                             'dollar_paid_user_count', 'dollar_paid_count', 'revenue', 'free_silver', 'free_gold',
                             'one_day_retention',
                             'seven_day_retention', 'thirty_day_retention')
    metrics = get_metrics(BIStatistic)

    charts_metrics = (func.date_format(BIStatistic.on_day, '%Y-%m-%d'), *metrics)
    charts_query = db.session.query(BIStatistic).with_entities(*charts_metrics)
    charts_query_result = charts_query.filter(and_(BIStatistic.on_day >= start_time,
                                                   BIStatistic.on_day <= end_time,
                                                   BIStatistic.game == 'All Game',
                                                   BIStatistic.platform == 'All Platform'))

    tables_query = db.session.query(BIStatistic).with_entities(*metrics)
    tables_query_result = tables_query.filter(and_(BIStatistic.on_day >= start_time,
                                                   BIStatistic.on_day <= end_time,
                                                   BIStatistic.game == 'All Game',
                                                   BIStatistic.platform == 'All Platform'))

    # process the charts and tables

    transpose_charts_query_result = list(map(list, zip(*charts_query_result)))
    charts_data = transpose_charts_query_result[1:]

    transpose_tables_data_query_result = list(map(list, zip(*tables_query_result)))
    tables_data = list(map(list, zip(*transpose_charts_query_result)))

    # add extra_calculate_metrics

    # new_reg_game_dau = transpose_charts_query_result[7]
    # new_reg = transpose_charts_query_result[4]
    # revenue = transpose_charts_query_result[10]
    # DAU = transpose_charts_query_result[1]
    # paid_user_count = transpose_charts_query_result[9]
    # users_numbers = db.session.query(BIUser.reg_time,func.count(BIUser.user_id)).group_by(cast(BIUser.reg_time,Date)).having(
    #     and_(BIUser.reg_time >= start_time, BIUser.reg_time <= end_time)).all()
    #
    # reg_retention = array(new_reg_game_dau) / array(new_reg)
    # ARPDAU = array(revenue) / array(DAU)
    # ARPPU = array(revenue) / array(paid_user_count)
    # ARPU = array(revenue) / array(users_numbers)
    #
    # extra_column_names = ['reg_retention', 'ARPDAU', 'ARPPU', 'ARPU']
    # extra_operational_metrics = [reg_retention, ARPDAU, ARPPU, ARPU]

    # process the labels and columns_name of charts and tables

    charts_legend = [column["name"] for column in charts_query.column_descriptions][1:]
    charts_legend.extend(extra_column_names)
    charts_labels = transpose_charts_query_result[0]
    charts_data.extend(*extra_operational_metrics)

    tables_column_names = [column["name"] for column in tables_query.column_descriptions]
    tables_column_names.extend(extra_column_names)
    tables_title = [{'title': column_name} for column_name in tables_column_names]
    tables_title.insert(0, {'title': 'day'})

    transpose_tables_data_query_result.insert(0, charts_labels)
    tables_data.extend((list(map(list, zip(*extra_operational_metrics)))))

    charts_result = dict(charts_labels=charts_labels, charts_data=charts_data, charts_legend=charts_legend)
    tables_result = dict(tables_title=tables_title, tables_data=tables_data)

    return jsonify(charts_result=charts_result, tables_result=tables_result)


select
t.on_day, t.new_reg, (select
sum(x.new_reg)
where
x.on_day < t.on_day and x.game = 'All Game' and x.platform = 'All Platform'  ) as cumulative_sum
where
t.game = 'All Game' and t.platform = 'All Platform' and t.on_day > '2017-02-01' and on_day < '2017-04-23';

select
t.on_day, t.revenue, (select
sum(x.revenue)
where
x.on_day < t.on_day and x.game = 'All Game' and x.platform = 'All Platform'  ) as cumulative_sum
where
t.game = 'All Game' and t.platform = 'All Platform' and t.on_day > '2017-02-01' and on_day < '2017-04-23';
