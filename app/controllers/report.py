from datetime import datetime

from flask import Blueprint, render_template, jsonify, request
from flask import current_app as app
from flask_login import login_required
from numpy import array, isnan
from sqlalchemy import and_, func, text
from sqlalchemy import select
from sqlalchemy.sql import table

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
    start_time = request.args.get('start_time', start_time)
    end_time = request.args.get('end_time', end_time)
    week = request.args.get("week")
    months = request.args.get("months")

    game = request.args.get("game", "All Game")
    platform = request.args.get("platform", "All Platform")

    metrics = [
        text('sum(wau)/:days'),
        text('sum(mau)/:days'),
        text('sum(new_reg)/:days'),
        text('sum(facebook_game_reg)/:days'),
        text('sum(facebook_login_reg)/:days'),
        text('sum(guest_reg)/:days'),
        text('sum(new_reg_game_dau)/:days'),
        text('sum(paid_user_count)/:days'),
        text('sum(paid_count)/:days'),
        text('sum(revenue)/:days'),
        text('sum(one_day_retention)/:days'),
        text('sum(seven_day_retention)/:days'),
        text('sum(thirty_day_retention)/:days')]

    # if week:
    target = text("date_format(on_day, '%Y-%u') as week")
    # target = literal_column(on_day, )
    metrics.insert(0, target)

    s = select(metrics).where(
        and_(text("game = :game"),
             text("platform = :platform "), )). \
        select_from(table("bi_statistic")). \
        group_by(text("week")). \
        having(and_(
        text("week>=:start_time"),
        text("week<=:end_time")))

    result = db.engine.execute(s, start_time=start_time, end_time=end_time, game=game, days=7,
                               platform=platform).fetchall()

    if months:
        target = text("date_format(on_day,'%Y-%m')").label("months")
        metrics.insert(0, target)

        s = select(metrics).where(
            and_(text("game = :game"),
                 text("platform = :platform "), )). \
            select_from(table("bi_statistic")). \
            group_by(text("months")). \
            having(and_(
            text("months>=:start_time"),
            text("months<=:end_time")))

        result = db.engine.execute(s, start_time=start_time, end_time=end_time, game=game, days=7,
                                   platform=platform).fetchall()

    #
    #     dau = transpose_query_result[1]
    #     wau = transpose_query_result[2]
    #     mau = transpose_query_result[3]
    #     new_reg_game_dau = transpose_query_result[8]
    #     paid_user_count = transpose_query_result[9]
    #     revenue = transpose_query_result[11]
    #     one_day_retention_count = transpose_query_result[12]
    #     seven_day_retention_count = transpose_query_result[13]
    #     thirty_day_retention_count = transpose_query_result[14]

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

    try:
        ARPDAU = list(map(lambda i: 0 if isnan(i) else int(round(i, 2)), array(revenue) / array(dau)))
        ARPPU = list(map(lambda i: 0 if isnan(i) else int(round(i, 2)), array(revenue) / array(paid_user_count)))

    except:
        ARPDAU = [0 for i in range(60)]
        ARPPU = [0 for i in range(60)]

    compound_metrics = [stickiness_weekly, stickiness_monthly, ARPDAU, ARPPU]

    # process charts

    column_names = [column["name"] for column in query.column_descriptions]
    column_names[12:15] = ['one_day_retention(%)', 'seven_day_retention(%)', 'thirty_day_retention(%)']
    column_names.extend(['stickiness_weekly(%)', 'stickiness_monthly(%)', 'ARPDAU', 'ARPPU'])

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

    reg_records = db.session.query(func.DATE_FORMAT(BIStatistic.on_day, '%Y-%m-%d'),
                                   BIStatistic.email_reg, BIStatistic.guest_reg,
                                   BIStatistic.facebook_game_reg, BIStatistic.facebook_login_reg).filter(
        and_(BIStatistic.on_day >= start_time, BIStatistic.on_day < end_time, BIStatistic.game == 'All Game'))

    transpose_query_result = list(map(list, zip(*reg_records)))

    platform = ["iOS", "Android", "Web", "Web Mobile", "Facebook Game", "All Platform"]
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
