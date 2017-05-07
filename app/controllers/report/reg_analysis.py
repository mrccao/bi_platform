import datetime

import arrow
from flask import current_app as app
from flask import render_template, jsonify, request
from flask_login import login_required
from sqlalchemy import text, func, and_

from app.extensions import db
from app.models.bi import BIStatistic
from app.utils import current_time, get_day_range_of_month
from . import report


@report.route("/reg/reg_platform", methods=["GET"])
# @login_required
def reg_platform():
    return render_template('report/new_users.html')


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
@login_required
def user_reg_data():
    now = current_time(app.config['APP_TIMEZONE'])
    last_month_start_day, last_month_end_day = get_day_range_of_month(now)
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
    current_month_each_reg_method_count_proxy = process_user_reg(last_month_start_day, last_month_end_day)

    last_month_data, last_month_day_range, last_each_platform_reg_count_proxy, \
    last_each_reg_method_count_proxy = process_user_reg(current_month_start_day, current_month_end_day)

    return jsonify(current_month_data=current_month_data, current_month_day_range=current_month_day_range,
                   current_month_each_platform_reg_count_proxy=current_month_each_platform_reg_count_proxy,
                   current_month_each_reg_method_count_proxy=current_month_each_reg_method_count_proxy,
                   last_month_data=last_month_data, last_month_day_range=last_month_day_range,
                   last_each_platform_reg_count_proxy=last_each_platform_reg_count_proxy,
                   last_each_reg_method_count_proxy=last_each_reg_method_count_proxy)


@report.route("/report/reg_conversion_rate")
# @login_required
def reg_conversion_rate():
    group_type = request.args.get("group")
    now = current_time(app.config['APP_TIMEZONE'])
    start_time = now.replace(days=-int(15)).format('YYYY-MM-DD')
    end_time = now.replace(days=-1).format('YYYY-MM-DD')

    every_platform_query_result_proxy = []
    if group_type == 'Weekly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Facebook Game']:
            query_result = db.engine.execute(text("""
                                                SELECT DATE_FORMAT(on_day, '%Y-%u') AS week,
                                                       ROUND(AVG(new_reg_game_dau / new_reg), 2)
                                                FROM   bi_statistic
                                                WHERE  game = 'All Game'
                                                       AND on_day BETWEEN :start_time AND :end_time
                                                       AND platform= :platform
                                                GROUP  BY week """), platform=platform, start_time=start_time,
                                             end_time=end_time)

            every_platform_query_result_proxy.append(query_result)


    elif group_type == 'Monthly':
        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Facebook Game']:
            query_result = db.engine.execute(text("""
                                                SELECT DATE_FORMAT(on_day, '%Y-%m') AS month,
                                                       ROUND(AVG(new_reg_game_dau / new_reg), 2)
                                                FROM   bi_statistic
                                                WHERE  game = 'All Game'
                                                       AND on_day BETWEEN :start_time AND :end_time
                                                       AND platform= :platform
                                                GROUP  BY month"""), platform=platform, start_time=start_time,
                                             end_time=end_time)

            every_platform_query_result_proxy.append(query_result)

    else:

        for platform in ['All Platform', 'iOS', 'Android', 'Web', 'Facebook Game']:
            query_result = db.engine.execute(text("""
                                                SELECT DATE_FORMAT(on_day, '%Y-%m-%d'),
                                                       ROUND(new_reg_game_dau / new_reg, 2)
                                                FROM   bi_statistic
                                                WHERE  game = 'All Game'
                                                       AND on_day BETWEEN :start_time AND :end_time 
                                                       AND platform= :platform
         """), platform=platform, start_time=start_time, end_time=end_time)

            every_platform_query_result_proxy.append(query_result)

    query_result = list(
        map(lambda query_result: list(map(list, zip(*query_result))), every_platform_query_result_proxy))

    return jsonify(user_reg_conversion=query_result)


# @report.route("/reg/reg_conversion_rate")
# @login_required
# def get_reg_conversion():
#     group_type = request.args.get("group")
#     now = current_time(app.config['APP_TIMEZONE'])
#     start_time = now.replace(days=-int(60)).format('YYYY-MM-DD')
#     end_time = now.replace(days=-1).format('YYYY-MM-DD')
#
#     every_platform_query_result_proxy = []
#     if group_type == 'Weekly':
#
#         start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
#         start_time = start_time.format('YYYY-MM-DD')
#
#         query_result = db.engine.execute(text(""" SELECT date_format(on_day,'%Y-%w')  AS week,platform, ROUND(AVG(new_reg,0) ) ,
#  ROUND(AVG(new_reg_game_dau),0)   FROM bi_statistic WHERE on_day=''     GROUP  BY platform ,week
#           """), start_time=start_time, end_time=end_time)
#
#     elif group_type == 'Monthly':
#
#         start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
#         start_time = start_time.format('YYYY-MM-DD')
#
#         query_result = db.engine.execute(text(""" SELECT date_format(on_day,'%Y-%m')  AS month,platform,ROUND(AVG(new_reg,0)),
#   ROUND(AVG(new_reg_game_dau),0)   FROM bi_statistic GROUP BY platform ,month;
#           """), start_time=start_time, end_time=end_time)
#
#     else:
#         query_result = db.engine.execute(text(""" SELECT on_day,platform,new_reg,new_reg_game_dau   FROM bi_statistic GROUP BY platform ,on_day;
#           """), start_time=start_time, end_time=end_time)
#
#     for row in query_result:
#         row_as_dict = dict(row)
#         every_platform_query_result_proxy.append(row_as_dict)


@report.route("/report/new_user_distribution")
# @login_required
def new_user_distribution():
    now = current_time(app.config['APP_TIMEZONE'])
    last_month_start_day, last_month_end_day, current_month_start_day, current_month_end_day = get_day_range_of_month(
        now)
    group_type = request.args.get("group")
    start_time = now.replace(days=-int(60)).format('YYYY-MM-DD')
    end_time = now.replace(days=-1).format('YYYY-MM-DD')

    every_platform_query_result_proxy = []
    if group_type == 'Weekly':

        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        query_result = db.engine.execute(text(""" 
                                            SELECT DATE_FORMAT(on_day, '%Y-%w') AS week,
                                                   platform,
                                                   ROUND(AVG(new_reg_game_dau), 0) AS new_user
                                            FROM   bi_statistic
                                            WHERE  on_day BETWEEN :start_time AND :end_time 
                                            GROUP  BY platform,
                                                      week; 
          """), start_time=start_time, end_time=end_time)

    elif group_type == 'Monthly':

        start_time = arrow.Arrow.strptime(start_time, '%Y-%m-%d') - datetime.timedelta(120)
        start_time = start_time.format('YYYY-MM-DD')

        query_result = db.engine.execute(text(""" 
                                            SELECT DATE_FORMAT(on_day, '%Y-%m') AS month,
                                                   platform,
                                                   ROUND(AVG(new_reg_game_dau), 0) AS new_user
                                            FROM   bi_statistic
                                            WHERE  on_day BETWEEN :start_time AND :end_time 
                                            GROUP  BY platform,
                                                      month; 
          """), start_time=start_time, end_time=end_time)

    else:
        query_result = db.engine.execute(text(""" 
                                            SELECT on_day,
                                                   platform,
                                                   ROUND(AVG(new_reg_game_dau), 0) AS new_user
                                            FROM   bi_statistic
                                            WHERE  on_day = :end_time 
                                            GROUP  BY platform,
                                                      on_day; 
          """), start_time=start_time, end_time=end_time)

    for row in query_result:
        row_as_dict = dict(row)
        every_platform_query_result_proxy.append(row_as_dict)
