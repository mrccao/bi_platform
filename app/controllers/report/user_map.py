from itertools import groupby
from operator import itemgetter

from flask import current_app as app, jsonify
from flask import render_template, request
from flask_login import login_required
from sqlalchemy import text

from app.extensions import db
from app.utils import current_time
from . import report


@report.route("/report/user_map", methods=["GET"])
@login_required
def user_map():
    return render_template('report/user_map.html')


@report.route("/report/world_user", methods=["GET"])
@login_required
def get_world_user_data():
    group = request.args.get('Group', 'Daily')
    now = current_time(app.config['APP_TIMEZONE'])
    timezone_offset = app.config['APP_TIMEZONE']
    start_time = now.replace(days=-int(30)).format('YYYY-MM-DD')
    end_time = now.replace(days=-1).format('YYYY-MM-DD')

    if group == 'Daily':

        reg_user_country_data = db.engine.execute(text("""
                                                        SELECT DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS time,
                                                               country                                                AS name,
                                                               Count(*)                                               AS value
                                                        FROM   bi_user
                                                        WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) BETWEEN
                                                               :start_time AND :end_time
                                                        GROUP  BY time,
                                                                  country 
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                                  end_time=end_time)

    else:

        start_time = now.replace(days=-int(180)).format('YYYY-MM-DD')

        reg_user_country_data = db.engine.execute(text("""
                                                SELECT    DATE_FORMAT(CONVERT_TZ(reg_time, '+00:00', :timezone_offset),'%Y-%m') AS time,
                                                          country  AS name,  count(user_id)                                     AS value
                                                FROM      bi_user
                                                WHERE     DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) 
                                                            BETWEEN :start_time AND :end_time
                                                GROUP  BY time,country
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                                  end_time=end_time)

    transpose_query_result = list(map(list, zip(*reg_user_country_data)))
    date_range = transpose_query_result[0]

    # bar

    user_location_data = []
    for row in reg_user_country_data:
        row_dict = dict(row)
        user_location_data.append(row_dict)

    bar_result = []
    country = []
    value = []
    user_location_data.sort(key=itemgetter('time'))
    reg_user_data_group_by_time = groupby(user_location_data, itemgetter('time'))

    for time, group in reg_user_data_group_by_time:
        group.sort(key=itemgetter('value'))
        ten_top_reg_countries = group[:11]
        for i in ten_top_reg_countries:
            country.append(i['country'])
            value.append(i['value'])
            bar_result.append({time: {'country': country, 'value': value}})

    # map

    user_location_data = []
    for row in reg_user_country_data:
        row_dict = dict(row)
        del row_dict['time']
        user_location_data.append(row_dict)

    return jsonify(date_range=date_range, bar_result=bar_result, user_location_data=user_location_data)


@report.route("/report/America_user", methods=["GET"])
@login_required
def get_America_user_data():
    group = request.args.get('Group', 'Daily')
    now = current_time(app.config['APP_TIMEZONE'])
    timezone_offset = app.config['APP_TIMEZONE']
    start_time = now.replace(days=-int(30)).format('YYYY-MM-DD')
    end_time = now.replace(days=-1).format('YYYY-MM-DD')

    if group == 'Daily':

        America_data = db.engine.execute(text("""
                                                  SELECT    DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) AS on_day,
                                                            state  AS name,  count(user_id)                        AS value
                                                  FROM      bi_user
                                                  WHERE     DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) 
                                                            BETWEEN :start_time AND :end_time
                                                            AND country='America'
                                                  GROUP BY  on_day, state
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                         end_time=end_time)

    else:

        start_time = now.replace(days=-int(180)).format('YYYY-MM-DD')
        America_data = db.engine.execute(text("""
                                                SELECT    DATE_FORMAT(CONVERT_TZ(reg_time, '+00:00', :timezone_offset),'%Y-%m') AS month,
                                                          state  AS name,  count(user_id)                                       AS value
                                                FROM      bi_user
                                                WHERE     DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) 
                                                            BETWEEN :start_time AND :end_time
                                                            AND country='America'
                                                GROUP BY  month, state
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                         end_time=end_time)
