from itertools import groupby

from flask import current_app as app, jsonify
from flask import render_template, request
from flask_login import login_required
from operator import itemgetter
from sqlalchemy import text

from app.extensions import db
from app.utils import current_time
from . import report


@report.route("/report/user_region", methods=["GET"])
# @login_required
def user_region():
    return render_template('report/user_region.html')


@report.route("/report/reg_user_state", methods=["GET"])
# @login_required
def get_reg_user_state_data():
    group = request.args.get('group')
    now = current_time(app.config['APP_TIMEZONE'])
    timezone_offset = app.config['APP_TIMEZONE']

    if group == 'Lifetime':

        query_result = db.engine.execute(text("""
                                                SELECT    reg_state  AS name,  count(user_id)                                     AS value
                                                FROM      bi_user
                                                WHERE     reg_country ='United States'
                                                GROUP BY reg_state
                                                ORDER BY value  LIMIT 10
                                                  """))

        query_result = list(query_result)
        transpose_query_result = list(map(list, zip(*query_result)))

        query_result_row_dict = []
        for row in query_result:
            row_dict = dict(row)
            query_result_row_dict.append(row_dict)

        location = transpose_query_result[0]
        reg_count = transpose_query_result[1]

        bar_result = {'location': location, 'reg_count': reg_count}
        map_result = query_result_row_dict

        return jsonify(map_result=map_result, bar_result=bar_result, type='lifetime')


    elif group == 'Monthly':

        start_time = now.replace(days=-int(180)).format('YYYY-MM-DD')
        end_time = now.replace(days=-1).format('YYYY-MM-DD')

        query_result = db.engine.execute(text("""
                                                SELECT    DATE_FORMAT(CONVERT_TZ(reg_time, '+00:00', :timezone_offset),'%Y-%m') AS time,
                                                          reg_state  AS name,  count(user_id)                                     AS value
                                                FROM      bi_user
                                                WHERE     DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) 
                                                          BETWEEN :start_time AND :end_time
                                                          AND     reg_country ='United States'
                                                GROUP  BY time,reg_state
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                         end_time=end_time)

    else:

        start_time = now.replace(days=-int(30)).format('YYYY-MM-DD')
        end_time = now.replace(days=-1).format('YYYY-MM-DD')

        query_result = db.engine.execute(text("""
                                                        SELECT    DATE_FORMAT(CONVERT_TZ(reg_time, '+00:00', :timezone_offset),'%Y-%m-%d') AS time,
                                                               reg_state                                                AS name,
                                                               Count(*)                                               AS value
                                                        FROM   bi_user
                                                        WHERE  DATE(CONVERT_TZ(reg_time, '+00:00', :timezone_offset)) BETWEEN
                                                               :start_time AND :end_time
                                                               AND     reg_country ='United States'
                                                        GROUP  BY time,
                                                                  reg_state
                                                  """), timezone_offset=timezone_offset, start_time=start_time,
                                         end_time=end_time)

    query_result = list(query_result)
    transpose_query_result = list(map(list, zip(*query_result)))
    time_range = sorted(list(set(transpose_query_result[0])))

    # bar

    bar_result = []
    query_result_row_dict = []

    for row in query_result:
        row_dict = dict(row)
        query_result_row_dict.append(row_dict)

    query_result_row_dict.sort(key=itemgetter('time'))
    reg_user_state_group_by_time = groupby(query_result_row_dict, itemgetter('time'))

    for time, group in reg_user_state_group_by_time:

        group = list(group)
        group.sort(key=itemgetter('value'))
        ten_top_reg_countries = group[:11]
        location = []
        reg_count = []

        for row in ten_top_reg_countries:
            location.append(row['name'])
            reg_count.append(row['value'])

        bar_result.append({time: {'location': location, 'reg_count': reg_count}})

    # map

    map_result = []
    query_result_row_dict.sort(key=itemgetter('time'))
    reg_user_state_group_by_time = groupby(query_result_row_dict, itemgetter('time'))

    for time, group in reg_user_state_group_by_time:
        group = list(group)
        for row in group:
            del row['time']
        map_result.append({time: group})

    return jsonify(time_range=time_range, bar_result=bar_result, map_result=map_result)
