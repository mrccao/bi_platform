import os
import pandas as pd
from flask import current_app as app
from numpy import array
from sqlalchemy import text

from app.constants import DAILY_REPORT_RECIPIENTS
from app.extensions import db
from app.tasks import celery
from app.tasks.mail import send_mail
from app.utils import current_time, dedup


@celery.task
def daily_report_dau():
    now = current_time(app.config['APP_TIMEZONE'])
    today = now.format('YYYY-MM-DD')
    yesterday = now.replace(days=-1).format('MM/DD/YY')
    generated_at = now.format('MM-DD-YYYY HH:mm:ss')

    sql = """
         SELECT DATE_FORMAT(a.on_day, '%m/%d/%y')           AS on_day,
               a.new_reg,
               a.new_reg_game_dau,
               a.new_reg_game_dau / a.new_reg              AS reg_to_dau,
               a.dau,
               a.dau - a.new_reg_game_dau                  AS return_dau,
               ( a.dau - a.new_reg_game_dau ) / b.dau_prev AS return_dau_rate,
               a.paid_user_count,
               a.paid_user_count / a.dau                   AS paid_user_rate,
               a.revenue,
               a.revenue / a.paid_user_count               AS ARPPU,
               a.revenue / a.dau                           AS ARPDAU
        FROM   (SELECT on_day,
                       new_reg,
                       new_reg_game_dau,
                       dau,
                       paid_user_count,
                       revenue
                FROM   bi_statistic
                WHERE  on_day >= DATE_ADD(:today, INTERVAL -31 DAY)
                       AND on_day < :today
                       AND game = 'all game'
                       AND platform = 'all platform') a
               INNER JOIN (SELECT on_day,
                                  dau dau_prev
                           FROM   bi_statistic
                           WHERE  on_day >= DATE_ADD(:today, INTERVAL -32 DAY)
                                  AND on_day < DATE_ADD(:today, INTERVAL -1 DAY)
                                  AND game = 'all game'
                                  AND platform = 'all platform') b
                       ON a.on_day = DATE_ADD(b.on_day, INTERVAL 1 DAY)
        ORDER  BY a.on_day DESC  
         """

    result_proxy = db.engine.execute(text(sql), today=today)

    column_names = dedup([col[0] for col in result_proxy.cursor.description])
    last_thirty_days_data = result_proxy.fetchall()
    last_thirty_days_data_formatted = [
        [row['on_day'], row['new_reg'], row['new_reg_game_dau'], format(row['reg_to_dau'], '0.2%'),
         row['dau'], row['return_dau'],
         format(row['return_dau_rate'], '0.2%'), row['paid_user_count'],
         format(row['paid_user_rate'], '0.2%'), row['revenue'],
         format(row['ARPPU'], '0.2f'),
         format(row['ARPDAU'], '0.2f')] for row in db.engine.execute(text(sql), today=today)]

    report_data = last_thirty_days_data_formatted[1:7]

    yesterday_data = array(last_thirty_days_data[0])[1:]
    yesterday_data_formatted = last_thirty_days_data_formatted[0][1:]
    the_day_before_day_data = array(last_thirty_days_data[1])[1:]

    delta = yesterday_data - the_day_before_day_data
    delta = list(map(lambda data_delta: True if data_delta >= 0 else False, delta))
    delta_style = list(map(lambda
                               data: "<td style='border-right: 1px solid #C1DAD7; border-bottom: 1px solid #C1DAD7; background: #fff; font-size:15px;font-weight: 400; padding: 6px 6px 6px 12px; color: #0e0e0e;' >{}</td>" if data else "<td style='border-right: 1px solid #C1DAD7; border-bottom: 1px solid #C1DAD7; background: #fff; font-size:15px;font-weight: 400; padding: 6px 6px 6px 12px; color: #0e0e0e;'>{} </td>",
                           delta))

    yesterday_data_style = ''.join(delta_style).format(*(yesterday_data_formatted))
    yesterday_data_style = '<tr>' + "<td style='border-right: 1px solid #C1DAD7; border-bottom: 1px solid #C1DAD7; background: #fff; font-size:15px;font-weight: 400; padding: 6px 6px 6px 12px; color: #0e0e0e;'>{}</td>".format(
        yesterday) + yesterday_data_style + '<tr>'

    title = 'Daily Report – DAU Related'
    email_subject = now.format('MM-DD-YYYY') + '_DAU_REPORT'
    filename = 'DAU_REPORT.csv'
    path = os.path.join(app.config["REPORT_FILE_FOLDER"], filename)

    result = pd.DataFrame(pd.DataFrame(last_thirty_days_data, columns=column_names))

    with open(path, 'w+') as f:
        result.to_csv(f, sep=',', encoding='utf-8')

    send_mail(to=DAILY_REPORT_RECIPIENTS, subject=email_subject, template='cron_daily_report', attachment=path,
              attachment_content_type='text/csv', filename=filename, column_names=column_names,
              report_data=report_data, title=title, generated_at=generated_at,
              yesterday_data_style=yesterday_data_style, DAU=True)


@celery.task
def daily_report_game_table_statistic():
    now = current_time(app.config['APP_TIMEZONE'])
    yesterday = now.replace(days=-1).format('YYYY-MM-DD')
    generated_at = now.format('MM-DD-YYYY HH:mm:ss')

    sql = """
          SELECT
                 date_format(convert_tz(cgz.time_update, '+08:00', '-04:00'), '%m/%d/%y %H') AS on_hour,
                 CASE mi.blindname
                          WHEN '1-2WPT盲注' THEN 'Beginner1'
                          WHEN 'WPT-2-4' THEN 'Beginner2'
                          WHEN 'WPT-3-6' THEN 'Beginner3'
                          WHEN 'cgz_5/10' THEN 'Beginner4'
                          WHEN '万能豆_10\20_新' THEN 'Amateur1'
                          WHEN 'cgz_50/100' THEN 'Amateur2'
                          WHEN 'cgz_100/200' THEN 'Amateur3'
                          WHEN 'cgz_250/500' THEN 'Amateur4'
                          WHEN 'cgz_500/1000' THEN 'Pro1'
                          WHEN '万能豆_1000/2000' THEN 'Pro2'
                          WHEN '万能豆_2500/5000' THEN 'Pro3'
                          WHEN 'cgz_5000/10000' THEN 'Pro4'
                          WHEN '万能豆_10000/20000_大' THEN 'Elite1'
                          WHEN 'cgz_2w/4w' THEN 'Elite2'
                          WHEN '万能豆_50000/100000_紫' THEN 'Elite3'
                          WHEN 'cgz_10w/20w' THEN 'Elite4'
                          ELSE mi.blindname
                 END                                                                          AS table_name,
                 CASE mi.blindname
                          WHEN '1-2WPT盲注' THEN '1/2'
                          WHEN 'WPT-2-4' THEN '2/4'
                          WHEN 'WPT-3-6' THEN '3/6'
                          WHEN 'cgz_5/10' THEN '5/10'
                          WHEN '万能豆_10\20_新' THEN '10/20'
                          WHEN 'cgz_50/100' THEN '50/100'
                          WHEN 'cgz_100/200' THEN '100/200'
                          WHEN 'cgz_250/500' THEN '250/500'
                          WHEN 'cgz_500/1000' THEN '500/1000'
                          WHEN '万能豆_1000/2000' THEN '1000/2000'
                          WHEN '万能豆_2500/5000' THEN '2500/5000'
                          WHEN 'cgz_5000/10000' THEN '5000/10000'
                          WHEN '万能豆_10000/20000_大' THEN '10000/20000'
                          WHEN 'cgz_2w/4w' THEN '20000/40000'
                          WHEN '万能豆_50000/100000_紫' THEN '50000/100000'
                          WHEN 'cgz_10w/20w' THEN '100000/200000'
                          ELSE mi.blindname
                 END                                                                         AS stakes_level,
                 COUNT(DISTINCT cgz.username)                                                AS uniq_players,
                 COUNT(DISTINCT cgz.pan_id)                                                  AS uniq_hands_played,
                 COUNT(cgz.pan_id)                                                           AS total_hands_played
          FROM    tj_matchinfo mi
          JOIN    tj_cgz_flow_userpaninfo cgz
          ON      cgz.matchid=mi.matchid
          WHERE   DATE(convert_tz(cgz.time_update, '+08:00', '-04:00'))= :yesterday
          GROUP BY stakes_level,
                   on_hour; 
        """

    result_proxy = db.get_engine(db.get_app(), bind='orig_wpt_ods').execute(text(str(sql)), yesterday=yesterday)

    column_names_attached = dedup([col[0] for col in result_proxy.cursor.description])

    last_24_hours_data = result_proxy.fetchall()

    title = 'Daily Report – Game Table Statistic Related'
    email_subject = now.format('MM-DD-YYYY') + '_Game_Table_Statistic_REPORT'
    filename = 'STAKES_LEVEL_REPORT.csv'

    path = os.path.join(app.config["REPORT_FILE_FOLDER"], filename)
    result = pd.DataFrame(pd.DataFrame(last_24_hours_data, columns=column_names_attached))

    with open(path, 'w+', encoding='utf-8') as f:
        result.to_csv(f, sep=',', encoding='utf-8')

    sql = """
          SELECT
                 date_format(convert_tz(cgz.time_update, '+08:00', '-04:00'), '%m/%d/%y')    AS on_day,
                 CASE mi.blindname
                          WHEN '1-2WPT盲注' THEN 'Beginner1'
                          WHEN 'WPT-2-4' THEN 'Beginner2'
                          WHEN 'WPT-3-6' THEN 'Beginner3'
                          WHEN 'cgz_5/10' THEN 'Beginner4'
                          WHEN '万能豆_10\20_新' THEN 'Amateur1'
                          WHEN 'cgz_50/100' THEN 'Amateur2'
                          WHEN 'cgz_100/200' THEN 'Amateur3'
                          WHEN 'cgz_250/500' THEN 'Amateur4'
                          WHEN 'cgz_500/1000' THEN 'Pro1'
                          WHEN '万能豆_1000/2000' THEN 'Pro2'
                          WHEN '万能豆_2500/5000' THEN 'Pro3'
                          WHEN 'cgz_5000/10000' THEN 'Pro4'
                          WHEN '万能豆_10000/20000_大' THEN 'Elite1'
                          WHEN 'cgz_2w/4w' THEN 'Elite2'
                          WHEN '万能豆_50000/100000_紫' THEN 'Elite3'
                          WHEN 'cgz_10w/20w' THEN 'Elite4'
                          ELSE mi.blindname
                 END                                                                         AS table_name,
                 CASE mi.blindname
                          WHEN '1-2WPT盲注' THEN '1/2'
                          WHEN 'WPT-2-4' THEN '2/4'
                          WHEN 'WPT-3-6' THEN '3/6'
                          WHEN 'cgz_5/10' THEN '5/10'
                          WHEN '万能豆_10\20_新' THEN '10/20'
                          WHEN 'cgz_50/100' THEN '50/100'
                          WHEN 'cgz_100/200' THEN '100/200'
                          WHEN 'cgz_250/500' THEN '250/500'
                          WHEN 'cgz_500/1000' THEN '500/1000'
                          WHEN '万能豆_1000/2000' THEN '1000/2000'
                          WHEN '万能豆_2500/5000' THEN '2500/5000'
                          WHEN 'cgz_5000/10000' THEN '5000/10000'
                          WHEN '万能豆_10000/20000_大' THEN '10000/20000'
                          WHEN 'cgz_2w/4w' THEN '20000/40000'
                          WHEN '万能豆_50000/100000_紫' THEN '50000/100000'
                          WHEN 'cgz_10w/20w' THEN '100000/200000'
                          ELSE mi.blindname
                 END                                                                         AS stakes_level,
                 COUNT(DISTINCT cgz.username)                                                AS uniq_players,
                 COUNT(DISTINCT cgz.pan_id)                                                  AS uniq_hands_played,
                 COUNT(cgz.pan_id)                                                           AS total_hands_played
          FROM    tj_matchinfo mi
          JOIN    tj_cgz_flow_userpaninfo cgz
          ON      cgz.matchid=mi.matchid
          WHERE   DATE(convert_tz(cgz.time_update, '+08:00', '-04:00'))= :yesterday
          GROUP BY stakes_level,
                   on_day;
    """

    result_proxy = db.get_engine(db.get_app(), bind='orig_wpt_ods').execute(text(str(sql)), yesterday=yesterday)
    column_names = dedup([col[0] for col in result_proxy.cursor.description])
    yesterday_data = result_proxy.fetchall()

    send_mail(to=DAILY_REPORT_RECIPIENTS, subject=email_subject, template='cron_daily_report', attachment=path,
              attachment_content_type='text/csv', filename=filename, column_names=column_names,
              report_data=yesterday_data, title=title, generated_at=generated_at, game_table_statistic=True)
