import os
import pandas as pd
from flask import current_app as app
from sqlalchemy import text

from app.constants import DAILY_DAU_REPORT_RECIPIENTS
from app.extensions import db
from app.tasks import celery
from app.tasks.mail import send_mail
from app.utils import current_time, dedup


@celery.task
def daily_report_dau():
    now = current_time(app.config['APP_TIMEZONE'])
    today = now.format('YYYY-MM-DD')
    yesterday = now.replace(days=-1).format('YYYY-MM-DD')

    generated_at = now.format('YYYY-MM-DD HH:mm:ss')

    sql = """
         SELECT DATE_FORMAT(a.on_day, '%Y-%m-%d')           AS on_day,
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

    from numpy import array

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
    email_subject = today + '_DAU_REPORT'
    filename = today + '_DAU_REPORT.csv'
    path = os.path.join('/home/', filename)

    result = pd.DataFrame(pd.DataFrame(last_thirty_days_data, columns=column_names))

    with open(path, 'w+') as f:
        result.to_csv(f, sep='\t', encoding='utf-8')

    send_mail(to=DAILY_DAU_REPORT_RECIPIENTS, subject=email_subject, template='cron_daily_report', attachment=path,
              attachment_content_type='text/csv', filename=filename, column_names=column_names,
              report_data=report_data, title=title, generated_at=generated_at,
              yesterday_data_style=yesterday_data_style)
