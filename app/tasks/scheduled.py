from flask import current_app as app

from app.tasks import celery
from app.tasks.bi_clubwpt_user import process_bi_clubwpt_user
from app.tasks.bi_statistic import process_bi_statistic
from app.tasks.bi_user import process_bi_user
from app.tasks.bi_user_bill import process_bi_user_bill
from app.tasks.bi_user_bill_detail import process_bi_user_bill_detail
from app.tasks.bi_user_currency import process_bi_user_currency
from app.tasks.cron_daily_report import daily_report_dau, daily_report_game_table_statistic
from app.tasks.promotion import process_promotion_facebook_notification, process_promotion_email
from app.tasks.sendgrid import get_campaigns
from app.tasks.sync_wpt_bi import process_wpt_bi_user_statistic


@celery.task
def process_bi():
    process_bi_user()
    process_bi_user_bill()
    process_bi_user_bill_detail()

    if app.config['ENV'] == 'prod':
        process_bi_clubwpt_user()


@celery.task
def process_bi_currency():
    if app.config['ENV'] == 'prod':
        process_bi_user_currency()


@celery.task
def process_wpt_bi():
    process_wpt_bi_user_statistic()


@celery.task
def process_promotion_push():
    process_promotion_facebook_notification()
    process_promotion_email()


@celery.task
def process_bi_statistic_for_yesterday():
    process_bi_statistic('yesterday')


@celery.task
def process_bi_statistic_for_today():
    process_bi_statistic('today')


@celery.task
def daily_report():
    daily_report_dau()
    daily_report_game_table_statistic()


@celery.task
def get_campaign_cached():
    get_campaigns()
