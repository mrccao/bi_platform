from flask import current_app as app

from app.tasks import celery
from app.tasks.bi_clubwpt_user import process_bi_clubwpt_user
from app.tasks.bi_statistic import process_bi_statistic
from app.tasks.bi_user import process_bi_user
from app.tasks.bi_user_bill import process_bi_user_bill
from app.tasks.bi_user_currency import process_bi_user_currency
from app.tasks.promotion import process_promotion_facebook_notification


@celery.task
def process_bi():
    process_bi_user()
    process_bi_user_bill()

    if app.config['ENV'] == 'prod':
        process_bi_user_currency()
        process_bi_clubwpt_user()


@celery.task
def process_promotion_push():
    process_promotion_facebook_notification()
    # process_promotion_email()


@celery.task
def process_bi_statistic_for_yesterday():
    process_bi_statistic('yesterday')


@celery.task
def process_bi_statistic_for_today():
    process_bi_statistic('today')
