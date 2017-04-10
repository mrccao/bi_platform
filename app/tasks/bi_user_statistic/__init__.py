from app.tasks import celery
from .consumption_records import process_bi_user_statistic_consumption_records
from .gold_silver_convert_reords import process_bi_user_statistic_convert_records
from .recharge_records import process_bi_user_statistic_recharge_records


@celery.task
def process_bi_user_statistic(target, recharge=1, consumption=1, convert=1):
    if recharge:
        process_bi_user_statistic_recharge_records(target)
        print('******* ' + target.capitalize() + ' user recharge records Done *******')

    if consumption:
        process_bi_user_statistic_consumption_records(target)
        print('******* ' + target.capitalize() + ' user consumption records  Done *******')

    if convert:
        process_bi_user_statistic_convert_records(target)
        print('******* ' + target.capitalize() + 'user convert records Done *******')
