from app.tasks import celery
from .recharge_records import process_bi_user_statistic_recharge_records
from .consumption_records import process_bi_user_statistic_consumption_records
from .gold_silver_convert_reords import process_bi_user_statistic_convert_records



@celery.task
def process_bi_user_statistic(target, recharge=1, consumption=1, convert=1):
    if recharge:

        try:
            process_bi_user_statistic_recharge_records(target)
        except:
            print('-----recharge_records Failed-----')
            raise
        else:
            print('-----recharge_records Done-----')

    if consumption:

        try:
            process_bi_user_statistic_consumption_records(target)
        except:
            print('-----consumption_records Failed-----')
            raise
        else:
            print('-----consumption_records Done-----')


    if convert:

        try:
            process_bi_user_statistic_convert_records(target)
        except:
            print('-----convert_records Failed-----')
            raise
        else:
            print('-----convert_records Done-----')
