from app.tasks import celery

from .dau import process_bi_statistic_dau
from .mau import process_bi_statistic_mau
from .new_reg import process_bi_statistic_new_reg
from .new_reg_dau import process_bi_statistic_new_reg_dau
from .wau import process_bi_statistic_wau


@celery.task
def process_bi_statistic(target, dau=1, wau=1, mau=1, new_reg=1, new_reg_dau=1):
    if dau:
        process_bi_statistic_dau(target)
        print('******* DAU Done *******')

    if wau:
        process_bi_statistic_wau(target)
        print('******* WAU Done *******')

    if mau:
        process_bi_statistic_mau(target)
        print('******* MAU Done *******')

    if new_reg:
        process_bi_statistic_new_reg(target)
        print('******* New_reg Done *******')

    if new_reg_dau:
        process_bi_statistic_new_reg_dau(target)
        print('******* New_reg_dau Done ******')
