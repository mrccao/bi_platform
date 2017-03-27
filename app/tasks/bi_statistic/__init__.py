from app.tasks import celery
from .dau import process_bi_statistic_dau
from .mau import process_bi_statistic_mau
from .new_reg import process_bi_statistic_new_reg
from .new_reg_dau import process_bi_statistic_new_reg_dau
from .wau import process_bi_statistic_wau


@celery.task
def process_bi_statistic(target, dau=1, wau=1, mau=1, new_reg=1, new_reg_dau=1):
    if dau:
        try:
            process_bi_statistic_dau(target)
        except:
            print('-----DAU Failed-----')
            raise
        else:
            print('------DAU Done------')

    if wau:
        try:
            process_bi_statistic_wau(target)
        except:
            print('-----WAU Failed------')
            raise
        else:
            print('------WAU Done------')
    if mau:
        try:
            process_bi_statistic_mau(target)
        except:
            print('------MAU Failed-----')
            raise
        else:
            print('------MAU Done------')

    if new_reg:
        try:
            process_bi_statistic_new_reg(target)
        except:
            print('-----New_reg Failed------')
            raise
        else:
            print('------New_reg Done------')

    if new_reg_dau:
        try:
            process_bi_statistic_new_reg_dau(target)
        except:
            print('------New_reg_dau Failed-----')
            raise
        else:
            print('------New_reg_dau Done-----')
