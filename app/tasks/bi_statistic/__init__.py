from app.utils import get_db_timezone_offset
from .dau import process_bi_statistic_dau
from .mau import process_bi_statistic_mau
from .new_reg import process_bi_statistic_new_reg
from .new_reg_dau import process_bi_statistic_new_reg_dau
from .revenue import process_bi_statistic_revenue
from .wau import process_bi_statistic_wau


def process_bi_statistic(target, dau=1, wau=1, mau=1, new_reg=1, new_reg_dau=1, revenue=1):
    timezone_offset = get_db_timezone_offset()

    if dau:
        try:
            process_bi_statistic_dau(target, timezone_offset)
        except:
            print('DAU Failed')
        else:
            print('DAU Done')

    if wau:
        try:
            process_bi_statistic_wau(target, timezone_offset)
        except:
            print('WAU Failed')
        else:
            print('WAU Done')
    if mau:
        try:
            process_bi_statistic_mau(target, timezone_offset)
        except:
            print('MAU Failed')
        else:
            print('MAU Done')

    if new_reg:
        try:
            process_bi_statistic_new_reg(target, timezone_offset)
        except:
            print('New_reg Failed')
        else:
            print('New_reg Done')

    if new_reg_dau:
        try:
            process_bi_statistic_new_reg_dau(target, timezone_offset)
        except:
            print('New_reg_dau Failed')
        else:
            print('New_reg_dau Done')

    if revenue:
        try:
            process_bi_statistic_revenue(target, timezone_offset)
        except:
            print('Revenue Failed')
        else:
            print('Revenue Done')
