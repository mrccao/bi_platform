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
        process_bi_statistic_dau(target, timezone_offset)
    if wau:
        process_bi_statistic_wau(target, timezone_offset)
    if mau:
        process_bi_statistic_mau(target, timezone_offset)
    if new_reg:
        process_bi_statistic_new_reg(target, timezone_offset)
    if new_reg_dau:
        process_bi_statistic_new_reg_dau(target, timezone_offset)
    if revenue:
        process_bi_statistic_revenue(target, timezone_offset)
