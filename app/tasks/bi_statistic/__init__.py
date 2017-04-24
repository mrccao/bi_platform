from app.tasks import celery
from .dau import process_bi_statistic_dau
from .free_gold_silver import process_bi_statistic_free_transaction
from .gold_silver_consumption import process_bi_statistic_gold_silver_consumption
from .mau import process_bi_statistic_mau
from .new_reg import process_bi_statistic_new_reg
from .new_reg_dau import process_bi_statistic_new_reg_dau
from .payment_records import process_bi_statistic_payment_records
from .retention import process_bi_statistic_retention
from .revenue import process_bi_statistic_revenue
from .wau import process_bi_statistic_wau
from .game_records import  process_bi_statistic_game_records


@celery.task
def process_bi_statistic(target, dau=1, wau=1, mau=1, new_reg=1, new_reg_dau=1, gold_silver_consumption=0,
                         free_gold_silver=1, payment_records=1,
                         retention=1, revenue=1, game_records=1):
    if dau:
        process_bi_statistic_dau(target)
        print('******* ' + target.capitalize() + ' DAU Done *******')

    if new_reg_dau:
        process_bi_statistic_new_reg_dau(target)
        print('******* ' + target.capitalize() + ' New_reg_dau Done ******')

    if wau:
        process_bi_statistic_wau(target)
        print('******* ' + target.capitalize() + ' WAU Done *******')

    if mau:
        process_bi_statistic_mau(target)
        print('******* ' + target.capitalize() + ' MAU Done *******')

    if new_reg:
        process_bi_statistic_new_reg(target)
        print('******* ' + target.capitalize() + ' New_reg Done *******')

    if new_reg_dau:
        process_bi_statistic_new_reg_dau(target)
        print('******* ' + target.capitalize() + ' New_reg_dau Done ******')

    if gold_silver_consumption:
        process_bi_statistic_gold_silver_consumption(target)
        print('******* ' + target.capitalize() + ' gold_silver_consumption Done *******')

    if free_gold_silver:
        process_bi_statistic_free_transaction(target)
        print('******* ' + target.capitalize() + ' free_gold_silver Done *******')

    if payment_records:
        process_bi_statistic_payment_records(target)
        print('******* ' + target.capitalize() + ' payments_records Done *******')

    if retention:
        process_bi_statistic_retention(target)
        print('******* ' + target.capitalize() + ' retention Done *******')

    if revenue:
        process_bi_statistic_revenue(target)
        print('******* ' + target.capitalize() + ' revenue Done *******')

    if game_records:
        process_bi_statistic_game_records(target)
        print('******* ' + target.capitalize() + ' game_records Done *******')

