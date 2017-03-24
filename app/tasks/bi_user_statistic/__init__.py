from .recharge_history import process_bi_user_statistic_recharge_history


def process_bi_user_statistic(target, recharge=1):
    if recharge:

        try:
            process_bi_user_statistic_recharge_history(target)
        except:
            print('-----Recharge_history Failed-----')
            raise
        else:
            print('-----Recharge_history Done-----')
