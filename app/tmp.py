from datetime import timedelta, datetime
from  random import choice
from random import randint

import pymysql.cursors

from app.constants import TRANSACTION_TYPES

connection = pymysql.connect(host='127.0.0.1',
                             port=3306,
                             user='root',
                             password='123',
                             db='bi_dev',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def random_date(start, end):
    return start + timedelta(
        seconds=randint(0, int((end - start).total_seconds())))


d11 = datetime.strptime('1/1/2017 1:30 PM', '%m/%d/%Y %I:%M %p')
d12 = datetime.strptime('2/1/2017 4:50 AM', '%m/%d/%Y %I:%M %p')

d21 = datetime.strptime('2/1/2017 1:30 PM', '%m/%d/%Y %I:%M %p')
d22 = datetime.strptime('3/1/2017 4:50 AM', '%m/%d/%Y %I:%M %p')

d31 = datetime.strptime('3/1/2017 1:30 PM', '%m/%d/%Y %I:%M %p')
d32 = datetime.strptime('3/21/2017 4:50 AM', '%m/%d/%Y %I:%M %p')

user_id_range = list(range(40000, 40528)) + list(range(1089089, 1089097)) + [1119276, 1120471, 1172459, 1204988,
                                                                             1226962, 1310742, 2048458, 2061936,
                                                                             2091893, ] + list(range(2120478, 2283357))
created_at_range_1 = [str((random_date(d11, d12))) for i in range(1000)]
created_at_range_2 = [str((random_date(d21, d22))) for i in range(1000)]
created_at_range_3 = [str((random_date(d31, d32))) for i in range(1000)]
game_id_and_currency_type_range = list(zip([39990, 23118], ['Gold', 'Silver']))
transaction_type_list = list(TRANSACTION_TYPES)
transaction_amount_list = range(10000)


def main():
    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO `bi_user_currency` (`orig_id`, `user_id` ,`og_account`, `game_id`,
                             `currency_type`, `transaction_type`, `transaction_amount`,`balance`,
                             `user_id_updated`, `created_at`) VALUES (%s,  %s, %s, %s, %s, %s, %s, %s,%s,%s)
                             """
            orig_id = 0
            n = 0

            for i in range(100000):
                n += 1
                print(n)

                user_id = choice(user_id_range)
                game_id, currency_type = choice(game_id_and_currency_type_range)
                created_at = choice(created_at_range_1)
                transaction_type = choice(transaction_type_list)
                transaction_amount = choice(transaction_amount_list)

                cursor.execute(sql,
                               (orig_id, user_id, '123', game_id, currency_type, transaction_type,
                                transaction_amount, 0, 0,
                                created_at))

            for i in range(200000):
                n += 1
                print(n)

                user_id = choice(user_id_range)
                game_id, currency_type = choice(game_id_and_currency_type_range)
                created_at = choice(created_at_range_2)
                transaction_type = choice(transaction_type_list)
                transaction_amount = choice(transaction_amount_list)

                cursor.execute(sql,
                               (orig_id, user_id, '123', game_id, currency_type, transaction_type,
                                transaction_amount, 0, 0,
                                created_at))

            for i in range(300000):
                n += 1
                print(n)

                user_id = choice(user_id_range)
                game_id, currency_type = choice(game_id_and_currency_type_range)
                created_at = choice(created_at_range_3)
                transaction_type = choice(transaction_type_list)
                transaction_amount = choice(transaction_amount_list)

                cursor.execute(sql,
                               (orig_id, user_id, '123', game_id, currency_type, transaction_type,
                                transaction_amount, 0, 0,
                                created_at))

            connection.commit()

    finally:
        connection.close()


if __name__ == '__main__':
    main()
