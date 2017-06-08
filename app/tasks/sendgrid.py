import json
from app.extensions import sendgrid, cache
from app.tasks import celery
from app.utils import error_msg_from_exception


@celery.task
@cache.cached(timeout=60 * 60 * 24, key_prefix='sendgrid/campaigns')
def get_campaigns():
    try:
        response = sendgrid.client.campaigns.get(query_params={'limit': 999999, 'offset': 1})
        parsed_response = json.loads(response.body.decode())

        return parsed_response['result']
    except Exception as e:
        print('get_campaigns exception: ' + error_msg_from_exception(e))
        return []



@celery.task
@cache.cached(timeout=60 * 60 * 24, key_prefix='sendgrid/senders')
def get_senders():
    try:
        response = sendgrid.client.senders.get()
        parsed_response = json.loads(response.body.decode())

        return parsed_response

    except Exception as e:
        print('get_senders exception: ' + error_msg_from_exception(e))
        return []




@celery.task
@cache.cached(timeout=60 * 60 * 24, key_prefix='sendgrid/categories')
def get_categories():
    try:
        response = sendgrid.client.categories.get(query_params={'limit': 100, 'offset': 1})
        parsed_response = json.loads(response.body.decode())
        return parsed_response

    except Exception as e:
        print('get_categories exception: ' + error_msg_from_exception(e))
        return []


#
#
# @celery.task
# @cache.cached(timeout=60 * 60 * 24, key_prefix='sendgrid/suppression')
# def get_asm():
#     try:
#         response = sendgrid.client.asm.groups.get()
#         parsed_response = json.loads(response.body.decode())
#         return parsed_response
#
#     except Exception as e:
#         print('get_suppression exception: ' + error_msg_from_exception(e))
#         return []



# b'[{"id":2157,"name":"Product Announcements","description":"New product announcements","last_email_sent_at":null,"is_default":false,"unsubscribes":2},{"id":2161,"name":"Promotional Offers \\u0026 Free Chip","description":"Notification of free chips, bonus spins \\u0026 other promotional offers","last_email_sent_at":null,"is_default":true,"unsubscribes":547},{"id":2163,"name":"Account \\u0026 Transactional","description":"Notifications of transactions pertaining to PlayWPT account, i.e. receipts, results \\u0026 account change","last_email_sent_at":null,"is_default":false,"unsubscribes":28}]'



# test
# Email Subject       email  : subject



