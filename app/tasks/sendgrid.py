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





