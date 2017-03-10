import grequests
import hashlib

from functools import partial

from flask import current_app as app
from app.extensions import db
from app.tasks import celery
from app.models.promotion import PromotionPush, PromotionPushHistory
from app.constants import PROMOTION_PUSH_HISTORY_STATUSES, PROMOTION_PUSH_TYPES
from app.utils import current_time


@celery.task
def process_facebook_notification(push_id=None):

    print('process_facebook_notification: preparing')

    if push_id is not None:
        # for retry
        push_histories = db.session.query(PromotionPushHistory).filter_by(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, status=PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value).all()
    else:
        # for scheduled
        now = current_time().format('YYYY-MM-DD HH:mm:ss')
        push_histories = db.session.query(PromotionPushHistory).filter_by(push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value).filter(PromotionPushHistory.scheduled_at <= now).all()

    if len(push_histories) == 0:
        print('process_facebook_notification: no data')
        return

    push_ids = []
    push_history_ids = []
    for item in push_histories:
        push_ids.append(item.push_id)
        push_history_ids.append(item.id)

    pushes = db.session.query(PromotionPush).filter(PromotionPush.id.in_(list(set(push_ids)))).all()
    id_to_message_mapping = {item.id: item.message for item in pushes}

    reqs = []
    for item in push_histories:
        data = {'access_token': '122212108221118|iY2aFJsHhyWUvTV_4oeoGrXX-TA', 'template': id_to_message_mapping[item.push_id]}
        headers = {'X-Correlation-Id': str(item.id)}
        req = grequests.post('https://graph.afacebook.com/v2.7/' + item.target + '/notifications',
                             data=data,
                             headers=headers,
                             timeout=5
                            )
        reqs.append(req)

    db.session.query(PromotionPushHistory).filter(PromotionPushHistory.id.in_(push_history_ids)).update({PromotionPushHistory.status: PROMOTION_PUSH_HISTORY_STATUSES.RUNNING.value}, synchronize_session=False)
    db.session.commit()

    print('process_facebook_notification: start sending')

    successed_push_history_ids = []
    for resp in grequests.imap(reqs, size=50):
        if resp:
            push_history_id = resp.request.headers['X-Correlation-Id']
            push_history = db.session.query(PromotionPushHistory).filter_by(id=push_history_id).one()

            content = resp.json()
            if content['error']:
                push_history.status = PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value
                push_history.error_message = content['error']['message']
            else:
                push_history.status = PROMOTION_PUSH_HISTORY_STATUSES.SUCCESS.value
                push_history.error_message = None
            db.session.commit()

            successed_push_history_ids.append(push_history_id)

    print('process_facebook_notification: finish sending')

    dead_push_history_ids = list(set(push_history_ids) - set(successed_push_history_ids))
    db.session.query(PromotionPushHistory).filter(PromotionPushHistory.id.in_(dead_push_history_ids)).update({PromotionPushHistory.status: PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value}, synchronize_session=False)
    db.session.commit()

    print('process_facebook_notification: done')

    return None
