import json
import urllib.parse
from random import randrange

import arrow
import requests

from app.constants import PROMOTION_PUSH_HISTORY_STATUSES, PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES
from app.extensions import db
from app.models.orig_wpt import WPTPlatformUser
from app.models.promotion import PromotionPush, PromotionPushHistory
from app.tasks import celery
from app.utils import current_time, error_msg_from_exception


@celery.task
def process_facebook_notification_items(push_id, scheduled_at, data=None):
    try:
        db.session.query(PromotionPush).filter_by(id=push_id).update({PromotionPush.status: PROMOTION_PUSH_STATUSES.PREPARING.value}, synchronize_session=False)
        db.session.commit()

        scheduled_at = arrow.get(scheduled_at)
        if data is None:
            data = db.session.query(WPTPlatformUser.user_id, WPTPlatformUser.platform_user_id).all()
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format('YYYY-MM-DD HH:mm:ss')
                db.session.add(PromotionPushHistory(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, user_id=row[0], target=row[1], scheduled_at=scheduled_time, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value))
        else:
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format('YYYY-MM-DD HH:mm:ss')
                db.session.add(PromotionPushHistory(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, user_id=row[0], target=row[1], scheduled_at=scheduled_time, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value))
        db.session.commit()

        db.session.query(PromotionPush).filter_by(id=push_id).update({PromotionPush.status: PROMOTION_PUSH_STATUSES.SCHEDULED.value}, synchronize_session=False)
        db.session.commit()
    except Exception as e:
        print('process_facebook_notification_items: ' + error_msg_from_exception(e))
        db.session.rollback()

        db.session.query(PromotionPush).filter_by(id=push_id).update({PromotionPush.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)
        db.session.commit()


@celery.task
def process_facebook_notification(push_id=None):

    print('process_facebook_notification: preparing')

    if push_id is not None: # for retry
        push_histories = db.session.query(PromotionPushHistory).filter_by(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, status=PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value).all()
    else: # for scheduled
        now = current_time().format('YYYY-MM-DD HH:mm:ss')
        push_histories = db.session.query(PromotionPushHistory).filter_by(push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value).filter(PromotionPushHistory.scheduled_at <= now).all()

    if len(push_histories) == 0:
        print('process_facebook_notification: no data')
        return

    push_ids = [item.push_id for item in push_histories]
    pushes = db.session.query(PromotionPush).filter(PromotionPush.id.in_(list(set(push_ids)))).all()
    id_to_message_mapping = {item.id: item.message for item in pushes}

    print('process_facebook_notification: start sending')

    db.session.query(PromotionPushHistory).filter(PromotionPushHistory.id.in_([item.id for item in push_histories])).update({PromotionPushHistory.status: PROMOTION_PUSH_HISTORY_STATUSES.RUNNING.value}, synchronize_session=False)
    db.session.commit()

    iter_step = 100
    for i in range(0, len(push_histories), iter_step):
        group = push_histories[i:i+iter_step]
        push_history_ids = [item.id for item in group]

        data = {
            'access_token': '122212108221118|iY2aFJsHhyWUvTV_4oeoGrXX-TA',
            'include_headers': 'false',
            'batch': json.dumps([{'method': 'POST',
                                  'relative_url': 'v2.7/' + item.target + '/notifications',
                                  'body' : urllib.parse.urlencode(
                                      {
                                          'template': id_to_message_mapping[item.push_id]
                                      }
                                  )
                                 } for item in group])
            }

        try:
            req = requests.post('https://graph.facebook.com', data=data, timeout=10)
        except Exception as e:
            db.session.query(PromotionPushHistory).filter(PromotionPushHistory.id.in_(push_history_ids)).update({PromotionPushHistory.status: PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value}, synchronize_session=False)
            db.session.commit()
        else:
            if req:
                succeeded_push_history_ids = []
                responses = req.json()
                for idx, response in enumerate(responses):
                    push_history_id = str(push_history_ids[idx])
                    resp_body = json.loads(response['body'])
                    if 'error' in resp_body:
                        push_history = db.session.query(PromotionPushHistory).filter_by(id=push_history_id).one()
                        push_history.status = PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value
                        push_history.error_message = resp_body['error']['message'].split('.')[0]
                        db.session.commit()
                    else:
                        succeeded_push_history_ids.append(push_history_id)

                if len(succeeded_push_history_ids) > 0:
                    db.session.query(PromotionPushHistory).filter(PromotionPushHistory.id.in_(succeeded_push_history_ids)).update({PromotionPushHistory.status: PROMOTION_PUSH_HISTORY_STATUSES.SUCCESS.value, PromotionPushHistory.error_message: None}, synchronize_session=False)
                    db.session.commit()

    print('process_facebook_notification: done')

    return None
