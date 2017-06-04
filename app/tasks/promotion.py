import json
import urllib.parse
from random import randrange

import arrow
import requests
from sqlalchemy import text
from sqlalchemy.sql.expression import bindparam

from app.constants import PROMOTION_PUSH_HISTORY_STATUSES, PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES
from app.extensions import db
from app.extensions import sendgrid
from app.models.promotion import PromotionPush, PromotionPushHistory
from app.tasks import celery, with_db_context
from app.utils import current_time, error_msg_from_exception


def get_facebook_users():
    def collection(connection, transaction):
        return connection.execute(text('SELECT u_id AS user_id, pu_id AS platform_user_id FROM tb_platform_user_info'))

    result_proxy = with_db_context(db, collection, 'orig_wpt')
    return [[row['user_id'], row['platform_user_id']] for row in result_proxy]


def get_email_users():
    # def collection(connection, transaction):
    #     return connection.execute(text(""" SELECT username,reg_country,reg_state,email  FROM bi_user"""))
    #
    # result_proxy = with_db_context(db, collection)
    #
    # recipients = [{'user_id': row['user_id'], 'username': row['username'], 'country': row['reg_country'],
    #                'state': row['reg_sate'], 'email': row['email']} for row in result_proxy]

    #
    # custom_field = {'xxxx': 1}.update({'query':111})
    #
    # [recipient.update(custom_field) for recipient in recipients]
    #
    # return recipients

    recipients = [{'username': 'fanhaipeng', 'country': 'China', 'state': 'beijing',
                   'email': 'fanhp@ourgame.com'}]
    return recipients


def update_promotion_status(data, status_value):
    where = PromotionPushHistory.__table__.c.id == bindparam('_id')

    if status_value == PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value:
        values = {'status': status_value, 'error_message': bindparam('error_message')}
    else:
        values = {'status': status_value, 'error_message': None}

    db.engine.execute(PromotionPushHistory.__table__.update().where(where).values(values), data)


@celery.task
def process_promotion_facebook_notification_items(push_id, scheduled_at, data=None):
    try:
        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.PREPARING.value}, synchronize_session=False)
        db.session.commit()

        rows = []

        scheduled_at = arrow.get(scheduled_at)
        status_value = PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value
        push_type_value = PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value

        if data is None:
            data = get_facebook_users()
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({
                    'push_id': push_id,
                    'push_type': push_type_value,
                    'user_id': row['user_id'],
                    'target': row[1],
                    'scheduled_at': scheduled_time,
                    'status': status_value
                })
        else:
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({
                    'push_id': push_id,
                    'push_type': push_type_value,
                    'user_id': row[0],
                    'target': row[1],
                    'scheduled_at': scheduled_time,
                    'status': status_value
                })

        db.engine.execute(PromotionPushHistory.__table__.insert(), rows)

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.SCHEDULED.value}, synchronize_session=False)
        db.session.commit()
    except Exception as e:
        print('process_promotion_facebook_notification_items exception: ' + error_msg_from_exception(e))
        db.session.rollback()

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)
        db.session.commit()


@celery.task
def process_promotion_facebook_notification_retrying(push_id):
    status_values = [PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value]
    db.session.query(PromotionPushHistory).filter_by(push_id=push_id) \
        .filter(PromotionPushHistory.status.in_(status_values)) \
        .update({PromotionPushHistory.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)
    db.session.commit()


@celery.task
def process_promotion_facebook_notification():
    print('process_promotion_facebook_notification: preparing')

    now = current_time().format('YYYY-MM-DD HH:mm:ss')
    push_histories = db.session.query(PromotionPushHistory).filter_by(
        push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value,
        status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value) \
        .filter(PromotionPushHistory.scheduled_at <= now) \
        .all()

    if len(push_histories) == 0:
        print('process_promotion_facebook_notification: no data')
        return

    fb_name_identifier = '@[fb_name]'

    push_ids = [item.push_id for item in push_histories]
    pushes = db.session.query(PromotionPush).filter(PromotionPush.id.in_(list(set(push_ids)))).all()
    id_to_message_mapping = {item.id: item.message for item in pushes}
    id_to_fb_name_replace_mapping = {item.id: fb_name_identifier in item.message for item in pushes}

    print('process_promotion_facebook_notification: start sending')
    update_promotion_status([{'_id': item.id} for item in push_histories],
                            PROMOTION_PUSH_HISTORY_STATUSES.RUNNING.value)

    iter_step = 50
    for i in range(0, len(push_histories), iter_step):
        partitions = push_histories[i:i + iter_step]
        partition_ids = [item.id for item in partitions]

        data = {
            'access_token': '122212108221118|iY2aFJsHhyWUvTV_4oeoGrXX-TA',
            'include_headers': 'false',
            'batch': json.dumps([{'method': 'POST',
                                  'relative_url': 'v2.7/' + item.target + '/notifications',
                                  'body': urllib.parse.urlencode({'template': id_to_message_mapping[
                                      item.push_id].replace(fb_name_identifier, '@[%s]' % item.target) if
                                  id_to_fb_name_replace_mapping[item.push_id] else id_to_message_mapping[
                                      item.push_id]})} for item in partitions])
        }

        try:
            req = requests.post('https://graph.facebook.com', data=data)
        except Exception as e:
            print('process_promotion_facebook_notification send request exception: ' + error_msg_from_exception(e))
            update_promotion_status([{'_id': partition_id} for partition_id in partition_ids],
                                    PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value)
        else:
            if req:
                print('process_promotion_facebook_notification: starting process response.')

                succeeded_partition_data = []
                failed_partition_data = []

                responses = req.json()
                for idx, response in enumerate(responses):
                    push_history_id = str(partition_ids[idx])
                    resp_body = json.loads(response['body'])
                    if 'error' in resp_body:
                        failed_partition_data.append(
                            {'_id': push_history_id, 'error_message': resp_body['error']['message'].split('.')[0]})
                    else:
                        succeeded_partition_data.append({'_id': push_history_id})

                print('process_promotion_facebook_notification succeeded count: ' + str(
                    len(succeeded_partition_data)) + ', failed count: ' + str(len(failed_partition_data)))

                if len(succeeded_partition_data) > 0:
                    update_promotion_status(succeeded_partition_data,
                                            PROMOTION_PUSH_HISTORY_STATUSES.SUCCESS.value)

                if len(failed_partition_data) > 0:
                    update_promotion_status(failed_partition_data,
                                            PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value)
            else:
                print('process_promotion_facebook_notification: no response.')

    print('process_promotion_facebook_notification: done')

    return None


@celery.task
def process_promotion_email_notification_items(push_id, scheduled_at, data=None):
    try:
        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.PREPARING.value}, synchronize_session=False)
        db.session.commit()

        rows = []

        scheduled_at = arrow.get(scheduled_at)
        status_value = PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value
        push_type_value = PROMOTION_PUSH_TYPES.EMAIL.value

        if data is None:
            data = get_email_users()
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({'push_id': push_id,
                             'push_type': push_type_value,
                             'user_id': row['user_id'],
                             'target': row,
                             'scheduled_at': scheduled_time,
                             'status': status_value
                             })
        else:
            max_minutes = int(len(data) / 1000) + 1
            for row in data:
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({
                    'push_id': push_id,
                    'push_type': push_type_value,
                    'user_id': row[0],
                    'target': row[1],
                    'scheduled_at': scheduled_time,
                    'status': status_value
                })

        db.engine.execute(PromotionPushHistory.__table__.insert(), rows)

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.SCHEDULED.value}, synchronize_session=False)

        db.session.commit()

    except Exception as e:

        print('process_promotion_email_notification_items exception: ' + error_msg_from_exception(e))

        db.session.rollback()

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)

        db.session.commit()


@celery.task
def process_promotion_email_retrying(push_id):
    status_values = [PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value]

    db.session.query(PromotionPushHistory).filter_by(push_id=push_id).filter(
        PromotionPushHistory.status.in_(status_values)).update(
        {PromotionPushHistory.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)

    db.session.commit()


@celery.task
def process_promotion_email():
    print('process_promotion_email_notification: preparing')

    now = current_time().format('YYYY-MM-DD HH:mm:ss')

    push_histories = db.session.query(PromotionPushHistory).filter_by(push_type=PROMOTION_PUSH_TYPES.EMAIL.value,
                                                                      status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value).filter(
        PromotionPushHistory.scheduled_at <= now).all()

    if len(push_histories) == 0:
        print('process_promotion_email_notification: no data')
        return

    push_ids = [item.push_id for item in push_histories]

    pushes = db.session.query(PromotionPush).filter(PromotionPush.id.in_(list(set(push_ids)))).all()

    print('process_email_facebook_notification: start sending')

    update_promotion_status([{'_id': item.id} for item in push_histories],
                            PROMOTION_PUSH_HISTORY_STATUSES.RUNNING.value)

    push_ids = [item.push_id for item in push_histories]

    pushes = db.session.query(PromotionPush).filter(PromotionPush.id.in_(list(set(push_ids)))).all()

    id_to_email_mapping = {item.id: item.message for item in pushes}

    id_to_campaign = {id: id_to_email_mapping[id].split('<--->')[0] for id in id_to_email_mapping}

    id_to_recipients = {id: id_to_email_mapping[id].split('<--->')[0] for id in id_to_email_mapping}

    id_to_recipients_list = {id: id_to_email_mapping[id].split('<--->')[0] for id in id_to_email_mapping}

    # TODO 测试完成后改成一次发1000条
    # TOD0  删除campaign_id
    # TOD0  删除lists
    # TOD0  删除recipients

    for item in push_histories:

        try:

            data = [json.loads(item.target)]

            response = sendgrid.client.contactdb.recipients.post(request_body=data)

        except Exception as e:

            print('process_promotion_email create recipients  sendgrid  exception: ' + error_msg_from_exception(e))

            update_promotion_status([{'_id': push_id} for push_id in push_ids],
                                    PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value)

        else:

            try:

                data = {"name": "your list name"}

                response = sendgrid.client.contactdb.lists.post(request_body=data)


            except Exception as e:

                print('process_promotion_email create list  request exception: ' + error_msg_from_exception(e))

                update_promotion_status([{'_id': push_id} for push_id in push_ids],
                                        PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value)

            else:
                try:

                    data = [
                        "recipient_id1",
                        "recipient_id2"
                    ]
                    list_id = "test_url_param"

                    response = sendgrid.client.contactdb.lists._(list_id).recipients.post(request_body=data)


                except Exception as e:

                    print(
                        'process_promotion_email add recipients to list request exception: ' + error_msg_from_exception(
                            e))

                    update_promotion_status([{'_id': push_id} for push_id in push_ids],
                                            PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value)

                else:

                    try:
                        data = {
                            "title": "March Newsletter",
                            "subject": "New Products for Spring!",
                            "sender_id": 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx要删除的,再使用',
                            "list_ids": [
                                110,
                                124
                            ],
                            "segment_ids": [
                                110
                            ],
                            "categories": [
                                "spring line"
                            ],
                            "suppression_group_id": 42,
                            "custom_unsubscribe_url": "",
                            "ip_pool": "marketing",
                            "html_content": "<html><head><title></title></head><body><p>Check out our spring line!</p></body></html>",
                            "plain_content": "Check out our spring line!"
                        }

                        response = sendgrid.client.campaigns.post(request_body=data)

                        # 第一次验证，之后不在验证

                        data = {"to": "your.email@example.com"}
                        campaign_id = "test_url_param"
                        response = sendgrid.client.campaigns._(campaign_id).schedules.test.post(request_body=data)



                    except Exception as e:

                        print(
                            'process_promotion_email created campaign  request exception: ' + error_msg_from_exception(
                                e))

                        update_promotion_status([{'_id': push_id} for push_id in push_ids],
                                                PROMOTION_PUSH_HISTORY_STATUSES.REQUEST_FAILED.value)


                    else:

                        try:

                            campaign_id = "test_url_param"

                            response = sendgrid.client.campaigns._(campaign_id).schedules.now.post()


                        except Exception as e:

                            'xxxx'

                        else:
                            pass


data = {
    "asm": {
        "group_id": 1,
        "groups_to_display": [
            1,
            2,
            3
        ]
    },
    "attachments": [
        {
            "content": "[BASE64 encoded content block here]",
            "content_id": "ii_139db99fdb5c3704",
            "disposition": "inline",
            "filename": "file1.jpg",
            "name": "file1",
            "type": "jpg"
        }
    ],
    "batch_id": "[YOUR BATCH ID GOES HERE]",
    "categories": [
        "category1",
        "category2"
    ],
    "content": [
        {
            "type": "text/html",
            "value": "<html><p>Hello, world!</p><img src=[CID GOES HERE]></img></html>"
        }
    ],
    "custom_args": {
        "New Argument 1": "New Value 1",
        "activationAttempt": "1",
        "customerAccountNumber": "[CUSTOMER ACCOUNT NUMBER GOES HERE]"
    },
    "from": {
        "email": "sam.smith@example.com",
        "name": "Sam Smith"
    },
    "headers": {},
    "ip_pool_name": "[YOUR POOL NAME GOES HERE]",
    "mail_settings": {
        "bcc": {
            "email": "ben.doe@example.com",
            "enable": True
        },
        "bypass_list_management": {
            "enable": True
        },
        "footer": {
            "enable": True,
            "html": "<p>Thanks</br>The SendGrid Team</p>",
            "text": "Thanks,/n The SendGrid Team"
        },
        "sandbox_mode": {
            "enable": False
        },
        "spam_check": {
            "enable": True,
            "post_to_url": "http://example.com/compliance",
            "threshold": 3
        }
    },
    "personalizations": [
        {
            "bcc": [
                {
                    "email": "sam.doe@example.com",
                    "name": "Sam Doe"
                }
            ],
            "cc": [
                {
                    "email": "jane.doe@example.com",
                    "name": "Jane Doe"
                }
            ],
            "custom_args": {
                "New Argument 1": "New Value 1",
                "activationAttempt": "1",
                "customerAccountNumber": "[CUSTOMER ACCOUNT NUMBER GOES HERE]"
            },
            "headers": {
                "X-Accept-Language": "en",
                "X-Mailer": "MyApp"
            },
            "send_at": 1409348513,
            "subject": "Hello, World!",
            "substitutions": {
                "id": "substitutions",
                "type": "object"
            },
            "to": [
                {
                    "email": "john.doe@example.com",
                    "name": "John Doe"
                }
            ]
        }
    ],
    "reply_to": {
        "email": "sam.smith@example.com",
        "name": "Sam Smith"
    },
    "sections": {
        "section": {
            ":sectionName1": "section 1 text",
            ":sectionName2": "section 2 text"
        }
    },
    "send_at": 1409348513,
    "subject": "Hello, World!",
    "template_id": "[YOUR TEMPLATE ID GOES HERE]",
    "tracking_settings": {
        "click_tracking": {
            "enable": True,
            "enable_text": True
        },
        "ganalytics": {
            "enable": True,
            "utm_campaign": "[NAME OF YOUR REFERRER SOURCE]",
            "utm_content": "[USE THIS SPACE TO DIFFERENTIATE YOUR EMAIL FROM ADS]",
            "utm_medium": "[NAME OF YOUR MARKETING MEDIUM e.g. email]",
            "utm_name": "[NAME OF YOUR CAMPAIGN]",
            "utm_term": "[IDENTIFY PAID KEYWORDS HERE]"
        },
        "open_tracking": {
            "enable": True,
            "substitution_tag": "%opentrack"
        },
        "subscription_tracking": {
            "enable": True,
            "html": "If you would like to unsubscribe and stop receiving these emails <% clickhere %>.",
            "substitution_tag": "<%click here%>",
            "text": "If you would like to unsubscribe and stop receiveing these emails <% click here %>."
        }
    }
}
response = sg.client.mail.send.post(request_body=data)
print(response.status_code)
print(response.body)
print(response.headers)
