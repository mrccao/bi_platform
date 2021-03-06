import json
import urllib.parse

import arrow
import re
import requests
from random import randrange
from sqlalchemy import text, func
from sqlalchemy.sql.expression import bindparam

from app.constants import PROMOTION_PUSH_HISTORY_STATUSES, PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES
from app.extensions import db, sendgrid
from app.models.promotion import PromotionPush, PromotionPushHistory, UsersGrouping
from app.tasks import celery, with_db_context
from app.utils import current_time, error_msg_from_exception


def get_facebook_users():
    def collection(connection, transaction):
        return connection.execute(text('SELECT u_id AS user_id, pu_id AS platform_user_id FROM tb_platform_user_info'))

    result_proxy = with_db_context(db, collection, 'orig_wpt')
    return [[row['user_id'], row['platform_user_id']] for row in result_proxy]


def get_email_users():
    def collection(connection, transaction):
        return connection.execute(
            text(""" SELECT user_id,  username,reg_country,reg_state,email  FROM bi_user WHERE user_id"""))

    result_proxy = with_db_context(db, collection)

    return [
        {'user_id': row['user_id'], 'username': row['username'], 'country': row['reg_country'], 'email': row['email']}
        for row in result_proxy if row['email'] is not None]

    # return [{'user_id': 1, 'email': 'fanhaipeng0403@gmail.com', 'username': 'fanhaipeng'}]


def update_promotion_status(data, status_value):
    where = PromotionPushHistory.__table__.c.id == bindparam('_id')

    if status_value == PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value:
        values = {'status': status_value, 'error_message': bindparam('error_message')}
    else:
        values = {'status': status_value, 'error_message': None}

    db.engine.execute(PromotionPushHistory.__table__.update().where(where).values(values), data)


@celery.task
def process_promotion_facebook_notification_items(push_id, scheduled_at, query_rules=None, data=None):
    try:

        if query_rules is not None:

            data = UsersGrouping.generate_recipients(query_rules, PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value)

            if not data: return

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
                    'user_id': row[0],
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
def process_promotion_email_notification_items(push_id, scheduled_at, query_rules=None, data=None):
    try:
        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.PREPARING.value}, synchronize_session=False)
        db.session.commit()

        rows = []

        scheduled_at = arrow.get(scheduled_at)
        status_value = PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value
        push_type_value = PROMOTION_PUSH_TYPES.EMAIL.value

        if query_rules is not None:

            data = UsersGrouping.generate_recipients(query_rules, PROMOTION_PUSH_TYPES.EMAIL.value)

            if not data: return

        if data is None:

            data = get_email_users()

            max_minutes = int(len(data) / 1000) + 1
            for recipients in data:
                user_id = recipients['user_id']
                recipients = json.dumps(recipients)
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({'push_id': push_id,
                             'push_type': push_type_value,
                             'user_id': user_id,
                             'target': recipients,
                             'scheduled_at': scheduled_time,
                             'status': status_value
                             })
        else:
            max_minutes = int(len(data) / 1000) + 1
            for recipients in data:
                user_id = recipients['user_id']
                recipients = json.dumps(recipients)

                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format(
                    'YYYY-MM-DD HH:mm:ss')
                rows.append({
                    'push_id': push_id,
                    'push_type': push_type_value,
                    'user_id': user_id,
                    'target': recipients,
                    'scheduled_at': scheduled_time,
                    'status': status_value
                })

        db.engine.execute(PromotionPushHistory.__table__.insert(), rows)

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.SCHEDULED.value}, synchronize_session=False)

        db.session.commit()


    except Exception as e:

        print('process_promotion_email_items exception: ' + error_msg_from_exception(e))

        db.session.rollback()

        db.session.query(PromotionPush).filter_by(id=push_id).update(
            {PromotionPush.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)

        db.session.commit()


@celery.task
def process_promotion_email_retrying(push_id):
    status_values = [PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value]

    db.session.query(PromotionPushHistory).filter_by(push_id=push_id).filter(
        PromotionPushHistory.status.in_(status_values)).update(
        {PromotionPushHistory.status: PROMOTION_PUSH_STATUSES.FAILED.value}, synchronize_session=False)

    db.session.commit()


@celery.task
def process_promotion_email():
    print('process_promotion_email: preparing')

    now = current_time().format('YYYY-MM-DD HH:mm:ss')

    push_histories = db.session.query(PromotionPushHistory.push_id, func.count(PromotionPushHistory.push_id)). \
        filter_by(push_type=PROMOTION_PUSH_TYPES.EMAIL.value, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value) \
        .filter(PromotionPushHistory.scheduled_at <= now).group_by(PromotionPushHistory.push_id).all()

    if not any([item[1] for item in push_histories]):
        print('process_promotion_email: no data')
        return

    for item in push_histories:

        push_id = item[0]
        result_proxy = list(db.session.query(PromotionPushHistory.id, PromotionPushHistory.target).filter(
            PromotionPushHistory.push_id == push_id).all())
        recipient_ids = [item[0] for item in result_proxy]
        recipients = [json.loads(item[1]) for item in result_proxy]

        id_to_recipient_mapping = dict(zip(recipient_ids, recipients))

        print('process_promotion_email: start sending')

        update_promotion_status([{'_id': id} for id in recipient_ids], PROMOTION_PUSH_HISTORY_STATUSES.RUNNING.value)

        email_campaign = json.loads(db.session.query(PromotionPush).get(push_id).message)

        iter_step = 800

        for i in range(0, len(recipient_ids), iter_step):

            partitions_recipient_ids = recipient_ids[i:i + iter_step]

            personalizations = []

            for recipient_id in partitions_recipient_ids:
                recipient = id_to_recipient_mapping[recipient_id]

                personalizations.append({"to": [{'email': recipient.get('email')}],
                                         "substitutions": {"-country-": recipient.get("country"),
                                                           "-email-": recipient.get("email"),
                                                           "-Play_Username-": recipient.get("username")}})

            def build_email_campaign(email_campaign, personalizations):

                email_campaign['personalizations'] = personalizations

                #  ubsubscribe
                email_content = email_campaign["content"][0]["value"]

                # custom field

                pattern = re.compile(r'\[%.*?%\]')
                custom_fields = list(re.findall(pattern, email_content))
                custom_fields_format = ['[%' + (field.split('%')[1]).split(' ')[0] + '%]' for field in custom_fields]
                for i in range(0, len(custom_fields)):
                    email_content = email_content.replace(custom_fields[i], custom_fields_format[i])

                email_content = email_content. \
                    replace("[Unsubscribe]", '<%asm_group_unsubscribe_raw_url%>'). \
                    replace("[Weblink]", "https://www.playwpt.com"). \
                    replace("[Sender_Name]", "PlayWPT"). \
                    replace("[Sender_Address]", "1920 Main Street, Suite 1150"). \
                    replace("[Sender_State]", "CA"). \
                    replace("[Sender_City]", "Irvine"). \
                    replace("[Sender_Zip]", "92614"). \
                    replace("[Unsubscribe_Preferences]", '<%asm_preferences_raw_url%>'). \
                    replace("[%country%]", "-country-"). \
                    replace("[%Play_Username%]", "-Play_Username-"). \
                    replace("[%email%]", "-email-")

                email_campaign["content"][0]["value"] = email_content

                # ubsubscribe
                suppression = {"group_id": 2161, "groups_to_display": [2161]}
                email_campaign['asm'] = suppression

                return email_campaign

            data = build_email_campaign(email_campaign, personalizations)

            try:

                sendgrid.client.mail.send.post(request_body=data)

            except Exception as e:

                print('process_promotion_email: sendgrid request exception: ' + error_msg_from_exception(e))

                update_promotion_status([{'_id': recipient_id, 'error_message': error_msg_from_exception(e)
                                          } for recipient_id in partitions_recipient_ids],
                                        PROMOTION_PUSH_HISTORY_STATUSES.FAILED.value)

            else:

                print('process_promotion_email: sendgrid have accept the email campaign.')

                update_promotion_status([{'_id': recipient_id} for recipient_id in partitions_recipient_ids],
                                        PROMOTION_PUSH_HISTORY_STATUSES.SUCCESS.value)

        print('process_promotion_email: push_id = ' + str(push_id) + ' done')

    else:
        print('process_promotion_email: All done')
