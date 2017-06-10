import hashlib
import json

import arrow
import pandas as pd
import sqlparse
from dateutil import tz
from flask import Blueprint, render_template, request, jsonify
from flask import current_app as app
from flask_login import login_required, current_user
from sqlalchemy import text

from app.constants import PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES, TEST_EMAIL_ADDRESS
from app.extensions import db, sendgrid
from app.models.main import AdminUserQuery
from app.models.promotion import PromotionPush
from app.tasks.promotion import (process_promotion_facebook_notification_items,
                                 process_promotion_facebook_notification_retrying,
                                 process_promotion_email_notification_items, process_promotion_email_retrying)
from app.tasks.sendgrid import get_campaigns, get_senders, get_categories
from app.utils import error_msg_from_exception, current_time

promotion = Blueprint('promotion', __name__)


@promotion.route("/promotion/facebook_notification", methods=["GET"])
@login_required
def facebook_notification():
    based_query_id = request.args.get('based_query_id')
    if based_query_id is None:
        return render_template('promotion/facebook_notification.html')

    based_query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()
    if based_query is None:
        return render_template('promotion/facebook_notification.html')
    return render_template('promotion/facebook_notification.html', based_query=based_query)


@promotion.route("/promotion/facebook_notification/histories", methods=["GET"])
@login_required
def facebook_notification_histories():
    data = db.session.query(PromotionPush).filter_by(push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value).order_by(
        PromotionPush.created_at.desc()).limit(10).all()
    return jsonify(data=[item.to_dict() for item in data])


@promotion.route("/promotion/facebook_notification/retry", methods=["POST"])
@login_required
def facebook_notification_retry():
    push_id = request.form.get('push_id')
    if push_id:
        if app.config['ENV'] == 'prod':
            process_promotion_facebook_notification_retrying.delay(push_id)
        else:
            process_promotion_facebook_notification_retrying(push_id)
    return jsonify(result='ok')


@promotion.route("/promotion/facebook_notification/sender", methods=["POST"])
@login_required
def facebook_notification_sender():
    based_query_id = request.form.get('based_query_id')
    query_rules = request.form.get('query_rules')
    message = request.form.get('message')

    scheduled_at = request.form.get('scheduled_at')
    if scheduled_at:
        # from est to utc
        scheduled_at = arrow.get(scheduled_at).replace(tzinfo=tz.gettz(app.config['APP_TIMEZONE'])).to('UTC')
    else:
        # utc
        scheduled_at = current_time()

    formatted_message = message.strip()
    pending_digest = (str(current_user.id) + '_' + formatted_message + '_' + scheduled_at.format('YYYYMMDD')).encode(
        'utf-8')
    message_key = hashlib.md5(pending_digest).hexdigest()

    push = db.session.query(PromotionPush).filter_by(message_key=message_key).first()
    if push is None:
        push = PromotionPush(
            admin_user_id=current_user.id,
            based_query_id=based_query_id,
            push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value,
            message=formatted_message,
            message_key=message_key,
            status=PROMOTION_PUSH_STATUSES.PENDING.value,
            scheduled_at=scheduled_at.format('YYYY-MM-DD HH:mm:ss')
        )

        db.session.add(push)
        db.session.commit()
    else:
        return jsonify(
            error="You have been sent this message before, Please change message if you don't need to send the same message."), 500

    push_id = push.id

    try:
        if based_query_id:
            # based some query
            query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()

            stmt = sqlparse.parse(query.sql)[0]
            tokens = [str(item) for item in stmt.tokens]
            tokens[2] = 'user_id, facebook_id'
            slim_sql = ''.join(tokens)

            result_proxy = db.engine.execute(text(slim_sql))
            column_names = [col[0] for col in result_proxy.cursor.description]

            if 'facebook_id' in column_names:
                data = result_proxy.fetchall()

                df = pd.DataFrame(data, columns=column_names)

                data = [[row['user_id'], row['facebook_id']] for _, row in df.iterrows() if
                        (row['user_id'] is not None and row['facebook_id'] is not None)]

                if app.config['ENV'] == 'prod':
                    process_promotion_facebook_notification_items.delay(push_id, scheduled_at.format(), data=data)
                else:
                    process_promotion_facebook_notification_items(push_id, scheduled_at.format(), data=data)

                return jsonify(result='ok')
            else:
                return jsonify(error="based query don't have column: user_id, facebook_id"), 500


        elif query_rules:

            if app.config['ENV'] == 'prod':

                process_promotion_facebook_notification_items.delay(push_id, scheduled_at.format(),
                                                                    query_rules=query_rules)
            else:

                process_promotion_facebook_notification_items(push_id, scheduled_at.format(), query_rules=query_rules)

            return jsonify(result='ok')


        else:

            if app.config['ENV'] == 'prod':
                process_promotion_facebook_notification_items.delay(push_id, scheduled_at.format())
            else:
                process_promotion_facebook_notification_items(push_id, scheduled_at.format())

            return jsonify(result='ok')

    except Exception as e:

        return jsonify(error=error_msg_from_exception(e)), 500


@promotion.route("/promotion/email", methods=["GET"])
@login_required
def email_notification():
    campaigns = list(map(lambda x: {'id': x['id'], 'title': x['title'], 'status': x['status']}, get_campaigns()))
    statuses = list(set(map(lambda x: x['status'], campaigns)))

    senders = list(map(lambda x: {'id': x['id'], 'nickname': x['nickname']}, get_senders()))

    categories = list(map(lambda x: {'category': x['category']}, get_categories()))

    based_query_id = request.args.get('based_query_id')
    template_file = 'promotion/email.html'
    if based_query_id is None:
        return render_template(template_file, statuses=statuses, campaigns=campaigns, senders=senders,
                               categories=categories)

    based_query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()
    if based_query is None:
        return render_template(template_file, statuses=statuses, campaigns=campaigns)
    return render_template(template_file, based_query=based_query, statuses=statuses, campaigns=campaigns,
                           senders=senders, categories=categories)


@promotion.route("/promotion/email/histories", methods=["GET"])
@login_required
def email_histories():
    data = db.session.query(PromotionPush).filter_by(push_type=PROMOTION_PUSH_TYPES.EMAIL.value).order_by(
        PromotionPush.created_at.desc()).limit(10).all()

    return jsonify(data=[item.to_dict() for item in data])


@promotion.route("/promotion/email/retry", methods=["POST"])
@login_required
def email_retry():
    push_id = request.form.get('push_id')
    if push_id:
        if app.config['ENV'] == 'prod':
            process_promotion_email_retrying.delay(push_id)
        else:
            process_promotion_email_retrying(push_id)
    return jsonify(result='ok')


@promotion.route("/promotion/email/campaign_html_content", methods=["GET"])
@login_required
def email_campaign_html_content():
    campaign_id = int(request.args.get('campaign_id'))
    html_content = None
    for campaign in get_campaigns():
        if campaign_id == campaign['id']:
            html_content = campaign['html_content']
            break
    return jsonify(html_content=html_content)


@promotion.route("/promotion/email/sender_campaign", methods=["POST"])
def email_sender():
    based_query_id = request.form.get('based_query_id')
    query_rules = json.loads(request.form.get('query_rules', 'null'))
    scheduled_at = request.form.get('scheduled_at')
    campaign_id = request.form.get('campaign_id')
    email_subject = request.form.get('email_subject')
    email_content = [campaign['html_content'] for campaign in get_campaigns() if campaign['id'] == int(campaign_id)][0]

    from_sender = {"email": "no-reply@playwpt.com", "name": "PlayWPT"}
    reply_to = {"email": "no-reply@playwpt.com", "name": ""}

    email_content = email_content. \
        replace("[Unsubscribe]", '<%asm_group_unsubscribe_raw_url%>'). \
        replace("[Weblink]", "[%weblink%]"). \
        replace("[Unsubscribe_Preferences]", '<%asm_preferences_raw_url%>')

    suppression = {"group_id": 2161, "groups_to_display": [2161]}

    data = {"content": [{"type": "text/html", "value": email_content}], "from": from_sender, "reply_to": reply_to,
            "personalizations": [{"subject": email_subject, "to": TEST_EMAIL_ADDRESS}], 'asm': suppression}

    try:
        sendgrid.client.mail.send.post(request_body=data)

    except Exception as e:

        return jsonify(error=error_msg_from_exception(e)), 500
    else:

        if scheduled_at:

            # from est to utc
            scheduled_at = arrow.get(scheduled_at).replace(tzinfo=tz.gettz(app.config['APP_TIMEZONE'])).to('UTC')

        else:

            # utc

            scheduled_at = current_time()

        email_campaign = {"content": [{"type": "text/html", "value": email_content}], "from": from_sender,
                          "reply_to": reply_to, "subject": email_subject}

        email_campaign = json.dumps(email_campaign)

        pending_digest = (str(current_user.id) + '_' + email_campaign + '_' + scheduled_at.format('YYYYMMDD')).encode(
            'utf-8')
        message_key = hashlib.md5(pending_digest).hexdigest()
        push = db.session.query(PromotionPush).filter_by(message_key=message_key).first()

        if push is None:

            push = PromotionPush(
                admin_user_id=current_user.id,
                based_query_id=based_query_id,
                push_type=PROMOTION_PUSH_TYPES.EMAIL.value,
                message=email_campaign,
                message_key=message_key,
                status=PROMOTION_PUSH_STATUSES.PENDING.value,
                scheduled_at=scheduled_at.format('YYYY-MM-DD HH:mm:ss'))

            db.session.add(push)
            db.session.commit()

        else:

            return jsonify(
                error="You have been sent this email before, please change message if you don't need to send the same email."), 500

        push_id = push.id

        try:
            if based_query_id:
                # based some query
                query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()

                stmt = sqlparse.parse(query.sql)[0]
                tokens = [str(item) for item in stmt.tokens]
                tokens[2] = 'user_id'
                slim_sql = ''.join(tokens)

                result_proxy = db.engine.execute(text(slim_sql))
                column_names = [col[0] for col in result_proxy.cursor.description]

                if 'user_id' in column_names:

                    user_ids = list(result_proxy.fetchall())

                    user_ids = [i[0] for i in user_ids]

                    result_proxy = db.engine.execute(text(
                        """ SELECT user_id,  username,email,reg_country  FROM bi_user WHERE user_id IN :user_ids """),
                        user_ids=tuple(user_ids))

                    data = [{'user_id': row['user_id'], 'username': row['username'], 'country': row['reg_country'],
                             'email': row['email']} for row in result_proxy]

                    if app.config['ENV'] == 'prod':

                        process_promotion_email_notification_items.delay(push_id, scheduled_at.format(), data=data)

                    else:

                        process_promotion_email_notification_items(push_id, scheduled_at.format(), data=data)

                    return jsonify(result='ok')

                else:

                    return jsonify(error="Based query don't have column: user_id, email"), 500

            elif query_rules:

                if app.config['ENV'] == 'prod':

                    process_promotion_email_notification_items.delay(push_id, scheduled_at.format(),
                                                                     query_rules=query_rules)

                else:

                    process_promotion_email_notification_items(push_id, scheduled_at.format(), query_rules=query_rules)

                return jsonify(result='ok')

            else:

                if app.config['ENV'] == 'prod':
                    process_promotion_email_notification_items.delay(push_id, scheduled_at.format())
                else:
                    process_promotion_email_notification_items(push_id, scheduled_at.format())

                return jsonify(result='ok')

        except Exception as e:

            return jsonify(error=error_msg_from_exception(e)), 500


@promotion.route("/promotion/email/send_test_email", methods=["POST"])
def test_email():
    campaign_id = request.form.get('campaign_id')
    email_subject = request.form.get('email_subject')

    email_content = [campaign['html_content'] for campaign in get_campaigns() if campaign['id'] == int(campaign_id)][0]

    from_sender = {"email": "no-reply@playwpt.com", "name": "PlayWPT"}
    reply_to = {"email": "no-reply@playwpt.com", "name": ""}

    email_content = email_content. \
        replace("[Unsubscribe]", '<%asm_group_unsubscribe_raw_url%>'). \
        replace("[Weblink]", "[%weblink%]"). \
        replace("[Unsubscribe_Preferences]", '<%asm_preferences_raw_url%>')

    suppression = {"group_id": 2161, "groups_to_display": [2161]}

    data = {"content": [{"type": "text/html", "value": email_content}], "from": from_sender, "reply_to": reply_to,
            "personalizations": [{"subject": email_subject, "to": TEST_EMAIL_ADDRESS}], 'asm': suppression}

    try:
        sendgrid.client.mail.send.post(request_body=data)

    except Exception as e:

        return jsonify(error=error_msg_from_exception(e)), 500

    else:

        return jsonify('ok'), 202
