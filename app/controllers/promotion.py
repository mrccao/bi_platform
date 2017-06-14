import json

import arrow
import hashlib
import pandas as pd
import re
import sqlparse
from dateutil import tz
from flask import Blueprint, render_template, request, jsonify
from flask import current_app as app
from flask_login import login_required, current_user
from sqlalchemy import text

from app.constants import PROMOTION_PUSH_STATUSES, PROMOTION_PUSH_TYPES, TEST_EMAIL_RECIPIENTS, REPLY_TO, FROM_SENDER
from app.extensions import db, sendgrid
from app.models.main import AdminUserQuery
from app.models.promotion import PromotionPush
from app.tasks.promotion import (process_promotion_facebook_notification_items,
                                 process_promotion_facebook_notification_retrying,
                                 process_promotion_email_notification_items, process_promotion_email_retrying)
from app.tasks.sendgrid import get_campaigns
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
    query_rules = json.loads(request.form.get('query_rules', 'null'))
    query_rules_sql = request.form.get("query_rules_sql")
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
            based_query_rules=query_rules_sql,
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


        elif query_rules is not None:

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

    based_query_id = request.args.get('based_query_id')
    template_file = 'promotion/email.html'
    if based_query_id is None:
        return render_template(template_file, statuses=statuses, campaigns=campaigns)

    based_query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()
    if based_query is None:
        return render_template(template_file, statuses=statuses, campaigns=campaigns)
    return render_template(template_file, based_query=based_query, statuses=statuses, campaigns=campaigns)


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
    query_rules_sql = request.form.get("query_rules_sql")
    scheduled_at = request.form.get('scheduled_at')
    campaign_id = request.form.get('campaign_id')
    email_subject = request.form.get('email_subject')
    email_content = [campaign['html_content'] for campaign in get_campaigns() if campaign['id'] == int(campaign_id)][0]

    if scheduled_at:
        # from est to utc
        scheduled_at = arrow.get(scheduled_at).replace(tzinfo=tz.gettz(app.config['APP_TIMEZONE'])).to('UTC')

    else:
        # utc
        scheduled_at = current_time()

    email_campaign = {"content": [{"type": "text/html", "value": email_content}], "from": FROM_SENDER,
                      "reply_to": REPLY_TO, "subject": email_subject}

    email_campaign = json.dumps(email_campaign)

    pending_digest = (str(current_user.id) + '_' + email_campaign + '_' + scheduled_at.format('YYYYMMDD')).encode(
        'utf-8')
    message_key = hashlib.md5(pending_digest).hexdigest()
    push = db.session.query(PromotionPush).filter_by(message_key=message_key).first()

    if push is None:

        push = PromotionPush(
            admin_user_id=current_user.id,
            based_query_id=based_query_id,
            based_query_rules=query_rules_sql,
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

            query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()
            stmt = sqlparse.parse(query.sql)[0]
            tokens = [str(item) for item in stmt.tokens]
            tokens[2] = 'user_id, username, reg_country, email'
            slim_sql = ''.join(tokens)

            result_proxy = db.engine.execute(text(slim_sql))
            column_names = [col[0] for col in result_proxy.cursor.description]

            if 'email' in column_names:
                data = result_proxy.fetchall()
                df = pd.DataFrame(data, columns=column_names)
                data = [{'user_id': row['user_id'], 'username': row['username'], 'country': row['reg_country'],
                         'email': row['email']} for _, row in df.iterrows() if
                        (row['user_id'] is not None and row['email'] is not None)]

                if app.config['ENV'] == 'prod':

                    process_promotion_email_notification_items.delay(push_id, scheduled_at.format(), data=data)

                else:

                    process_promotion_email_notification_items(push_id, scheduled_at.format(), data=data)

                return jsonify(result='ok'), 202

            else:

                return jsonify(error="Based query don't have column: user_id, email"), 500

        elif query_rules is not None:

            if app.config['ENV'] == 'prod':

                process_promotion_email_notification_items.delay(push_id, scheduled_at.format(),
                                                                 query_rules=query_rules)

            else:

                process_promotion_email_notification_items(push_id, scheduled_at.format(), query_rules=query_rules)

            return jsonify(result='ok'), 202

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

    email_campaign = {"content": [{"type": "text/html", "value": email_content}], "from": FROM_SENDER,
                      "reply_to": REPLY_TO, "subject": "TESTING: " + email_subject}

    personalizations = []
    for recipient in TEST_EMAIL_RECIPIENTS:
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
        custom_fields = re.findall(pattern, email_content)
        custom_fields_format = ['[%' + (field.split('%')[1]).split(' ')[0] + '%]' for field in custom_fields]
        for i in range(0, len(custom_fields)):
            email_content = email_content.replace(custom_fields[i], custom_fields_format[i])

        email_content = email_content. \
            replace("[Unsubscribe]", '<%asm_group_unsubscribe_raw_url%>'). \
            replace("[weblink]", "https://www.playwpt.com"). \
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

        return jsonify(error=error_msg_from_exception(e)), 500

    else:

        return jsonify(result='ok'), 202
