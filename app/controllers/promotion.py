import hashlib
import sqlparse
import arrow
import pandas as pd
from random import randrange

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text

from app.extensions import db
from app.models.main import AdminUserQuery
from app.models.promotion import PromotionPush, PromotionPushHistory
from app.models.orig_wpt import WPTPlatformUser
from app.utils import error_msg_from_exception, current_time
from app.constants import PROMOTION_PUSH_HISTORY_STATUSES, PROMOTION_PUSH_TYPES
from app.tasks.promotion import process_facebook_notification


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
    queries = db.session.query(PromotionPush).order_by(PromotionPush.created_at.desc()).limit(10)
    return jsonify(data=[i.to_dict() for i in queries.all()])


@promotion.route("/promotion/facebook_notification/retry", methods=["POST"])
@login_required
def facebook_notification_retry():
    push_id = request.form.get('push_id')
    if push_id:
        process_facebook_notification(push_id)
    return jsonify(result='ok')


@promotion.route("/promotion/facebook_notification/sender", methods=["POST"])
@login_required
def facebook_notification_sender():
    based_query_id = request.form.get('based_query_id')
    message = request.form.get('message')

    scheduled_at = request.form.get('scheduled_at')
    if scheduled_at:
        scheduled_at = arrow.get(scheduled_at).replace(hours=+5) # from est to utc
    else:
        scheduled_at = current_time() # utc

    formatted_message = message.strip()
    pending_digest = (str(current_user.id) + '_' + formatted_message).encode('utf-8')
    message_key = hashlib.md5(pending_digest).hexdigest()

    push = db.session.query(PromotionPush).filter_by(message_key=message_key).first()
    if push is None:
        push = PromotionPush(
            admin_user_id=current_user.id,
            based_query_id=based_query_id,
            push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value,
            message=formatted_message,
            message_key=message_key
        )

        db.session.add(push)
        db.session.commit()
    else:
        return jsonify(error="You have been sent this message before, Please change message if you don't need to send the same message."), 500

    push_id = push.id

    try:
        if based_query_id is not None:
            # based some query
            query = db.session.query(AdminUserQuery).filter_by(id=based_query_id).first()

            stmt = sqlparse.parse(query.sql)[0]
            tokens = [str(item) for item in stmt.tokens]
            tokens[2] = 'facebook_id'
            slim_sql = ''.join(tokens)

            result_proxy = db.engine.execute(text(slim_sql))
            column_names = [col[0] for col in result_proxy.cursor.description]

            if 'facebook_id' in column_names:
                data = result_proxy.fetchall()

                df = pd.DataFrame(data, columns=column_names)
                facebook_ids = df[df['facebook_id'].notnull()]['facebook_id'].tolist()

                for facebook_id in facebook_ids:
                    max_minutes = int(len(facebook_ids) / 1000) + 1
                    scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format('YYYY-MM-DD HH:mm:ss')
                    db.session.add(PromotionPushHistory(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, target=facebook_id, scheduled_at=scheduled_time, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value))
                db.session.commit()

                return jsonify(result='ok')
            else:
                return jsonify(error="based query don't have column: facebook_id"), 500
        else:
            facebook_ids = db.session.query(WPTPlatformUser.platform_user_id).all()
            for row in facebook_ids:
                max_minutes = int(len(facebook_ids) / 1000) + 1
                scheduled_time = scheduled_at.replace(minutes=+(randrange(0, max_minutes))).format('YYYY-MM-DD HH:mm:ss')
                db.session.add(PromotionPushHistory(push_id=push_id, push_type=PROMOTION_PUSH_TYPES.FB_NOTIFICATION.value, target=row[0], scheduled_at=scheduled_time, status=PROMOTION_PUSH_HISTORY_STATUSES.SCHEDULED.value))
            db.session.commit()

            return jsonify(result='ok')
    except Exception as e:
        return jsonify(error=error_msg_from_exception(e)), 500
