from flask import current_app as app
from flask import render_template
from flask_mail import Message

from app.extensions import mail
from app.tasks import celery


@celery.task
def send_mail(to, subject, template, attachment=None, attachment_content_type=None, **kwargs):
    msg = Message(app.config['MAIL_SUBJECT_PREFIX'] + subject)
    if isinstance(to, list):
        msg.recipients = to
    else:
        msg.add_recipient(to)
    msg.html = render_template('mail/' + template + '.html', **kwargs)
    if attachment is not None and attachment_content_type is not None:
        with app.open_resource(attachment) as fp:
            msg.attach(kwargs['filename'], attachment_content_type, fp.read())
    mail.send(msg)
    return None
