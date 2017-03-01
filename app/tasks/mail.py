from flask import render_template
from flask import current_app as app
from app.tasks import celery
from app.extensions import mail

from flask_mail import Message

@celery.task
def send_mail(to, subject, template, attachment=None, attachment_content_type=None, **kwargs):
    msg = Message(app.config['MAIL_SUBJECT_PREFIX'] + subject)
    msg.add_recipient(to)
    msg.html = render_template('mail/' + template + '.html', **kwargs)
    if attachment is not None and attachment_content_type is not None:
        with app.open_resource(attachment) as fp:
            msg.attach(kwargs['filename'], attachment_content_type, fp.read())
    mail.send(msg)
    return None
