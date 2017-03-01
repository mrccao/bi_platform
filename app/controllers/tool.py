from flask import Blueprint, render_template
from flask_login import login_required

tool = Blueprint('tool', __name__)

@tool.route("/tool/facebook_notification", methods=["GET"])
@login_required
def facebook_notification():
    return render_template('tool/facebook_notification.html')
