from flask import Blueprint, render_template
from flask_login import login_required

page = Blueprint('page', __name__)

@page.route("/changelog", methods=["GET"])
@login_required
def changelog():
    return render_template('page/changelog.html')
