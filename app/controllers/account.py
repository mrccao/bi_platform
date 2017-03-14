from flask import Blueprint, render_template, flash, request, redirect, url_for
from flask_login import login_user, logout_user, login_required, current_user

from app.forms import LoginForm
from app.models.main import AdminUser

account = Blueprint('account', __name__)


@account.route("/sign_in", methods=["GET", "POST"])
def sign_in():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.index"))

    form = LoginForm()
    if request.method == 'POST':
        user = AdminUser.query.filter_by(email=form.email.data).first()
        if form.validate_on_submit():
            login_user(user)
            user.track_sign_in_success(request.headers.get('True-Client-IP'))

            flash("Logged in successfully.", "success")

            next = request.args.get('next')

            # TODO: check next is valid
            # if next_is_valid(next):
            #     return abort(400)

            return redirect(next or url_for("dashboard.index"))
        else:
            if user:
                user.track_sign_in_failure()

    return render_template("account/sign_in.html", form=form)


@account.route("/sign_out", methods=["GET", "DELETE"])
@login_required
def sign_out():
    logout_user()
    flash("You have been logged out.", "success")

    return redirect(url_for("account.sign_in"))
