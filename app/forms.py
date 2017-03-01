from flask_wtf import FlaskForm
from wtforms import TextField, PasswordField
from wtforms import validators

from app.models.main import AdminUser


class LoginForm(FlaskForm):
    email = TextField('Email', validators=[validators.required()])
    password = PasswordField('Password', validators=[validators.required()])

    def validate(self):
        check_validate = super(LoginForm, self).validate()

        # if our validators do not pass
        if not check_validate:
            return False

        # Does our the exist
        user = AdminUser.query.filter_by(email=self.email.data).first()
        if not user:
            self.email.errors.append('Invalid email or password')
            return False

        if not user.active:
            self.email.errors.append('Invalid email or password')
            return False

        # Do the passwords match
        if not user.check_password(self.password.data):
            self.email.errors.append('Invalid email or password')
            return False

        return True
