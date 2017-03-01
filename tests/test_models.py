#! ../env/bin/python
# -*- coding: utf-8 -*-

import pytest

from app.extensions import db
from app.models.main import AdminUser

create_user = False


@pytest.mark.usefixtures("testapp")
class TestModels:
    def test_user_save(self, testapp):
        """ Test Saving the user model to the database """

        admin = AdminUser(email='admin@admin.com', password='supersafepassword')
        db.session.add(admin)
        db.session.commit()

        user = AdminUser.query.filter_by(email="admin@admin.com").first()
        assert user is not None

    def test_user_password(self, testapp):
        """ Test password hashing and checking """

        admin = AdminUser(email='admin@admin.com', password='supersafepassword')

        assert admin.email == 'admin@admin.com'
        assert admin.check_password('supersafepassword')
