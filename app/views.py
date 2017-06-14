from flask import url_for, redirect, request, abort, flash
from flask_admin import expose, AdminIndexView
from flask_admin.babel import gettext
from flask_admin.contrib.sqla import ModelView
from flask_admin.contrib.sqla.filters import BaseSQLAFilter, FilterEqual
from flask_admin.helpers import get_redirect_target
from flask_admin.model.helpers import get_mdict_item_or_list
from flask_login import current_user
from jinja2 import Markup
from wtforms import TextField, PasswordField, SelectField
from wtforms import validators

from app.constants import ADMIN_USER_ROLES
from app.models.bi import BIUser, BIUserCurrency, BIUserBill, BIUserBillDetail
from app.models.orig_wpt import WPTUserLoginLog


class FilterInStringList(BaseSQLAFilter):
    def __init__(self, column, name, options=None, data_type=None):
        super(FilterInStringList, self).__init__(column, name, options, data_type)

    def clean(self, value):
        return [v.strip() for v in value.split(',') if v.strip()]

    def apply(self, query, value, alias=None):
        return query.filter(self.get_column(alias).in_(value))

    def operation(self):
        return 'in list'


class AdminBaseIndexView(AdminIndexView):

    def is_accessible(self):
        return False

    def inaccessible_callback(self, name, **kwargs):
        return abort(404)


class AdminBaseModelView(ModelView):

    page_size = 50

    can_create = False
    can_edit = False
    can_delete = False
    can_export = False
    can_view_details = False

    def is_accessible(self):
        return current_user.is_authenticated and current_user.is_active

    def inaccessible_callback(self, name, **kwargs):
        return redirect(url_for('account.sign_in', next=request.url))

    # def scaffold_filters(self, name):
    #     filters = super().scaffold_filters(name)
    #     for f in filters:
    #         f.name = 'Filter by ' + name
    #     return filters

    def details_view_with_extra_func(self, extra_func):
        """
            Details model view
        """
        return_url = get_redirect_target() or self.get_url('.index_view')

        if not self.can_view_details:
            return redirect(return_url)

        if self.__class__.__name__ == 'AdminBIUserModelView':
            id = get_mdict_item_or_list(request.args, 'id')
            user_id = get_mdict_item_or_list(request.args, 'user_id')

            if id is not None:
                model = self.get_one(id)
            elif user_id is not None:
                model = self.session.query(BIUser).filter_by(user_id=user_id).one()
            else:
                return redirect(return_url)
        else:
            id = get_mdict_item_or_list(request.args, 'id')
            if id is None:
                return redirect(return_url)
            model = self.get_one(id)

        if model is None:
            flash(gettext('Record does not exist.'), 'error')
            return redirect(return_url)

        return extra_func(model, return_url)


class AdminUserModelView(AdminBaseModelView):
    can_delete = False
    can_view_details = False

    def is_accessible(self):
        permission = current_user.has_role(ADMIN_USER_ROLES.ROOT.value)
        self.can_create = permission
        self.can_edit = permission

        return super(AdminBaseModelView, self).is_accessible()

    def on_model_change(self, form, model, is_created):
        if is_created:
            model.password = form.required_password.data
        else:
            pwd = form.optional_password.data
            if bool(pwd and pwd.strip()):
                model.password = pwd

    column_list = ['id', 'name', 'email', 'active', 'sign_in_count', 'created_at']
    column_searchable_list = ['id', 'name', 'email']
    column_sortable_list = ['id', 'sign_in_count']

    # form_columns = ['name', 'email', 'password', 'role', 'active']
    form_create_rules = ('name', 'email', 'required_password', 'role', 'active')
    form_edit_rules = ('name', 'email', 'optional_password', 'role', 'active')

    form_extra_fields = {
        'email': TextField('Email', validators=[validators.DataRequired(), validators.Email()]),
        'required_password': PasswordField('Password', [validators.DataRequired(), validators.Length(min=8)]),
        'optional_password': PasswordField('Password', [validators.Optional(), validators.Length(min=8)]),
        'role': SelectField(label='Role', choices=[('manager', 'Manager'), ('admin', 'Admin'), ('root', 'Super Admin')])
    }

class AdminBIUserModelView(AdminBaseModelView):
    can_view_details = True
    can_export = True

    column_default_sort = ('user_id', True)

    def dollar_paid_amount_formatter(v, c, m, p):
        if m.dollar_paid_amount and m.dollar_paid_amount > 0:
            return Markup("""
                          %s 
                          <br />
                          <a href="/data/bi_user_bill/?flt1_0=%s&flt2_1=Dollar" target="_blank"><i class="fa fa-money"></i></a>
                          &nbsp; 
                          <a href="/data/bi_user_currency/?flt1_0=%s&flt3_3=15111201,15115300,925011309" target="_blank"><i class="fa fa-gamepad"></i></a>
                          """ % (m.dollar_paid_amount, m.user_id, m.user_id))
        return None

    column_formatters = dict(dollar_paid_amount=dollar_paid_amount_formatter)

    column_list = ['user_id', 'email', 'username', 'account_status', 'dollar_paid_amount', 'email_validate_time', 'reg_time', 'last_poker_time', 'gold_balance', 'last_slots_time', 'silver_balance']
    column_searchable_list = ['user_id', 'og_account', 'email', 'username']
    column_sortable_list = ['user_id', 'dollar_paid_amount', 'email_validate_time', 'reg_time', 'last_poker_time', 'gold_balance', 'last_slots_time', 'silver_balance']

    @expose('/details/', methods=['GET'])
    def details_view(self):
        """ Details view """

        def extra_func(model, return_url):
            """ Extra function for details view """

            return self.render('admin/custom/bi_user_details.html',
                               model=model,
                               return_url=return_url,
                               attribute_pairs=model.attribute_pairs(),
                               login_logs=model.login_logs(5))

        return super(AdminBIUserModelView, self).details_view_with_extra_func(extra_func)


class AdminBIUserCurrencyModelView(AdminBaseModelView):
    def transaction_type_formatter(v, c, m, p):
        return m.transaction_type_display()

    def user_id_formatter(v, c, m, p):
        return Markup('<a href="/data/bi_user/details/?user_id=%s" target="_blank">%s</a>' % (m.user_id, m.user_id))

    column_formatters = dict(transaction_type=transaction_type_formatter, user_id=user_id_formatter)

    column_default_sort = ('created_at', True)
    column_list = ['user_id', 'og_account', 'currency_type', 'transaction_type', 'transaction_amount', 'balance', 'created_at']
    column_sortable_list = ['created_at']

    column_filters = [
        FilterEqual(column=BIUserCurrency.user_id, name='User Id'),
        FilterEqual(column=BIUserCurrency.currency_type, name='Currency Type', options=(('Gold', 'Gold'), ('Silver', 'Silver'))),
        FilterEqual(column=BIUserCurrency.transaction_type, name='Transaction Type'),
        FilterInStringList(column=BIUserCurrency.transaction_type, name='Transaction Type')
    ]


class AdminBIUserBillModelView(AdminBaseModelView):
    def user_id_formatter(v, c, m, p):
        return Markup('<a href="/data/bi_user/details/?user_id=%s" target="_blank">%s</a>' % (m.user_id, m.user_id))

    def products_formatter(v, c, m, p):
        return Markup('<br />'.join(['%s: %s' % (row.category, row.goods)for row in m.bill_detail_products()]))

    column_formatters = dict(user_id=user_id_formatter, products=products_formatter)

    column_default_sort = ('created_at', True)
    column_list = ['user_id', 'platform', 'currency_type', 'currency_amount', 'goods', 'products', 'quantity', 'created_at']
    column_sortable_list = ['currency_amount', 'quantity', 'created_at']

    currency_type_options = (
        ('Gold', 'Gold'),
        ('MasterPoints', 'MasterPoints'),
        ('Dollar', 'Dollar'),
        ('Silver Coins', 'Silver Coins')
    )

    platform_options = (
        ('Web', 'Web'),
        ('iOS', 'iOS'),
        ('Android', 'Android'),
        ('Facebook Game', 'Facebook Game'),
        ('Unknown', 'Unknown')
    )

    column_filters = [
        FilterEqual(column=BIUserBill.user_id, name='User Id'),
        FilterEqual(column=BIUserBill.currency_type, name='Paid Currency', options=currency_type_options),
        FilterEqual(column=BIUserBill.platform, name='Platform', options=platform_options),
    ]


class AdminBIUserBillDetailModelView(AdminBaseModelView):
    def product_formatter(v, c, m, p):
        return '%s: %s' % (m.category, m.goods)

    def user_id_formatter(v, c, m, p):
        return Markup('<a href="/data/bi_user/details/?user_id=%s" target="_blank">%s</a>' % (m.user_id, m.user_id))

    column_formatters = dict(product=product_formatter, user_id=user_id_formatter)

    column_default_sort = ('created_at', True)
    column_list = ['user_id', 'platform', 'currency_type', 'currency_amount', 'product', 'quantity', 'created_at']
    column_sortable_list = ['currency_amount', 'quantity', 'created_at']

    currency_type_options = (
        ('Gold', 'Gold'),
        ('MasterPoints', 'MasterPoints'),
        ('Dollar', 'Dollar'),
        ('Silver Coins', 'Silver Coins')
    )

    category_options = (
        ('Gold', 'Gold'),
        ('Silver Coins', 'Silver Coins'),
        ('Lucky Spin Set', 'Lucky Spin Set'),
        ('Poker Lucky Charm Set', 'Poker Lucky Charm Set'),
        ('Avatar Set', 'Avatar Set'),
        ('Poker Ticket Set', 'Poker Ticket Set'),
        ('Emoji Set', 'Emoji Set')
    )

    platform_options = (
        ('Web', 'Web'),
        ('iOS', 'iOS'),
        ('Android', 'Android'),
        ('Facebook Game', 'Facebook Game'),
        ('Unknown', 'Unknown')
    )

    column_filters = [
        FilterEqual(column=BIUserBillDetail.user_id, name='User Id'),
        FilterEqual(column=BIUserBillDetail.currency_type, name='Paid Currency', options=currency_type_options),
        FilterEqual(column=BIUserBillDetail.category, name='Product Category', options=category_options),
        FilterEqual(column=BIUserBillDetail.platform, name='Platform', options=platform_options),
    ]

    def is_visible(self):
        return False


class AdminBIClubWPTUserModelView(AdminBaseModelView):
    def exchanged_user_id_formatter(v, c, m, p):
        return Markup('<a href="/data/bi_user/details/?user_id=%s" target="_blank">%s</a>' % (m.exchanged_user_id, m.exchanged_user_id))

    column_formatters = dict(exchanged_user_id=exchanged_user_id_formatter)

    column_default_sort = ('exchanged_at', True)
    column_list = ['email', 'username', 'gold_balance', 'exchanged_at', 'exchanged_user_id']
    column_sortable_list = ['exchanged_at']


class AdminWPTUserLoginLogModelView(AdminBaseModelView):
    column_default_sort = ('id', True)

    column_filters = [
        FilterEqual(column=WPTUserLoginLog.user_id, name='User Id')
    ]

    def is_visible(self):
        return False
