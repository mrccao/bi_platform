from flask import Blueprint

report = Blueprint('report', __name__)

from .daily_summary import daily_summary
from .new_users import new_users
from .user_region import user_region
