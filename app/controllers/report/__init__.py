from flask import Blueprint

report = Blueprint('report', __name__)



from .daily_summary import  daily_summary
from .reg_analysis import  reg_platform
