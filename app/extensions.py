from flask_cache import Cache
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_gravatar import Gravatar
from flask_mail import Mail
from sendgrid import SendGridAPIClient


import geoip2.database
import os


cache = Cache()
migrate = Migrate()
db = SQLAlchemy()
debug_toolbar = DebugToolbarExtension()
login = LoginManager()
gravatar = Gravatar()
mail = Mail()
geoip_reader = geoip2.database.Reader(os.path.dirname(__file__) + '/data/GeoIP2-City.mmdb')
sendgrid = SendGridAPIClient(apikey='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')
# sendgrid = SendGridAPIClient(apikey='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')

def __del__(self):
    geoip_reader.close()
