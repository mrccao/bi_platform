from flask_cache import Cache
from flask_debugtoolbar import DebugToolbarExtension
from flask_login import LoginManager
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_gravatar import Gravatar
from flask_mail import Mail

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


def __del__(self):
    geoip_reader.close()
