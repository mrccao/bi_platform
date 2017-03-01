import os

from app import create_app

env = os.environ.get('WPT_DWH_ENV', 'prod')
app = create_app('app.settings.%sConfig' % env.capitalize())

if __name__ == '__main__':
    app.run()
