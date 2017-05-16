from flask_assets import Bundle, Environment

common_css = Bundle(
    '../assets/css/adminlte.css',
    filters='cssmin',
    output='static/css/common.css'
)

common_js = Bundle(
    '../assets/js/adminlte.js',
    filters='jsmin',
    output='static/js/common.js'
)

flask_admin_css = Bundle(
    '../assets/css/admin_filters.css',
    filters='cssmin',
    output='static/css/flask_admin.css'
)

flask_admin_js = Bundle(
    '../assets/js/admin_filters.js',
    '../assets/js/admin_form.js',
    filters='jsmin',
    output='static/js/flask_admin.js'
)

flask_AmericaGeojson_js = Bundle(
    '../assets/js/AmericaGeojson.js',
    filters='jsmin',
    output='static/js/flask_AmericaGeojson.js'
)


flask_WorldGeojson_js = Bundle(
    '../assets/js/WorldGeojson.js',
    filters='jsmin',
    output='static/js/flask_WorldGeojson.js'
)

assets = Environment()

assets.register('common_css', common_css)
assets.register('common_js', common_js)
assets.register('flask_admin_css', flask_admin_css)
assets.register('flask_admin_js', flask_admin_js)
assets.register('flask_AmericaGeojson_js', flask_AmericaGeojson_js)
assets.register('flask_WorldGeojson_js', flask_WorldGeojson_js)
