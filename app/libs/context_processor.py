from app.utils import current_time

def global_processor():
    def display_current_time(timezone=None, format='YYYY-MM-DD HH:mm:ss'):
        return current_time(timezone).format(format)
    return dict(display_current_time=display_current_time)
