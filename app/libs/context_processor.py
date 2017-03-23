from app.utils import current_time, str_to_class


def global_processor():
    def display_current_time(timezone=None, format='YYYY-MM-DD HH:mm:ss'):
        return current_time(timezone).format(format)

    def get_columns_of_table(module_name, class_name):
        return [column.key for column in str_to_class(module_name, class_name).__table__.columns]

    return dict(display_current_time=display_current_time, get_columns_of_table=get_columns_of_table)
