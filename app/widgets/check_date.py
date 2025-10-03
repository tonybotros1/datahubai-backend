from datetime import datetime


def is_date_equals_today_or_older(date: datetime):
    today = datetime.today()
    return date.date() <= today.date()
