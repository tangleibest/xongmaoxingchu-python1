from datetime import datetime, date, timedelta

yesterday = date.today() + timedelta(days = -1)
print(yesterday)