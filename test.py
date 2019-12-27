import calendar
import time,datetime

# this_month_mk = time.mktime(datetime.date.today().timetuple())
#
# this_time=int(time.time())
# up_time=int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month, 30).timetuple()))
# print(up_time)
# print(up_time-this_time)

# up_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).timetuple()))
# to_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month +1, 1).timetuple()))
# print(up_time)
# print(to_time)

englist=calendar.month_abbr[12]
print(englist)
li=list(calendar.month_abbr).index('Dec')
print(li)