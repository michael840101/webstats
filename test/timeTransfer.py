from datetime import timedelta,datetime
strs = "2012-05-12T13:04:35.347-07:00"
str="2017-01-17T12:05:19Z"
#replace the last ':' with an empty string, as python UTC offset format is +HHMM
# strs = strs[::-1].replace(':','',1)[::-1]
# try:
#     offset = int(strs[-5:])
# except:
#     print "Error"

#delta = timedelta(hours = offset / 100)

time = datetime.strptime(str, "%Y-%m-%dT%H:%M:%SZ")
#time -= delta                #reduce the delta from this time object
