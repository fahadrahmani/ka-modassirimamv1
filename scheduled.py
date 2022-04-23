import schedule 
from main import *
import time
from utils import try_except_log, LoggerForDaemon


logger = LoggerForDaemon('scheduled.logs')

#decorated functions
data_collection = try_except_log(logger, 'data collection')(data_collection)
assignment_collection = try_except_log(logger, 'assignment collection')(assignment_collection)


#adding datacollection schedule
schedule.every().day.at(convert_time_zones('01:00')).do(data_collection)
print(f'{data_collection.__name__} is scheduled at 01:00 hrs IST')

schedule.every().day.at(convert_time_zones('13:00')).do(assignment_collection)
print(f'{assignment_collection.__name__} is scheduled at 13:00 hrs IST')
#adding assignmentcollection schedule
# for h in list(map(lambda x: str(x).zfill(2), range(13, 20, 20))):
#     schedule.every().day.at(convert_time_zones(f'{h}:00')).do(assignment_collection)
#     print(f'{assignment_collection.__name__} is scheduled at {h} hrs IST')

# schedule.every().hour.do(assignment_collection)

while True:
    schedule.run_pending()
    time.sleep(1)
