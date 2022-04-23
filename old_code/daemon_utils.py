import os, time
# from collect_data import main as collect
# from recollect_data import main as recollect
from pathlib import Path
from utils import *
from config import *
# from daemons.prefab import run
# from python_daemon.daemon import Daemon
import traceback

class Args(object):
    def __init__(self, start_date, end_date, delta, datefree=False, email_id=None):
        self.start_date = start_date
        self.end_date = end_date
        self.delta = delta
        self.datefree = datefree
        self.email_id = email_id


class LoggerForDaemon(object):
    def __init__(self, fname='autocollect_logs.txt'):
        p = Path('./')
        self.logfile = p/"autocollect.logs"
        if not os.path.exists(self.logfile):
            print(f"Creating log file at: {self.logfile}")
            with open(self.logfile, 'w') as f:
                f.write(f'{get_today()} FILE CREATED\n')
    
    def log(self, message):

        if not message.endswith('\n'):
            message = message.strip() + '\n'
        else: 
            message = message.strip()

        with open(self.logfile, 'a') as f:
            f.write(message)
        
        return

