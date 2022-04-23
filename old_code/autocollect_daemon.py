import sys, os
from pathlib import Path

from daemon_utils import AutoDataCollecter


if __name__ == '__main__':

    p = Path('./')
    action = sys.argv[1]
    pidfile = p/"autocollect.pid"
    # with open(pidfile, 'w') as f:
    #     print("Able to open the file")
    print(f"Creating pid file at: {pidfile}")
    d = AutoDataCollecter(pidfile=pidfile)

    if action == "start":

        d.start()

    elif action == "stop":

        d.stop()

    elif action == "restart":

        d.restart()