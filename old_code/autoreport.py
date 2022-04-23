from create_report import create_report
from database.db_utils import get_db_conn
import time
import os
from utils import get_args, get_dates

def main(args):
    args.save = False
    args.loop = True
    dates = get_dates(8)
    while True:
        print(f'Getting report at {dates[0]}.......')
        r = create_report(args)
        r.to_csv(f'data/REPORT {dates[0]}.csv', index=None)

        if os.path.exists(f'data/REPORT {dates[-1]}.csv'):
            os.remove(f'data/REPORT {dates[-1]}.csv')

        time.sleep(86000)

if __name__ == "__main__":
    args = get_args()
    main(args)