from database.report import get_report
from create_report import autoreport
from utils import *
from config import *
from collect_data import collect_data
from database.db_utils import *
import datetime



def run():

    conn = get_db_conn()

    report_saved = None
    logger = LoggerForDaemon()
    
    normal = try_except_log(logger, 'NORMAL')(collect_data)
    soi_ = try_except_log(logger, 'SOI')(collect_data)

    print('Logger created!')
    logging.basicConfig(filename='logs/collection_log.logs', filemode='w',
                    level=logging.INFO, format='%(message)s')

    while True:
        
        start_date, end_date = get_dates(og_format=True)
        args = Args(start_date=start_date, end_date=end_date, delta=30)
        args.conn = conn

        progress_tables = []
        for i in range(TOTAL_EMAIL_IDS):
            args.email_id = i + 1
            logger.log(f'\n Date and Time: {get_time_str()}, ID {args.email_id}')

            args.start_date, args.end_date = get_dates(og_format=True) 
            # args.datefree = True
            _ = normal(args) 

            args.start_date = START
            # args.datefree = False 
            progress_tables.append(soi_(args, redo=True, up_delta=True))

        progress_df = pd.concat(progress_tables)

        if report_saved != get_today():
            dates = get_dates(8)
            r = get_report(args.conn, start_date=START, end_date=dates[-1], df=progress_df)

            r.to_csv(f'data/REPORT {dates[0]}.csv', index=None)
            if os.path.exists(f'data/REPORT {dates[-1]}.csv'):
                os.remove(f'data/REPORT {dates[-1]}.csv')
            report_saved = get_today()

if __name__ == '__main__':
    run()
