from utils import *
from config import *
from collect_data import collect_assignment, collect_data
from database.db_utils import *
from database.report import get_report

WAIT_HOURS = 4

def save_assignment_report(args):
    dates = get_dates(8)
    assignments = get_pd_table('assignments', args.conn)
    save_assign(assignments, args.conn, f'data/ASSIGNMENT {dates[1]}.csv')
    if os.path.exists(f'data/ASSIGNMENT {dates[0]}.csv'):
        os.remove(f'data/ASSIGNMENT {dates[0]}.csv')

def data_collection():
    conn = get_db_conn()
    logger = LoggerForDaemon()

    normal = try_except_log(logger, 'NORMAL')(collect_data)
    soi_ = try_except_log(logger, 'SOI')(collect_data)

    print('Logger created!')
    logging.basicConfig(filename='logs/collection_log.logs', filemode='w',
                    level=logging.INFO, format='%(message)s')

    start_date, end_date = get_dates(og_format=True)
    args = Args(start_date=start_date, end_date=end_date, delta=30)
    args.conn = conn

    progress_tables = []
    for i in range(TOTAL_EMAIL_IDS):
        args.email_id = i + 1
        logger.log(f'\n Date and Time: {get_time_str()}, ID {args.email_id} - DATA COLLECTION')
        args.start_date, args.end_date = get_dates(og_format=True) 
        _ = normal(args) 
        args.start_date = START
        progress_tables.append(soi_(args, redo=True, up_delta=True))

    try:
        progress_df = pd.concat(progress_tables)
    except:
        progress_df = pd.DataFrame()

    dates = get_dates(8)
    r = get_report(args.conn, start_date='March 2, 2020', end_date=dates[-1], df=progress_df)

    # r = get_report(args.conn, 'March 2, 2020')
    # save_report(r, args.conn, f'data/REPORT {dates[-1]}.csv')

    if 'id' in list(r.columns):
        r.drop('id', axis=1, inplace=True)
    r.to_csv(f'data/REPORT {dates[-1]}.csv')

    if os.path.exists(f'data/REPORT {dates[0]}.csv'):
        os.remove(f'data/REPORT {dates[0]}.csv')

    kill_chrome()
        

def assignment_collection():
    conn = get_db_conn('assignments.db')
    logger = LoggerForDaemon()

    logging.basicConfig(filename='logs/assignment_logs.logs', filemode='w',
                        level=logging.INFO, format='%(message)s')
    
    start_date, end_date = get_dates(og_format=True)
    args = Args(start_date=start_date, end_date=end_date, delta=30, datefree=True)
    args.conn = conn

    a = try_except_log(logger, 'NORMAL')(collect_assignment)
    for i in range(TOTAL_EMAIL_IDS):
        args.email_id = i + 1
        logger.log(f'\n Date and Time: {get_time_str()}, ID {args.email_id} - Assignment Collections')
        a(args)
            
    update_asgn_track(conn, get_time_str())
    save_assignment_report(args)

    kill_chrome()


