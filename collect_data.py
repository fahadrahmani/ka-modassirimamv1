from scipy.stats.mstats_basic import argstoarray
from utils import *
from database.db_utils import *
from config import *


def collect_data(args, redo=False, up_delta=False, only_main=False):

    if not args.start_date == START:
        up_delta = False
        
    if not hasattr(args, 'conn'):
        conn = get_db_conn()
        args.conn = conn 

    combined, dates, browser = get_main_table(args)

    # combined.to_csv(f'data/progress_{args.email_id}.csv', index=False)
    if not only_main:
        get_data(browser, combined, args, soi=redo)

    if only_main:
        update_progress(combined, args.email_id, args.conn)

    if up_delta:
        update_progress(combined, args.email_id, args.conn)
        update_delta(args.conn, dates[0], dates[1])

    kill_browser()
    return combined


def get_main_table(args):
    USERNAME, PASSWORD = get_email_from_id(args.email_id)

    browser = login(USERNAME, PASSWORD, visibility='hidden')

    combined, dates = get_progress_table(browser, args)
    return combined, dates, browser


def collect_assignment(args):

    if not hasattr(args, 'conn'):
        conn = get_db_conn('assignments.db')
        args.conn = conn 

    combined, _, browser = get_main_table(args)

    args.assignment = True 
    args.activity = False

    get_data(browser, combined, args)

    kill_browser()


if __name__ == "__main__":
    arg = get_args()
    

    if arg.loop:
        for i in range(1, TOTAL_EMAIL_IDS+1):
            arg.email_id = i
            collect_data(arg, up_delta=False, only_main=arg.only_main)
            
        if arg.update_delta:
            start_date, end_date = convert_dates(arg)
            update_delta(get_db_conn(), start_date, end_date)

    else:
        collect_data(arg, redo=arg.redo, up_delta=arg.update_delta, only_main=arg.only_main)
            
