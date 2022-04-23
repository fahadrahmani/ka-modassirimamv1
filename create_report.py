from utils import *
from database.db_utils import *
from collect_data import get_main_table
from config import *
from database.report import get_report, get_report_csv
import os

def create_report(args):

    if not hasattr(args, 'conn'):
        conn = get_db_conn()
        args.conn = conn 
       

    if args.start_date is None:
        _, END = get_dates()
        dates = ['March 2, 2020', END]

        args.start_date = START
        _, args.end_date = get_dates(og_format=True)
    else:
        start_date, end_date = convert_dates(args)
        dates = [start_date, end_date]


#     if args.loop:
#         df_list = []
#         for i in range(TOTAL_EMAIL_IDS):
#             args.email_id = i +1 
#             try:
#                 combined, dates, browser = get_main_table(args)
#                 df_list.append(combined)
#                 kill_browser()
#             except: 
#                 combined, dates, browser = get_main_table(args)
#                 df_list.append(combined)
#                 kill_browser()
#             finally:
#                 pass
        
#         progress_df = pd.concat(df_list)
#     else:
#         progress_df = pd.DataFrame()
    
    
    progress_df = pd.DataFrame()
    

    activity_list = [f'Course challengeClass {i} math (India)' for i in range(1, 11)]
    
    print('Getting report.......')
    r = get_report(args.conn, start_date=dates[0], end_date=dates[1], act_list = activity_list, df=progress_df)

    if not hasattr(args, 'save'):
        args.save = True 
    
    if args.save:
        save_report(r, args.conn, f'REPORT {dates[0]} - {dates[1]}.csv')
    else:
        return r

def autoreport(args):
    args.save = False
    args.loop = True
    dates = get_dates(8)
    print(f'Getting report at {dates[0]}.......')
    r = create_report(args)
    save_report(r, args.conn, f'data/REPORT {dates[1]}.csv')

    if os.path.exists(f'data/REPORT {dates[0]}.csv'):
        os.remove(f'data/REPORT {dates[0]}.csv')


if __name__ == "__main__":
    args = get_args()
    create_report(args)
