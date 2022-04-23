from utils import *
from config import *
from collect_data import collect_data
from database.db_utils import *
# from daemon_utils import Args


def run(redo_at=7):
    count = 0

    conn = get_db_conn()

    while True:
        if count == redo_at:
            redo = True
            count = 0
        else:
            redo = False
            count += 1

        redo = True

        logger = LoggerForDaemon()
        print('Logger created!')
        logging.basicConfig(filename='collection_log.logs', filemode='w',
                            level=logging.INFO, format='%(message)s')
        

        today = get_today()
        start_date, end_date = get_dates(og_format=True)

        for i in range(TOTAL_EMAIL_IDS):

            args = Args(start_date=start_date, end_date=end_date, delta=30)
            args.email_id = i + 1
            lprint(f'Date and Time: {get_time_str()}, ID {args.email_id}')
            
            if len(get_activity(conn, start_date="March 2, 2020", end_date="May 2, 2020", 
                                teacher_id=args.email_id)) < 20:
                args.start_date = START
                logger.log('First time!!! ')

            
            try:
                logger.log(f'{today} {args.email_id} STARTING COLLECTION')
                collect_data(args)
                logger.log(f'{today} {args.email_id} SUCCESS COLLECTION')
            except Exception as e:
                tb = traceback.format_exc()
                logger.log(f'{today} {args.email_id} FAILED COLLECTION {e} {tb}')

            try:
                logger.log(f'{today} {args.email_id} STARTING Global time COLLECTION')
                args.start_date = START 
                collect_data(args, only_main=True, up_delta=True, redo=False)
                logger.log(f'{today} {args.email_id} SUCCESS Global time COLLECTION')
            except Exception as e:
                tb = traceback.format_exc()
                logger.log(f'{today} {args.email_id} FAILED Global time COLLECTION {e} {tb}')
            
            if redo:
                try:
                    logger.log(f'{today} {args.email_id} STARTING REDO COLLECTION')
                    args.start_date = START
                    collect_data(args, up_delta=True, redo=True)
                    logger.log(f'{today} {args.email_id} SUCCESS REDO COLLECTION')
                except Exception as e:
                    tb = traceback.format_exc()
                    logger.log(f'{today} {args.email_id} FAILED REDO COLLECTION {e} {tb}')
            lprint(f'Done for ID {args.email_id}')

            kill_chrome()

        logger.log(f'Waiting for {DELAY} s')        
        for s in tqdm(range(DELAY), total=DELAY, desc='WAITING'):
            time.sleep(1)
        


if __name__ == '__main__':
    run()