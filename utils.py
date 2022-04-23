import argparse
import logging
import os
import time
import traceback
import functools 
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from helium import *
from selenium.webdriver.chrome.options import Options

from config import *
from database.db_utils import *
import datetime
from pytz import timezone
from dateutil import *
from dateutil.tz import *

FILLER = '*' * 30

def convert_time_zones(t):
    
    utc_zone = tz.gettz('UTC')
    local_zone = tz.gettz('Asia/Kolkata')
    local_time = datetime.datetime.strptime(t, '%H:%M')

    local_time = local_time.replace(tzinfo=local_zone)
    utc_time = local_time.astimezone(utc_zone)
    return utc_time.strftime('%H:%M')



def get_time_str():
    now_utc = datetime.datetime.now(timezone('UTC'))
    now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))
    return now_asia.strftime("%d/%m/%Y %H:%M:%S")


def lprint(string, f=False):
    if f is True or f == 'f':
        # fprint(string, filler=True if f == 'f' else False)
        logging.info(f'\n{FILLER if f == "f" else ""} ~o~ {string} ~o~ {FILLER if f == "f" else ""}\n')
    else:
        # print(string)
        logging.info(string)


def fprint(string, filler=True):
    print(f'\n{FILLER if filler else ""} ~o~ {string} ~o~ {FILLER if filler else ""}\n')


def get_args(desc="No Desc"):
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-st', '--start_date',
                        help='Start date for date in format "Month date, year" for example "April_25_2020"',
                        default=None)
    parser.add_argument('-ed', '--end_date',
                        help='End date for date in format "Month date, year" for example "April_25_2020"',
                        default=None)
    parser.add_argument('-id', '--email_id', type=int,
                        help='Select the teacher id', default=1)
    parser.add_argument('-d', '--delta',
                        help='max delta limit', type=int, default=30)
    parser.add_argument('-r', '--redo', action='store_true', help='do you want to redo for sois',
                        default=False)
    parser.add_argument('-ud', '--update_delta', action='store_true', help='do you want to update_delta',
                        default=False)
    parser.add_argument('-m', '--only_main', action='store_true', help='do you want use only main',
                        default=False)
    parser.add_argument('-l', '--loop', action='store_true', help='loop?',
                        default=False)
    parser.add_argument('-pd', '--positive_delta', action='store_true', help='do you want only positive delta?',
                        default=False)  
    args = parser.parse_args()
    return args


def get_soup(elem):
    return BeautifulSoup(elem.get_attribute('innerHTML'), 'html.parser')


def login(username, password, visibility='hidden'):
    if visibility == 'hidden':
        chrome_options = Options()
        user_agent = 'I LIKE CHOCOLATE'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--headless")
        browser = start_chrome(options=chrome_options, headless=True)
    else:
        browser = start_chrome()
        browser.maximize_window()

    go_to("https://www.khanacademy.org/login")
    write(username, into='Email or username')
    write(password, into='Password')
    click('Log in')
    return browser

def convert_dates(args):
    mos = ['January', 'February', 'March', 'April', 'May', 'June',
           'July', 'August', 'September', 'October', 'November', 'December']
    st = args.start_date.split('_')
    ed = args.end_date.split('_')
    assert st[0] in mos
    assert ed[0] in mos
    start_date = f'{st[0]} {st[1]}, {st[2]}'
    end_date = f'{ed[0]} {ed[1]}, {ed[2]}'
    return start_date, end_date

def input_date(args, browser):
    wait_until(Text('Last 7 days').exists, timeout_secs=TIMEOUT)
    click('Last 7 days')
    start_date, end_date = convert_dates(args)

    wait_until(Text('Custom range').exists, timeout_secs=TIMEOUT)
    click('Custom range')
    empty = ''.join([f'\b' for _ in range(50)])

    wait_until(Text('From').exists, timeout_secs=TIMEOUT)
    wait_until(Text('To').exists, timeout_secs=TIMEOUT)
    
    write(empty + start_date,
            into=browser.find_elements_by_tag_name('input')[-2])
    write(empty + end_date,
            into=browser.find_elements_by_tag_name('input')[-1])
    click('Confirm')
    return [start_date, end_date]


# def create_folder(username, start_date=None, end_date=None, datefree=False):
#     root = Path(username)
#     if start_date is None:
#         folder = Path(f'{username}/{datetime.datetime.today().strftime("%Y-%m-%d")}')
#     else:
#         folder = Path(f'{username}/{start_date}-{end_date}')

#     if datefree:
#         folder = Path(f'{username}')

#     os.makedirs(str(root), exist_ok=True)
#     os.makedirs(str(folder), exist_ok=True)

#     os.makedirs(str(folder / 'logs'), exist_ok=True)
#     return folder


def get_link(series):
    links = []
    for text in series:
        link = Link(text)
        if not link.exists():
            links.append(None)
        else:
            links.append(link.href)
    assert len(links) == len(series)
    return pd.Series(links)


def recursivetable(browser, wait_element, escape_element=None, table_no=0, link=None):
    if escape_element is not None:
        if Text(escape_element).exists():
            lprint('blank activity')
            return None

    try:
        wait_until(Text(wait_element).exists, timeout_secs=TIMEOUT)
    except Exception as e:
        lprint(e)
        return None

    n = Button('Next')
    if n.exists() and n.is_enabled():
        table_list = []
        while n.is_enabled():
            wait_until(Text(wait_element).exists, timeout_secs=TIMEOUT)
            try:
                table = pd.read_html(browser.page_source)[table_no]
                if link is not None:
                    series = table.iloc[:, link]
                    table['links'] = get_link(series)
            except:
                time.sleep(3)
                table = pd.read_html(browser.page_source)[table_no]
                if link is not None:
                    series = table.iloc[:, link]
                    table['links'] = get_link(series)
            table_list.append(table)
            click(n)
            n = Button('Next')

        final_table = pd.read_html(browser.page_source)[table_no]
        if link is not None:
            series = final_table.iloc[:, link]
            final_table['links'] = get_link(series)

        table_list.append(final_table)
        table = pd.concat(table_list)
    else:
        table = pd.read_html(browser.page_source)[table_no]
        if link is not None:
            series = table.iloc[:, link]
            table['links'] = get_link(series)
    return table


def get_student_data(browser, student_id, args):

    if not hasattr(args, 'activity'): 
        activity = True
    else: 
        activity = args.activity

    if not hasattr(args, 'assignment'): 
        assignment = True
    else:
        assignment = args.assignment 
        

    if activity:
        try:
            click(find_all(Text('Activity log'))[0])
            st_activity = recursivetable(browser, 'CORRECT/TOTAL PROBLEMS', 'No results')

        except Exception as e:
            lprint(traceback.format_exc())
            lprint(f'exception found at activity - get_student_data {e}')
            lprint(e)
            lprint(f'{student_id} activity not found', True)
            st_activity = None
    else:
        lprint('Skipping Activity')
        st_activity = None

    if assignment:
        try:
            wait_until(Text('Assignments').exists, timeout_secs=TIMEOUT)
            b = find_all(Text('Assignments'))
            if len(b) == 1:
                time.sleep(5)
                b = find_all(Text('Assignments'))
            bf = b[1] if b[1].x > b[0].x else b[0]
            click(bf)
            load = Button('Load More')
            if load.exists() and load.is_enabled():
                click(load)
            else:
                pass
            as_activity = recursivetable(browser, 'DUE DATE & TIME')
        except Exception as e:
            lprint(traceback.format_exc())
            lprint(f'exception found at assignment - get_student_data {e}')
            lprint(f'{student_id} assignment not found', True)
            as_activity = None
    else:
        print('Skipping assignment')
        as_activity = None

    return st_activity, as_activity


def goto_student(name):
    b = Link('Activity overview')
    click(b)
    wait_until(Text('TOTAL LEARNING MINUTES').exists, timeout_secs=TIMEOUT)
    click(Text(name))


def get_email_from_id(id):
    USERNAME = f'r30-2020prejoining{str(id).zfill(2)}@rahmanimission.org'
    PASSWORD = 'abc123***'
    return USERNAME, PASSWORD


def get_sois(df, delta, lengths=False):
    df = df.fillna('Activity_not_available')
    sois = list(df[df.delta == 'Activity_not_available'].index)
    sois.extend(list(df[df.delta == None].index))
    sep = len(sois)
    new_df = df.drop(sois)
    new_df['delta'] = new_df.delta.apply(int)
    sois.extend(list(new_df[new_df.delta > delta].index))
    sois.extend(list(new_df[new_df.delta < -1 * delta].index))
    if lengths:
        return len(sois) - sep, sep
    else:
        return df.iloc[sois].Student


def isint(string):
    try:
        int(string)
        flag = True
    except:
        flag = False
    return flag


def get_progress_table(browser, args):
    class_name = f"R30-2020prejoining-{str(args.email_id).zfill(2)}: Multiple courses"
    print(class_name)
    wait_until(Text(class_name).exists, timeout_secs=TIMEOUT)
    click(class_name)

    try:
        wait_until(Button('Update').exists, timeout_secs=20)
        click(Button('Update'))
    except:
        pass
    
    wait_until(Text('USERNAME / EMAIL').exists, timeout_secs=TIMEOUT)
    student = recursivetable(browser, 'USERNAME / EMAIL', table_no=1)
    student['Student'] = student['Student name']
    student.drop(['Student name', 'Unnamed: 2', 'Unnamed: 3'], inplace=True, axis=1)

    wait_until(Text('Activity overview').exists, timeout_secs=TIMEOUT)
    click('Activity overview')

    if not hasattr(args, 'datefree'): args.datefree = False
    
    if not args.datefree:
        dates = input_date(args, browser)
    else:
        print('Going datefree')
        dates = convert_dates(args)

    wait_until(Text('TOTAL LEARNING MINUTES').exists, timeout_secs=TIMEOUT)
    og_table = recursivetable(browser, 'TOTAL LEARNING MINUTES', link=0)
    wait_until(Text('Activity overview').exists, timeout_secs=TIMEOUT)
    click('Activity overview')

    combined = pd.concat([student.reset_index(drop=True), og_table.reset_index(drop=True)], axis=1)
    combined = combined.loc[:, ~combined.columns.duplicated()]

    combined['delta'] = None
    return combined, dates


# noinspection PyTypeChecker
def get_data(browser, og_table, args, soi=False):

    if soi:
        sois = get_student_of_interest(args.delta, args.conn, args.email_id)
        soi_ids = [student_id for student_id, _, _ in sois]


    for student_id, l, name in tqdm(zip(og_table.loc[:, 'Username / Email'], og_table.links, og_table.Student), 
                            total=len(og_table), desc=f'collecting for {args.email_id}'):
        student_id = student_id.split('@')[0]


        if soi:
            if student_id not in soi_ids:
                continue

        try:
            if l is not None:
                go_to(l)
            else: 
                goto_student(name)
        except:
            lprint(f'cannot go to student records for {student_id}')

        lprint(student_id, 'f')

        go_to(l)
        wait_until(Text('Activity Log').exists, timeout_secs=TIMEOUT) and wait_until(Text('Assignments').exists, timeout_secs=TIMEOUT)

        lprint('getting data .....')
        st_activity, as_activity = get_student_data(browser, student_id, args)
        update_db(student_id, st_activity, as_activity, args)



def get_today():
    return '{dt:%B}_{dt.day}_{dt.year}'.format(dt=datetime.datetime.now())


def update_db(student_id, st_activity, as_activity, args):
    if st_activity is not None:
        update_activity(student_id, st_activity, args.email_id, args.conn)
    else:
        lprint(f'Activity for {student_id} not present')

    if as_activity is not None:
        update_assignment(student_id, as_activity, args.email_id, args.conn)
    else:
        lprint(f'Assignment for student {student_id} not present')


def get_dates(n_days=3, og_format=False):
    tod = datetime.datetime.now()
    start_date = datetime.timedelta(days=n_days)
    start_date = tod - start_date
    if not og_format:
        return '{dt:%B} {dt.day}, {dt.year}'.format(dt=start_date), '{dt:%B} {dt.day}, {dt.year}'.format(dt=tod)
    else:
        return '{dt:%B}_{dt.day}_{dt.year}'.format(dt=start_date), '{dt:%B}_{dt.day}_{dt.year}'.format(dt=tod)


def kill_chrome():
    # experimental: yet to be tested
    os.system(f'pkill chrome')


class LoggerForDaemon(object):
    def __init__(self, fname='autocollect_logs.logs'):
        p = Path('logs')
        self.logfile = p/fname
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
            print(message)
            f.write(message)
        
        return

class Args(object):
    def __init__(self, start_date, end_date, delta, datefree=False, email_id=None):
        self.start_date = start_date
        self.end_date = end_date
        self.delta = delta
        self.datefree = datefree
        self.email_id = email_id



def get_dates_list(n_days=7):
    _, tod = get_dates()
    end = datetime.datetime.strptime(tod, "%B %d, %Y")
    date_list = [datetime.datetime.strftime(end - datetime.timedelta(days=i),"%B %d, %Y") 
                    for i in range(n_days)]
    return date_list


class try_except_log(object):
    def __init__(self, logger, message):
        self.logger = logger
        self.message = message

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*arguments, **kwargs):
            today = get_time_str()
            try:
                self.logger.log(f'\n {today}  STARTING {self.message} COLLECTION at {get_time_str()}')
                r = func(*arguments, **kwargs)
                self.logger.log(f'\n {today} SUCCESS {self.message} COLLECTION at {get_time_str()}')
                return r
            except Exception as e:
                tb = traceback.format_exc()
                self.logger.log(f'\n {today} FAILED {self.message} COLLECTION {e} {tb}')
        return wrapper


def save_report(rep, conn, name):
    progress_table = pd.read_sql('select * from progress', conn)
    main_ids, sup_ids = set(rep.id), set(progress_table.id)
    diff = sup_ids.difference(main_ids)
    index = [i for i in range(len(progress_table)) if progress_table.id[i] in diff]
    df = progress_table.drop(index)
    df = df[['email', 'id']]
    report = pd.merge(df, rep, on='id')
    report.to_csv(name, index=None)


def save_assign(assign, conn, name):
    progress_table = pd.read_sql('select * from progress', conn)
    main_ids, sup_ids = set(assign.student_id), set(progress_table.id)
    diff = sup_ids.difference(main_ids)
    index = [i for i in range(len(progress_table)) if progress_table.id[i] in diff]
    df = progress_table.drop(index)
    df = df[['email', 'id']]
    df.index = df.id
    df.drop('id', axis=1)
    dictionary = df.to_dict()
    for id in main_ids: 
        assign.loc[assign.student_id == id, 'email'] = dictionary['email'][id]
    assign.to_csv(name, index=None)

