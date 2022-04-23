import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from numpy import unicode
from config import TOTAL_EMAIL_IDS


def get_db_conn(filename='rahmanimission.db'):
    try:
        conn = sqlite3.connect(f'file:{filename}?mode=rw', uri=True)
        print(f'Database exists with name: "{filename}"')
    except sqlite3.Error as e:
        print(e)
        #print('Database file dosen\'t exists.')
        conn = sqlite3.connect(filename)
        print('New database created named: "rahmanimission"')
        conn.cursor().executescript('''
            CREATE TABLE IF NOT EXISTS track_log
            (last_1 TEXT,last_2 TEXT,last_3 TEXT,last_4 TEXT,last_5 TEXT,last_6 TEXT,last_7 TEXT);

            INSERT INTO track_log (last_1,last_2,last_3,last_4,last_5,last_6,last_7) 
            VALUES ("last_1","last_2","last_3","last_4","last_5","last_6","last_7");
        ''')

    conn.cursor().executescript('''
        CREATE TABLE IF NOT EXISTS Progress
        (id TEXT PRIMARY KEY, email TEXT ,name TEXT ,total_mins_learned INTEGER, skills_leveled_up INTEGER,
        [skill_w/o_progress] INTEGER, teacher_id TEXT, links TEXT, delta TEXT);
        
        CREATE TABLE IF NOT EXISTS Activities
        (student_id Text, activity TEXT, teacher_id TEXT, date TEXT, level TEXT, change TEXT, time NUMERIC,
        correct_problems NUMERIC, total_problems NUMERIC,[correct/total_problems] TEXT);
        
        CREATE TABLE IF NOT EXISTS Assignments
        (assignment Text, student_id TEXT, teacher_id TEXT, due_date TEXT, status TEXT, attempts INTEGER, best_score FLOAT,
        last_1 NUMERIC,last_2 NUMERIC,last_3 NUMERIC,last_4 NUMERIC,last_5 NUMERIC,last_6 NUMERIC,last_7 NUMERIC);
    ''')
    return conn


def update_progress(df, teacher_id, db_conn):
    print('Updating Progress...')
    db = db_conn.cursor()
    query = '''INSERT OR REPLACE INTO Progress (id, email, name, total_mins_learned, skills_leveled_up,
                        [skill_w/o_progress], teacher_id, links, delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    for index in range(len(df)):
        db.execute(query, ((df.loc[index, 'Username / Email']).split('@')[0], df.loc[index, 'Username / Email'], df.loc[index, 'Student'], int(df.loc[index, 'Total learning minutes']), int(df.loc[index, 'Skills leveled up']), int(df.loc[index, 'Skills w/o progress']), teacher_id, df.loc[index, 'links'], df.loc[index, 'delta']))
        # try:
        #     db.execute(query, ((df.loc[index, 'Username / Email']).split('@')[0], df.loc[index, 'Username / Email'], df.loc[index, 'Student'], int(df.loc[index, 'Total learning minutes']), int(df.loc[index, 'Skills leveled up']), int(df.loc[index, 'Skills w/o progress']), teacher_id, df.loc[index, 'links'], df.loc[index, 'delta']))
        # except:
        #     db.execute(query, ((df.loc[index, 'Username / Email']).split('@')[0], df.loc[index, 'Username / Email'], df.loc[index, 'Student'], 0, 0, 0, teacher_id, df.loc[index, 'links'], df.loc[index, 'delta']))
    db_conn.commit()
    print('Progress Updated.\n')


# noinspection PyBroadException
def update_activity(student_id, df, teacher_id, db_conn, clear_prev=False):
    df.to_csv('temp.csv', index=None)
    df = pd.read_csv('temp.csv')
    # print('Updating Activity of', student_id,'...')

    db_cur = db_conn.cursor()
    insert_sql = '''INSERT INTO Activities (student_id,activity, teacher_id, date,
        level, change, time, correct_problems, total_problems,[correct/total_problems])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    delete_sql = '''DELETE FROM Activities WHERE student_id = ? AND activity = ?'''
    delete_duplicate = '''DELETE FROM Activities WHERE rowid NOT IN (SELECT MAX(rowid) FROM Activities GROUP BY student_id, activity, date)'''

    len_df = len(df)
    for index in range(len_df):
        if clear_prev is True:
            db_cur.execute(delete_sql,(student_id, df.loc[index, 'Activity']))

        cor_tot_prob = unicode(df.loc[index, 'Correct/Total Problems'])
        cor_prob = None
        tot_prob = None

        time_mins = 0
        try:
            time_mins = int(df.loc[index, 'Time (min)'])
        except:
            pass

        db_cur.execute(insert_sql,
                       (student_id, df.loc[index, 'Activity'], teacher_id, df.loc[index, 'Date'], df.loc[index, 'Level'],
                        df.loc[index, 'Change'], time_mins,cor_prob,
                        tot_prob, cor_tot_prob))

    db_cur.execute(delete_duplicate)

    db_conn.commit()
    # print('Activity Updated of', student_id)


def update_assignment(student_id, df, teacher_id, db_conn):
    # print('Updating Assignments of', student_id,'...')
    db_cur = db_conn.cursor()
    insert_sql = '''INSERT INTO Assignments (assignment, student_id, teacher_id, due_date, status, attempts, best_score)
                VALUES (?, ?, ?, ?, ?, ?, ?)'''
    delete_sql = '''DELETE FROM Assignments WHERE student_id = ? AND assignment = ?'''
    
    db_cur.execute('SELECT * FROM track_log')
    t = db_cur.fetchone()
    query_track = f'''SELECT [{t[0]}], [{t[1]}], [{t[2]}], [{t[3]}], [{t[4]}], [{t[5]}], [{t[6]}] FROM 
                Assignments WHERE student_id = ? AND assignment = ?'''
    update_track = f'''UPDATE Assignments SET [{t[0]}]=?, [{t[1]}]=?, [{t[2]}]=?, [{t[3]}]=?, [{t[4]}]=?, [{t[5]}]=?, [{t[6]}]=? 
                    WHERE student_id = ? AND assignment = ?'''

    len_df = len(df)
    for index in range(len_df):
        db_cur.execute(query_track, (student_id, df.loc[index, 'Assignment']))
        row = db_cur.fetchone()
        db_cur.execute(delete_sql, (student_id, df.loc[index, 'Assignment']))
        attempts = 0
        try:
            attempts = int(df.loc[index, 'Attempts'])
        except:
            pass
        db_cur.execute(insert_sql, (df.loc[index, 'Assignment'], student_id, teacher_id, df.loc[index, 'Due date & time'],
                                    df.loc[index, 'Status'], attempts, df.loc[index, 'Best Score']))
        if row is not None:
            db_cur.execute(update_track, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], 
                                          student_id, df.loc[index, 'Assignment']))

    db_conn.commit()
    # print('Assignments updated of', student_id)


def update_asgn_track(db_conn, track_time=""):
    if track_time == "":
        track_time = datetime.now().strftime('%b %d,%Y at %H:%M')
    db_cur = db_conn.cursor()
    db_cur.execute('SELECT * FROM track_log')
    t = db_cur.fetchone()
    
    if track_time in t:
        print('Aborting Track Update !!!')
        print('You have already updated data for time',track_time)
        print('Try after atleast 60 secs or with different column name...')
        return
    
    db_cur.executescript(f'''UPDATE Assignments SET [{t[6]}] = [{t[5]}];
                            UPDATE Assignments SET [{t[5]}] = [{t[4]}];
                            UPDATE Assignments SET [{t[4]}] = [{t[3]}];
                            UPDATE Assignments SET [{t[3]}] = [{t[2]}];
                            UPDATE Assignments SET [{t[2]}] = [{t[1]}];
                            UPDATE Assignments SET [{t[1]}] = [{t[0]}];
                            UPDATE Assignments SET [{t[0]}] = attempts;''')
    
    db_cur.executescript(f'''ALTER TABLE Assignments RENAME COLUMN [{t[0]}] TO [{track_time}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[1]}] TO [{t[0]}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[2]}] TO [{t[1]}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[3]}] TO [{t[2]}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[4]}] TO [{t[3]}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[5]}] TO [{t[4]}];
                            ALTER TABLE Assignments RENAME COLUMN [{t[6]}] TO [{t[5]}];''')
    db_cur.execute('UPDATE track_log SET last_1= ?, last_2= ?, last_3= ?, last_4= ?, last_5= ?, last_6= ?, last_7= ?',
                   (track_time, t[0], t[1], t[2], t[3], t[4], t[5]))
    db_conn.commit()


def get_activity(db_conn, student_id="", start_date="", end_date="", teacher_id=""):
    if student_id != "" and teacher_id != "":
        act_df = pd.read_sql(f'SELECT * FROM Activities WHERE student_id = "{student_id}" AND teacher_id = "{teacher_id}"', db_conn)
    elif teacher_id != "":
        act_df = pd.read_sql(f'SELECT * FROM Activities WHERE teacher_id = "{teacher_id}"', db_conn)
    elif student_id != "":
        act_df = pd.read_sql(f'SELECT * FROM Activities WHERE student_id = "{student_id}"', db_conn)
    else:
        act_df = pd.read_sql('SELECT * FROM Activities', db_conn)


    act_df['DATE'] = pd.to_datetime(act_df['date'])
    act_df.sort_values(by=['DATE'], inplace=True, ascending=False)
    if start_date != "" and end_date != "":
        start_date = datetime.strptime(start_date, "%B %d, %Y")
        end_date = datetime.strptime(end_date, "%B %d, %Y") + timedelta(days=1)
        mask = (act_df['DATE'] >= start_date) & (act_df['DATE'] <= end_date)
        return act_df.loc[mask]
    elif start_date != "":
        start_date = datetime.strptime(start_date, "%B %d, %Y")
        mask = (act_df['DATE'] > start_date)
        return act_df.loc[mask]
    elif end_date != "":
        end_date = datetime.strptime(end_date, "%B %d, %Y") + timedelta(days=1)
        mask = (act_df['DATE'] < end_date)
        return act_df.loc[mask]
    else:
        return act_df


def update_delta(db_conn, start_date="", end_date=""):
    print('Updating Delta...')
    db_cur = db_conn.cursor()
    prog_df = pd.read_sql('SELECT id, total_mins_learned FROM Progress', db_conn)
    for row in prog_df.itertuples():
        act_df = get_activity(db_conn, student_id=row.id, start_date=start_date, end_date=end_date)
        if act_df.empty:
            db_cur.execute('UPDATE Progress SET delta = "Activity_not_available" WHERE id = ?', (row.id,))
        else:
            sum_time = sum(t for t in act_df.time)
            delta = row.total_mins_learned - sum_time
            db_cur.execute('UPDATE Progress SET delta = ? WHERE id = ?', (delta, row.id))
    db_conn.commit()
    print('Delta updated')


def get_student_of_interest(min_delta, db_conn, teacher_id, non_available=True, positive_delta=False):

    
    assert isinstance(min_delta,(int, float)), "min_delta must be integer or float type."
    list_soi = []
    db_cur = db_conn.cursor()
    db_cur.execute(f'''SELECT id, name, links, delta FROM Progress WHERE delta != "Activity_not_available" 
                    AND  teacher_id = "{teacher_id}"''')
    results = db_cur.fetchall()
    for id, name, link, delta in results:

        try:
            if positive_delta: 
                if int(delta) >= min_delta : list_soi.append((id, name, link))
            else:
                if abs(int(delta)) >= min_delta : list_soi.append((id, name, link))
        except:
            pass

    if non_available:
        db_cur.execute(f'''SELECT id, name, links, delta FROM Progress WHERE delta = "Activity_not_available" 
                                AND teacher_id = "{teacher_id}"''')
        results = db_cur.fetchall()
        for id, name, link, delta in results:
            try:
                list_soi.append((id, name, link))
            except:
                pass

    return list_soi



def get_pd_table(table_name, db_conn, student_id=""):
    
    if student_id != "":
        if table_name.upper() =='PROGRESS':
            df = pd.read_sql(f'SELECT * FROM {table_name} WHERE id = "{student_id}"', db_conn)
        else:
            df = pd.read_sql(f'SELECT * FROM {table_name} WHERE student_id = "{student_id}"', db_conn)
    else:
        df = pd.read_sql(f'SELECT * FROM {table_name}', db_conn)
        
    if table_name.upper() =='ASSIGNMENTS':
        db_cur = db_conn.cursor()
        db_cur.execute('SELECT * FROM track_log')
        t = db_cur.fetchone()
        dfcpy = df[[col for col in t]]
        for i in range(6):
            if dfcpy[t[i+1]].dtype == type(None):
                df[t[i]] = dfcpy[t[i]]
                break
            df[t[i]] = dfcpy[t[i]] - dfcpy[t[i+1]]
        asum =  df[[col for col in t]].sum(axis=1)
        assert (asum == df.attempts).astype(int).sum() == len(df), "Error: Sum of past attempts in not equal to total attempts" 
#         df.fillna(0, inplace=True)
    return df


def link_to_id(link, db_conn):
    db_cur = db_conn.cursor()
    db_cur.execute('SELECT id FROM Progress WHERE link = ?',(link,))
    id = db_cur.fetchone()[0]
    return id


def get_summary(args):
    db_conn = args.conn
    summary = {}
    db_cur = db_conn.cursor()

    db_cur.execute('SELECT COUNT(id) FROM Progress')
    n_students = db_cur.fetchone()[0]
    summary['total_students'] = n_students

    db_cur.execute('select COUNT(*) from Activities')
    summary['Rows in activities table'] = db_cur.fetchone()[0]

    db_cur.execute('select COUNT(*) from Assignments')
    summary['Rows in assignments table'] = db_cur.fetchone()[0]

    db_cur.execute('select teacher_id, COUNT(DISTINCT student_id) from Activities GROUP BY teacher_id')
    summary['st_per_teacher'] = db_cur.fetchall()

    db_cur.execute('SELECT COUNT(id) from Progress WHERE delta = "Activity_not_available"')
    summary['null_activity_students'] = db_cur.fetchone()[0]

    sois = [(str(i+1), len(get_student_of_interest(30, db_conn, i+1, False, positive_delta=args.positive_delta))) for i in range(TOTAL_EMAIL_IDS)] 
    summary['Delta: High delta students'] = sois

    total_hd = sum([x[1] for x in sois])
    summary['Delta: Total number of high delta students'] = total_hd
    summary['Delta: Percentage of high delta students'] = round(total_hd * 100 / n_students, 2)


    return summary
