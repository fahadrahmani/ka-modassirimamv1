import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm


def update_df_columns(rep_df, act_list, start_date, end_date, db_conn):

    cur = db_conn.cursor()

    # get complete activity dataframe
    orig_act_df = pd.read_sql('SELECT * FROM Activities', db_conn)
    orig_act_df['DATE'] = pd.to_datetime(orig_act_df['date'])

    # for each id/student/row in dataframe
    st_ids = orig_act_df['student_id'].unique()
    for student_id in tqdm(st_ids, total=len(st_ids)):

        new_appended = False
        # add a new row in rep_df if the student_id doesn't exists previously
        rep_stud_ids = rep_df['id'].values
        if student_id not in rep_stud_ids :
            # rep_df =rep_df.append({'id': student_id}, ignore_index = True)
            rep_df.loc[len(rep_df), 'id'] = student_id
            # print('Appended', student_id)
            new_appended = True

        filt = (rep_df['id'] == student_id)

        # get dataframe specific for current student
        stud_act_df = orig_act_df.loc[orig_act_df['student_id'] == student_id]

        # adding teacher's id
        tid = cur.execute("SELECT teacher_id FROM Progress WHERE id=?",(student_id,)).fetchone()[0]
        rep_df.loc[filt, 'teacher_id'] = tid

        # adding delta
        delta = cur.execute("SELECT delta FROM Progress WHERE id=?",(student_id,)).fetchone()[0]
        rep_df.loc[filt, 'delta'] = delta

        if new_appended:
            rep_df.loc[filt, 'time'] = sum(stud_act_df.time)
            # adding delta
            email = cur.execute("SELECT email FROM Progress WHERE id=?",(student_id,)).fetchone()[0]
            rep_df.loc[filt, 'email'] = email

        '''PART_1 : Updating Activity passed columns'''

        start = datetime.strptime(start_date, "%B %d, %Y")
        end = datetime.strptime(end_date, "%B %d, %Y")
        mask = (stud_act_df['DATE'] >= start) & (stud_act_df['DATE'] <= end + timedelta(days=1))
        act_df = stud_act_df.loc[mask]

        # for each activity from input activity list
        for act in act_list:
            df=act_df.loc[act_df['activity'] == act]
            # passed be no. of times activity passed
            passed = 0
            if df.empty :
                rep_df.loc[filt, act] = passed
                continue

            df.reset_index(drop=True,inplace=True)
            len_df = len(df)
            # for each time activity is done
            for index in range(len_df):
                cor_tot_prob = str(df.loc[index,'correct/total_problems']).split('/')
                cor_prob = None
                tot_prob = None
                if len(cor_tot_prob) > 1:
                    try:
                        cor_prob = int(cor_tot_prob[0])
                        tot_prob = int(cor_tot_prob[1])
                        # if correct problems >= 27 activity passsed
                        if cor_prob >= 27: passed=passed+1
                    except:
                        pass
            rep_df.loc[filt, act] = passed

        min_times = rep_df.loc[filt, act_list].min(axis=1)
        if min_times.values[0] >= 3:
            rep_df.loc[filt, ['1_time_or_more', '2_time_or_more', '3_time_or_more']] = True
        elif min_times.values[0] >= 2:
            rep_df.loc[filt, ['1_time_or_more', '2_time_or_more']] = True
        elif min_times.values[0] >= 1:
            rep_df.loc[filt, '1_time_or_more'] = True
        else:
            pass

        rep_df.loc[filt, '0_times'] = (rep_df.loc[filt, act_list] == 0).astype(int).sum(axis=1)

        ####################
        # Yesterday's Data #
        ####################

        endY = datetime.strptime(end_date, "%B %d, %Y") - timedelta(days=1)
        maskY = (stud_act_df['DATE'] >= endY) & (stud_act_df['DATE'] <= endY + timedelta(days=1))
        act_df_Y = stud_act_df.loc[maskY]

        # for each activity from input activity list
        for act in act_list:
            act_Y = act+'__Y'
            df_Y=act_df_Y.loc[act_df_Y['activity'] == act]
            # passed be no. of times activity passed
            passed_Y = 0
            if df_Y.empty :
                rep_df.loc[filt, act_Y] = passed_Y
                continue

            df_Y.reset_index(drop=True,inplace=True)
            len_df_Y = len(df_Y)
            # for each time activity is done
            for index in range(len_df_Y):
                cor_tot_prob_Y = str(df_Y.loc[index,'correct/total_problems']).split('/')
                cor_prob_Y = None
                tot_prob_Y = None
                if len(cor_tot_prob_Y) > 1:
                    try:
                        cor_prob_Y = int(cor_tot_prob_Y[0])
                        tot_prob_Y = int(cor_tot_prob_Y[1])
                        # if correct problems >= 27 activity passsed
                        if cor_prob_Y >= 27: passed_Y=passed_Y+1
                    except:
                        pass
            rep_df.loc[filt, act_Y] = passed_Y

        '''PART_2 : Updating the sum of activity in dates columns'''

        date_list = []
        for i in range(7):
            date = end - timedelta(days=i)
            mask = (stud_act_df['DATE'] >= date) & (stud_act_df['DATE'] <= date + timedelta(days=1))
            time_sum = stud_act_df.loc[mask, 'time'].sum()
            rep_df.loc[filt, datetime.strftime(date,"%B %d, %Y")] = time_sum
            date_list.append(datetime.strftime(date,"%B %d, %Y"))
        avg_time_sum = rep_df.loc[filt, date_list].mean(axis=1)
        rep_df.loc[filt, 'avg_7_days'] = avg_time_sum.values[0]

    # Droping students with no activity in database
    rep_df.drop(index = rep_df.loc[~rep_df.id.isin(st_ids)].index, inplace=True)
    rep_df.reset_index(drop=True,inplace=True)

    return rep_df



def get_report(db_conn, start_date, end_date = "",
               act_list=[f'Course challengeClass {i} math (India)' for i in range(1, 11)],
               df=pd.DataFrame(), use_db=False):
    columns = ['id', 'email']
    columns.extend(act_list)
    columns.extend(['teacher_id', 'time', 'time_rank', 'delta', 'skills', 'skills_rank', 'top_20_time', 'top_20_skills'])
    columns.extend(['1_time_or_more', '2_time_or_more', '3_time_or_more', '0_times'])
    if end_date == "":
        end_date = datetime.now().strftime('%B %d, %Y')
    date = datetime.strptime(end_date, "%B %d, %Y")
    columns.extend([(date - timedelta(days=i)).strftime("%B %d, %Y") for i in range(7)])
    columns.extend(['avg_7_days'])
    columns.extend([act+'__Y' for act in act_list])

    rep_df = pd.DataFrame(columns=columns)

    # If input df is given
    if not df.empty:
        rep_df['id'] = df['Username / Email'].str.split('@', expand=True)[0]
        rep_df['email'] = df['Username / Email']
        rep_df['time'] = df['Total learning minutes']
        rep_df['skills'] = df['Skills leveled up']
        rep_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

        rep_df['skills_rank'] = rep_df['skills'].rank(method='dense', ascending=False).astype(int)
        rep_df['top_20_skills'] = rep_df['skills_rank'].apply(lambda x: True if x <= 20 else False)

    elif use_db:
        db_df = pd.read_sql('SELECT id, email, total_mins_learned, skills_leveled_up FROM Progress', db_conn)
        rep_df['id'] = db_df['id']
        rep_df['email'] = db_df['email']
        rep_df['time'] = db_df['total_mins_learned']
        rep_df['skills'] = db_df['skills_leveled_up']
        rep_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

        rep_df['skills_rank'] = rep_df['skills'].rank(method='dense', ascending=False).astype(int)
        rep_df['top_20_skills'] = rep_df['skills_rank'].apply(lambda x: True if x <= 20 else False)


    rep_df.reset_index(drop=True, inplace=True)

    rep_df = update_df_columns(rep_df,act_list, start_date, end_date, db_conn)

    rep_df['time_rank'] = rep_df['time'].rank(method='dense', ascending=False).astype(int)
    rep_df['top_20_time'] = rep_df['time_rank'].apply(lambda x: True if x <= 20 else False)

    rep_df['1_time_or_more'].fillna(False, inplace=True)
    rep_df['2_time_or_more'].fillna(False, inplace=True)
    rep_df['3_time_or_more'].fillna(False, inplace=True)
    rep_df['0_times'].fillna(False, inplace=True)

    rep_df.fillna(0,inplace=True)

    rep_df['top_20_time'] = rep_df['top_20_time'].astype(bool)
    rep_df['top_20_skills'] = rep_df['top_20_skills'].astype(bool)

    return rep_df



def get_report_csv(rep_df):
    date = datetime.now().strftime('%a_%b_%d_%Y')
    rep_df.drop(columns=['id'], inplace=True)
    rep_df.to_csv(f'students_report_{date}.csv', index=False)
