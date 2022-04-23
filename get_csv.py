from database.db_utils import * 

conn = get_db_conn()

get_pd_table('progress', conn).to_csv('data/Progress_all_students.csv', index=None)

get_pd_table('activities', conn).to_csv('data/activities.csv', index=None)

get_pd_table('assignments', conn).to_csv('data/assignments.csv', index=None)