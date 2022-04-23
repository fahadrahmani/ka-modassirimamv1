import sqlite3
from database.db_utils import get_db_conn

def run_one_time_only(db_conn):
    db_cur = db_conn.cursor()
    db_cur.executescript('''ALTER TABLE Assignments ADD COLUMN last_1 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_2 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_3 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_4 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_5 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_6 NUMERIC;
                            ALTER TABLE Assignments ADD COLUMN last_7 NUMERIC;''')
    db_cur.executescript('''
            CREATE TABLE IF NOT EXISTS track_log
            (last_1 TEXT,last_2 TEXT,last_3 TEXT,last_4 TEXT,last_5 TEXT,last_6 TEXT,last_7 TEXT);

            INSERT INTO track_log (last_1,last_2,last_3,last_4,last_5,last_6,last_7) 
            VALUES ("last_1","last_2","last_3","last_4","last_5","last_6","last_7");
        ''')
    #Comment out following line to make changes permenantly in database
    # db_conn.commit()


if __name__ == "__main__":
    run_one_time_only(get_db_conn())