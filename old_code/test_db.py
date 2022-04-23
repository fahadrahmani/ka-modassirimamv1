from database.db_utils import *
from utils import *
from glob import glob

ls = glob('data/progress*.csv')
print(ls)

conn = get_db_conn()
for file in ls:
    t = file.split('_')[1]
    t = t.split('.')[0]
    print(t)
    combined = pd.read_csv(file)
    update_progress(combined, t, conn)