from database.db_utils import *

conn = get_db_conn()

update_delta(conn)
