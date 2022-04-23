from database.db_utils import get_db_conn, get_summary
import pprint
from utils import get_args



if __name__ == '__main__':
    conn = get_db_conn()
    args = get_args()

    args.conn = conn
    pprint.pprint(get_summary(args))