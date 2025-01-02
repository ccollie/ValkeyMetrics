import os
import datetime
import calendar
import shutil
from datetime import datetime

import redis

from data_helpers import load_power_consumption_data

def parse_timestamp(ts):
    date_time_obj = datetime.strptime(ts, '%Y-%m-%d')
    return calendar.timegm(date_time_obj.timetuple()) * 1000


def load_into_redis(redis_conn):
    r = redis_conn.pipeline(transaction=False)
    count = 0
    data = load_power_consumption_data()
    for key in data.keys():
        split = key.split(':')
        region = split[0]
        location = split[1]
        metric = 'consumption{{region="{}",location_type="{}"}}'.format(region, location)
        r.execute_command('VM.CREATE-SERIES', key,  metric)

    for key, values in data.items():
        for ts, consumption in values:
            r.execute_command('VM.ADD', key, ts, consumption)
            count += 1
        r.execute()


WORK_DIR = 'work'
RDB_PATH = os.path.join(WORK_DIR, 'dump.rdb')
PORT = 6379

def main(version):
    if not os.path.exists(WORK_DIR):
        os.mkdir(WORK_DIR)
    elif os.path.exists(RDB_PATH):
        os.unlink(RDB_PATH)

    try:
        redis_conn = redis.StrictRedis(port=PORT)

        print("waiting for server")
        while True:
            try:
                redis_conn.ping()
                print("done")
                break
            except redis.exceptions.ConnectionError:
                pass

        redis_conn.flushall()

        load_into_redis(redis_conn)

        redis_conn.save()
        redis_conn.ping()
        shutil.copyfile(RDB_PATH, os.path.join('rdb', "{}.rdb".format(version)))

    finally:
        proc.send_signal(15)


if __name__ == '__main__':
    main(sys.argv[1])
