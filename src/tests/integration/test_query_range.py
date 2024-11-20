import os
import json
import datetime
import calendar
import shutil
from datetime import datetime

import redis

def parse_timestamp(ts):
    date_time_obj = datetime.strptime(ts, '%Y-%m-%d')
    return calendar.timegm(date_time_obj.timetuple()) * 1000

Timestamp = 'timestamp'
Region = 'region'
LocationType = 'location_type'
Consumption = 'consumption'

def load_power_consumption_data():
    with open('../data/power_consumption_data.json', 'r') as f:
        d = json.load(f)
        return d

def group_data(data):
    res = {}
    for row in data:
        ts = row[Timestamp]
        region = row[Region]
        location_type = row[LocationType]
        consumption = row[Consumption]

        group_key = region + ':' + location_type
        if group_key not in res:
            res[group_key] = []

        res[group_key].append((ts, consumption))

def load_data():
    data = load_power_consumption_data()
    group_data(data)
    return data


def load_into_redis(redis_conn):
    r = redis_conn.pipeline(transaction=False)
    count = 0
    data = load_data()
    for key in data.keys():
        split = key.split(':')
        region = split[0]
        location = split[1]
        metric = 'consumption\{region="{}",location_type="{}"\}'.format(region, location)
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
