import calendar
import csv
import datetime
import os
import tempfile
import zipfile
from datetime import datetime

from RLTest import Env

from tests.integration import MODULE_PATH, LOG_DIR

PIPELINE_SIZE = 1000
OUTPUT_DIR = './rdbs'

class TemperatureRecord:
    def __init__(self, sensor_id, air_temp, day, hour, install_type, borough, nta_code):
        self.sensor_id = sensor_id
        self.air_temp = air_temp
        self.day = day
        self.hour = hour
        self.timestamp = create_timestamp(day, hour)
        self.install_type = install_type
        self.borough = borough
        self.nta_code = nta_code

    def __repr__(self):
        return (f"TemperatureRecord(sensor_id={self.sensor_id}, air_temp={self.air_temp}, "
                f"day={self.day}, hour={self.hour}, install_type={self.install_type}, "
                f"borough={self.borough}, nta_code={self.nta_code})")
    def key(self):
        return 'ny_temps:{}'.format(self.nta_code)

    def metric(self):
        return ('ny_temps{{nta_code="{}",sensor_id="{}",borough="{}",install_type="{}"}}'
                .format(self.nta_code, self.sensor_id, self.borough, self.install_type))

# ['Sensor.ID', 'AirTemp', 'Day', 'Hour', 'Install.Type', 'Borough', 'ntacode']
# ['Bk-BR_01', '71.189', '06/15/2018', '1', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '70.24333333', '06/15/2018', '2', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '69.39266667', '06/15/2018', '3', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '68.26316667', '06/15/2018', '4', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '67.114', '06/15/2018', '5', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '65.9655', '06/15/2018', '6', 'Street Tree', 'Brooklyn', 'BK81']

# column indexes
SENSOR_ID = 0
AIR_TEMP = 1
DAY = 2
HOUR = 3
INSTALL_TYPE = 4
BOROUGH = 5
NTA_CODE = 6

def load_rows_from_csv():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Open the ZIP file
        with zipfile.ZipFile('../data/Hyperlocal_Temperature_Monitoring_20241012_1M.zip', 'r') as zip_ref:
            # Extract all contents to the temporary directory
            zip_ref.extractall(temp_dir)

        # Find the CSV file in the temporary directory
        csv_file = next(file for file in os.listdir(temp_dir) if file.endswith('.csv'))
        csv_path = os.path.join(temp_dir, csv_file)

        count = 0
        # Parse the timestamp and convert it to Unix timestamp
        # Read and print the CSV contents
        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                sensor_id = row[SENSOR_ID]
                if sensor_id == 'Sensor.ID':
                    continue

                air_temp = row[AIR_TEMP]
                day = row[DAY]
                hour = row[HOUR]
                install_type = normalize_string(row[INSTALL_TYPE])
                borough = row[BOROUGH]
                nta_code = row[NTA_CODE]

                # print(f"Sensor ID: {sensor_id}, Air Temp: {air_temp}, Day: {day}, Hour: {hour}, ")
                # Create a TemperatureRecord object
                record = TemperatureRecord(sensor_id, air_temp, day, hour, install_type, borough, nta_code)
                count += 1
                if count > 500:
                    break
                yield record

def normalize_string(s):
    return s.lower().replace(' ', '_')

def create_timestamp(day, hour):
    date_time_obj = datetime.strptime(day, '%m/%d/%Y')
    date_time_obj = date_time_obj.replace(hour=int(hour))
    return calendar.timegm(date_time_obj.timetuple()) * 1000

def load_into_redis(redis_conn):
    print("Loading data into Redis...")
    r = redis_conn.pipeline(transaction=False)
    count = 0
    added_keys = set([])

    print("Loading rows...")

    for row in load_rows_from_csv():
        # print("Loading row: ", row)
        if row.timestamp < 0:
            continue

        temperature = row.air_temp
        if temperature is None:
            continue
            
        if count > PIPELINE_SIZE:
            r.execute()
            count = 0
            r = redis_conn.pipeline(transaction=False)


        # Create series if not already exists
        key = row.key()
        if key not in added_keys:
            added_keys.add(key)
            metric = row.metric()
            redis_conn.execute_command('VM.CREATE-SERIES', key, row.metric(), 'DECIMAL_DIGITS', 1)
            print(f"Created series: {key}, metric={metric}")

        r.execute_command('VM.ADD', key, row.timestamp, temperature)
        count += 1

    r.execute()

def run():
    print("Module path", MODULE_PATH)
    env = Env(module=MODULE_PATH, logDir=LOG_DIR, enableDebugCommand=True, enableModuleCommand=True)
    with env.getConnection() as r:
        r.ping()
        rdb_dir = r.execute_command('CONFIG', 'GET', 'DIR')
        print(rdb_dir[1])
        # r.module_load(MODULE_PATH)
        modules = r.module_list()
        print("Modules = ", modules)
        load_into_redis(r)
        print("Data loaded into Redis")
        r.save()
        r.ping()

if __name__ == '__main__':
    run()