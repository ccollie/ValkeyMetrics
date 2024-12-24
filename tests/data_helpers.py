import json
from datetime import datetime

PC_Timestamp = 'timestamp'
PC_Region = 'region'
PC_LocationType = 'location_type'
PC_Consumption = 'consumption'

class PowerConsumptionRecord:
    def __init__(self, timestamp, region, location_type, consumption):
        self.timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        self.region = region
        self.location_type = location_type
        self.consumption = float(consumption)

    def __repr__(self):
        return (f"PowerConsumptionRecord(timestamp={self.timestamp}, region={self.region}, "
                f"location_type={self.location_type}, consumption={self.consumption})")
    def key(self):
        return 'power_consumption:{}::{}'.format(self.region, self.location_type)
    def metric(self):
        return ('power_consumption{{region="{}",location_type="{}"}}'
                .format(self.region, self.location_type))


def load_json_rows(file_path):
    """
    Generator function to load rows from a JSON file.

    Args:
    file_path (str): Path to the JSON file.

    Yields:
    dict: Each row from the JSON file as a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            # Load the entire JSON content
            data = json.load(file)

            # Check if the loaded data is a list
            if isinstance(data, list):
                for row in data:
                    yield row
            # If it's a dictionary, yield it as a single item
            elif isinstance(data, dict):
                yield data
            else:
                raise ValueError("JSON file must contain a list of objects or a single object")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")



def load_power_consumption_data():
    with open('../data/power_consumption_data.json', 'r') as f:
        data = json.load(f)
        res = {}
        for row in data:
            ts = row[PC_Timestamp]
            region = row[PC_Region]
            location_type = row[PC_LocationType]
            consumption = row[PC_Consumption]

            group_key = region + ':' + location_type
            if group_key not in res:
                res[group_key] = []

            res[group_key].append([ts, consumption])

        return res


def load_text_rows(file_path):
    """
    Generator function to load rows from a text file.

    Args:
    file_path (str): Path to the text file.

    Yields:
    str: Each row from the text file.
    """
    try:
        with open(file_path, 'r') as file:
            for row in file:
                yield row
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def load_samples_from_file(filename, start=None, step=None):
    """
    Load a series from a file.

    Args:
    filename (str): Path to the file.

    Returns:
    list: A list of series data.
    """
    if start is None:
        start = datetime.now() - datetime.timedelta(days=1)  # Load data for the previous day

    # Calculate the interval between each data point
    if step is None:
        step = datetime.timedelta(minutes=5)

    timestamp = start
    for row in load_text_rows(filename):
        yield [timestamp, row]
        timestamp += step

def load_cpu_data(start=None, step=None):
    return load_samples_from_file('../data/cpu-values.txt', start, step)

def load_memory_data(start=None, step=None):
    return load_samples_from_file('../data/valkey-memory.txt', start, step)