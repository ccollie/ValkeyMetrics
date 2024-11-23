import json
from datetime import datetime



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

# Example usage
if __name__ == "__main__":
    json_path = "path/to/your/json/file.json"

    for elem in load_json_rows(json_path):
        print(elem)
        # Process each row as needed