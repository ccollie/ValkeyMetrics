import pytest
import redis
from RLTest import Env

def test_commands():
    with Env().getConnection(1) as r:
        commands = r.execute_command('COMMAND', 'LIST', 'FILTERBY', 'PATTERN', 'VM.*')
        print(commands)


def test_create_series():
    with Env().getConnection(1) as r:
        r.execute_command('VM.CREATE-SERIES', 'temperature:3:east', 'temperature{area_id="32",sensor_id="1",region="east"}', 'RETENTION', '24h')
        assert r.type('temperature:3:east') == 'VKMSERIES'
