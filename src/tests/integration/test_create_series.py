import pytest
import redis
from RLTest import Env

def test_commands():
    with Env().getConnection(1) as r:
        commands = r.execute_command('COMMAND', 'LIST', 'FILTERBY', 'PATTERN', 'VM.*')
        print(commands)

def test_create_params():
    with Env().getClusterConnectionIfNeeded() as r:
        # test string instead of value
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'RETENTION', 'retention')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'CHUNK_SIZE', 'chunk_size')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'ENCODING')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', 'abc')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', '-2')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', 'abc')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '-2')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '4000000000')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'DUPLICATE_POLICY', 'bla')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'invalid', 'LABELS', 'key', 'val', 'label', 'DUPLICATE_POLICY', 'bla')


        r.execute_command('VM.CREATE-SERIES', 'a')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('VM.CREATE-SERIES', 'a')  # filter exists

def test_create_series():
    with Env().getConnection(1) as r:
        r.execute_command('VM.CREATE-SERIES', 'temperature:3:east', 'temperature{area_id="32",sensor_id="1",region="east"}', 'RETENTION', '24h')
        assert r.type('temperature:3:east') == 'VKMSERIES'
