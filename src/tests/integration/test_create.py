import pytest
import redis
from RLTest import Env

def test_create_params():
    with Env().getClusterConnectionIfNeeded() as r:
        # test string instead of value
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'RETENTION', 'retention')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'CHUNK_SIZE', 'chunk_size')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'ENCODING')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', 'abc')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', '-2')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', 'abc')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '-2')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '4000000000')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'DUPLICATE_POLICY', 'bla')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'label', 'DUPLICATE_POLICY', 'bla')


        r.execute_command('TS.CREATE', 'a')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.CREATE', 'a')  # filter exists
