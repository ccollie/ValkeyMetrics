from RLTest import Env

from data_helpers import ingest_temperature_data
from valkeytests.common import getModulePath, LOG_DIR, getServerPath

PIPELINE_SIZE = 1000
OUTPUT_DIR = './rdbs'

def run():
    module_path = getModulePath()
    server_path = getServerPath(None)
    print("Module path", module_path)
    env = Env(module=module_path, redisBinaryPath=server_path, logDir=LOG_DIR, enableDebugCommand=True, enableModuleCommand=True)
    env.start()

    with env.getConnection() as r:
        r.ping()
        rdb_dir = r.execute_command('CONFIG', 'GET', 'DIR')
        print(rdb_dir[1])
        modules = r.module_list()
        print("Modules = ", modules)
        ingest_temperature_data(r)
        r.save()
        r.ping()

if __name__ == '__main__':
    run()