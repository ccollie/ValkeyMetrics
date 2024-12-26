from valkey_metrics_test_case import ValkeyMetricsTestCaseBase


class TestSeriesBasic(ValkeyMetricsTestCaseBase):

    def test_commands(self):
        client = self.server.get_new_client()
        commands = client.execute_command('COMMAND', 'LIST', 'FILTERBY', 'PATTERN', 'VM.*')
        print(commands)



    def test_create_series(self):
        client = self.server.get_new_client()
        client.execute_command('VM.CREATE-SERIES', 'temperature:3:east', 'METRIC', 'temperature{area_id="32",sensor_id="1",region="east"}', 'RETENTION', '24h')
        assert client.type('temperature:3:east') == 'VKMSERIES'
