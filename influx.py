import yaml
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxWriter:

    def __init__(self, config_path: str):
        with open(config_path) as f:
            config = yaml.safe_load(f)
            self.org = config['influx']['org']
            influx = influxdb_client.InfluxDBClient(
                    url=config['influx']['url'],
                    token=config['influx']['token'],
                    org=self.org
                    )
            self.write_api = influx.write_api(write_options=SYNCHRONOUS)
            self.bucket = config['influx']['bucket']


    def publish(self, name: str, fields: dict):
        p = influxdb_client.Point(name)
        for k, v in fields.items():
            p.field(k, v)
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)

