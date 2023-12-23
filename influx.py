import yaml
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, DEFAULT_WRITE_PRECISION


class InfluxWriter:

    def __init__(self, config_path: str, tag_name: str):
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

        self.tag_name = tag_name


    def publish(self, tag_value: str, fields: dict, bucket: str = None, timestamp: int = None):
        for k,v in fields.items():
            record = f"{k},{self.tag_name}={tag_value.replace(' ','_')} value={v}"
            if timestamp:
                record += f" {timestamp}"
                precision = 's' # python uses time in seconds, Influx uses nanoseconds by default
            else:
                precision = DEFAULT_WRITE_PRECISION
            #print("Write record", record, "to bucket", bucket or self.bucket)
            self.write_api.write(bucket=bucket or self.bucket, org=self.org, record=record, write_precision=precision)

