import yaml
from pyvesync import VeSync
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# load VeSync username/password from a file in /etc
with open("/etc/vesync/vesync.yaml") as f:
    config = yaml.safe_load(f)

manager = VeSync(config['username'], config['password'], config['timezone'], debug=False, redact=True)
manager.login()

# Get/Update Devices from server - populate device lists
manager.update()

influx = influxdb_client.InfluxDBClient(
        url=config['influx']['url'],
        token=config['influx']['token'],
        org=config['influx']['org']
        )
write_api = influx.write_api(write_options=SYNCHRONOUS)

for device in manager.fans:
    data = device.details
    p = influxdb_client.Point(device.device_name).field("air_quality", data['air_quality_value'])
    write_api.write(bucket="vesync", org=config['influx']['org'], record=p)

