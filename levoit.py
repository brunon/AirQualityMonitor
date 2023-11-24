import yaml
from pyvesync import VeSync
import influx

# load VeSync username/password from a file in /etc
with open("/etc/vesync/vesync.yaml") as f:
    config = yaml.safe_load(f)

manager = VeSync(config['username'], config['password'], config['timezone'], debug=False, redact=True)
manager.login()

# Get/Update Devices from server - populate device lists
manager.update()

influxdb = influx.InfluxWriter("/etc/vesync/vesync.yaml")

for device in manager.fans:
    influxdb.publish(device.device_name, {
        "air_quality": device.details["air_quality_value"]
        })

