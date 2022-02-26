import csv
import argparse
from datetime import datetime
from pms_a003 import Sensor
from oled_091 import SSD1306
from time import sleep
from os import path
from serial import SerialException

# setup CLI argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--csv', dest='csv', help="History CSV to append to")
parser.add_argument('--debug', action='store_true')
parser.add_argument('--write-delay', dest='write_delay', type=int, default=60, help="How long (in seconds) between writes to CSV file")
args = parser.parse_args()

TIME_FORMAT = "%b %d %Y @ %H:%M:%S"
DIR_PATH = path.abspath(path.dirname(__file__))
DefaultFont = path.join(DIR_PATH, "Fonts/GothamLight.ttf")

def info_print():
    oled_display.DirImage(path.join(DIR_PATH, "Images/SB.png"))
    oled_display.DrawRect()
    oled_display.ShowImage()
    sleep(1)
    oled_display.PrintText("  Waiting....", FontSize=14)
    oled_display.ShowImage()


oled_display = SSD1306()
air_mon = Sensor()
air_mon.connect_hat(port="/dev/ttyAMA0", baudrate=9600)


if __name__ == "__main__":
    info_print()
try:
    last_write = None
    while True:
        values = air_mon.read()
        if args.debug:
            print("PM 1.0 : {} \tPM 2.5 : {} \tPM 10 : {}".format(
                values.pm10_cf1, values.pm25_cf1, values.pm100_cf1))

        oled_display.PrintText("PM1.0= {:2d}".format(values.pm10_cf1),
                               cords=(2, 2), FontSize=10)
        oled_display.PrintText("PM2.5= {:2d}".format(values.pm25_cf1),
                               cords=(65, 2), FontSize=10)
        oled_display.PrintText("PM10= {:2d}".format(values.pm100_cf1),
                               cords=(25, 20), FontSize=13)
        oled_display.ShowImage()

        now = datetime.now()
        if args.csv:
            if last_write is None or (now - last_write).seconds >= args.write_delay:
                with open(args.csv, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow((now.strftime(TIME_FORMAT), values.pm10_cf1, values.pm25_cf1, values.pm100_cf1))
                last_write = now

        sleep(1)

except KeyboardInterrupt:
    air_mon.disconnect_hat()

