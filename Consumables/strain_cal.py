#

#import all
from hx711_multi import HX711
import RPi.GPIO as GPIO

#init GPIO and set GPIO pin mode to BCM numbering
GPIO.setmode(GPIO.BCM)

#create HX711 instance
hx711 = HX711(dout_pins= 17,
              sck_pin= 4,
              channel_A_gain = 128,
              channel_select = 'A',
              all_or_nothing = False,
              log_level = 'CRITICAL')

#calibration using known weights, populate list with values before running for faster cycling.
weight_multiple = hx711.run_calibration(known_weights=[52, 311, 1842, 3369])
print(f'Weight multiple = {weight_multiple}')

#final output value to be entered into strain_test.py