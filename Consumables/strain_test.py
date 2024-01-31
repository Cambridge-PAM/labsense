from hx711_multi import HX711
from time import perf_counter
import RPi.GPIO as GPIO

#init GPIO and set GPIO pin mode to BCM numbering
GPIO.setmode(GPIO.BCM)

readings_to_average = 10
sck_pin = 4
dout_pins = [17]
weight_multiples = [-3.008] #values to be determined at setup from calibration with known masses. Run strain_cal.py to obtain value for each linked HX711.

#create HX711 instance
hx711 = HX711(dout_pins=dout_pins,
              sck_pin=sck_pin,
              channel_A_gain = 128,
              channel_select = 'A',
              all_or_nothing = False,
              log_level = 'CRITICAL')

#reset and zero ADC
hx711.reset()
try:
    hx711.zero(readings_to_average=readings_to_average*3)
except Exception as e:
    print(e)
hx711.set_weight_multiples(weight_multiples=weight_multiples)
