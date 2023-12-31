import time
from breakout_bme68x import BreakoutBME68X
from pimoroni_i2c import PimoroniI2C

PINS_BREAKOUT_GARDEN = {"sda": 4, "scl": 5}

i2c = PimoroniI2C(**PINS_BREAKOUT_GARDEN)
bme = BreakoutBME68X(i2c)
# If this gives an error, try the alternative address
# bme = BreakoutBME68X(i2c, 0x77)

while True:
    temperature, pressure, humidity, gas, status, _, _ = bme.read()
    heater = "Stable" if status & STATUS_HEATER_STABLE else "Unstable"
    print("{:0.2f}c, {:0.2f}Pa, {:0.2f}%, {:0.2f} Ohms, Heater: {}".format(
        temperature, pressure, humidity, gas, heater))
    time.sleep(1.0)