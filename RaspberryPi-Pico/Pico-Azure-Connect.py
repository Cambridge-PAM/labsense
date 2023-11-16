import network
import time
import machine
from umqtt.simple import MQTTClient
from machine import Pin
  
ssid = 'DESKTOP-S2IAEKH 9692'
password = '4@7t588L'
 
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(ssid, password)
 
# Wait for connect or fail
max_wait = 10
while max_wait > 0:
    if wlan.status() < 0 or wlan.status() >= 3:
        break
    max_wait -= 1
    print('waiting for connection...')
    time.sleep(1)

# Handle connection error
if wlan.status() != 3:
    raise RuntimeError('network connection failed')
else:
    print('connected')
    status = wlan.ifconfig()
    print( 'ip = ' + status[0] )

led = Pin('LED', Pin.OUT)

hostname = 'tl45-test-iot-hub.azure-devices.net'
clientid = 'tl457-test-iot-device-001'
user_name = 'YOUR_IOT_HUB_NAME.azure-devices.net/picow/?api-version=2021-04-12'
passw = 'SharedAccessKey=2MpjHctsZWJd3POkmvKpQj/LFpr08Ngp2AIoTG6FqMY='
topic_pub = b'devices/picow/messages/events/'
topic_msg = b'{"buttonpressed":"1"}'
port_no = 8883
subscribe_topic = "devices/picow/messages/devicebound/#"

def mqtt_connect():

    certificate_path = "baltimore.cer"
    print('Loading Blatimore Certificate')
    with open(certificate_path, 'r') as f:
        cert = f.read()
    print('Obtained Baltimore Certificate')
    sslparams = {'cert':cert}
    
    client = MQTTClient(client_id=clientid, server=hostname, port=port_no, user=user_name, password=passw, keepalive=3600, ssl=True, ssl_params=sslparams)
    client.connect()
    print('Connected to IoT Hub MQTT Broker')
    return client

def reconnect():
    print('Failed to connect to the MQTT Broker. Reconnecting...')
    time.sleep(5)
    machine.reset()

def callback_handler(topic, message_receive):
    print("Received message")
    print(message_receive)
    if message_receive.strip() == b'led_on':
        led.value(1)
    else:
        led.value(0)

try:
    client = mqtt_connect()
    client.set_callback(callback_handler)
    client.subscribe(topic=subscribe_topic)
except OSError as e:
    reconnect()

while True:
    
    client.check_msg()
    
    if button.value():
        client.publish(topic_pub, topic_msg)
        time.sleep(0.5)
    else:
        pass