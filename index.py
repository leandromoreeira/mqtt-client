import paho.mqtt.client as mqtt
import time, datetime, json
from random import randint

TOPIC = 'uel/dc/lab'
BROKER= 'test.mosquitto.org'

client = mqtt.Client()

while(True):
  print("Publishing message...")
  client.connect(BROKER)
  message = { }
  message['timestamp'] = str(datetime.datetime.now())
  message['temperature'] = randint(25, 40)
  message['humidity'] = randint(15, 95)
  client.publish(TOPIC,json.dumps(message))
  print(json.dumps(message))
  time.sleep(10)