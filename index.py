import paho.mqtt.client as mqtt
import time, datetime, json, os, sys
import argparse
from decouple import config
from random import randint
from uuid import uuid1
import boto3


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--topic", help = "Tópico utilizado para subscrever ou publicar")
parser.add_argument("-s", "--scenario", help = "Cenário utilizado na execução atual publisher/subscriber")

BROKER_HOST = config('BROKER_HOST')
BROKER_PORT = config('BROKER_PORT')
BROKER_USER = config('BROKER_USER')
BROKER_PASSWORD = config('BROKER_PASSWORD')
DYNAMO_TABLE = config('DYNAMO_TABLE')
TOPIC = parser.parse_args().topic
SCENARIO = parser.parse_args().scenario
connected = True

if TOPIC == None or SCENARIO == None:
  sys.exit('topic and scenario should be specified, use the args')

def put_telemetry(message,dynamodb=None):
  if not dynamodb:
    dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(DYNAMO_TABLE)
  response = table.put_item(
       Item=message
  )
  return response

def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))
    global connected
    connected = True


def on_message(mqttc, obj, msg):
    msg_payload = str(msg.payload.decode('utf-8'))
    print('New message on topic ' + msg.topic + ' using qos ' + str(msg.qos) + ' containing ' + msg_payload)
    parsed_msg = json.loads(msg_payload)
    put_telemetry(parsed_msg)

def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print(string)


def on_disconnect(mqttc, obj, rc):
    print('disconnected...rc=' + str(rc))
    global connected
    connected = False


client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_disconnect = on_disconnect


client.username_pw_set(username=BROKER_USER,password=BROKER_PASSWORD)
client.connect(BROKER_HOST)

if SCENARIO == 'subscriber':
  client.subscribe(TOPIC)
  client.loop_forever()

elif SCENARIO == 'publisher':
  while(True):
    if not connected:
      client.connect(BROKER_HOST)
    message = { }
    message['id'] = str(uuid1())
    message['timestamp'] = str(datetime.datetime.now())
    message['temperature'] = randint(25, 40)
    message['humidity'] = randint(15, 95)
    client.publish(TOPIC,json.dumps(message))
    put_telemetry(message)
    print(json.dumps(message))
    time.sleep(10)

else:
   sys.exit('scenario should publisher or subscriber')