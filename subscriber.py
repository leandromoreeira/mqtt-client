import time, json, os, sys, argparse, sched
from datetime import datetime
from random import randint
from uuid import uuid1
from dynamo import Dynamo
from mqtt import Mqtt


def validacao_parametros():
  parser = argparse.ArgumentParser()
  parser.add_argument("-t", "--topic", help = "Tópico utilizado para subscrever ou publicar")
  parser.add_argument("-b", "--broker", help = "Endereço do Broker MQTT")
  parser.add_argument("-u", "--user", help = "Usuário do Broker MQTT")
  parser.add_argument("-p", "--password", help = "Senha do usuário do Broker MQTT")
  parser.add_argument("-d", "--dynamodb", help = "Nome da tablela do DynamoDB")

  response = {
    'topic': parser.parse_args().topic,
    'broker': parser.parse_args().broker,
    'user': parser.parse_args().user,
    'password': parser.parse_args().password,
    'dynamodb': parser.parse_args().dynamodb,
  }
  for _,v in response.items():
    if v is None:
      sys.exit('Todos os parametros deve ser especificado, chame --help para saber todos os parametros')

  return response


def on_message(mqttc, obj, msg):
  msg_payload = str(msg.payload.decode('utf-8'))
  print('Nova mensagen no tópico ' + msg.topic + ' usando qos ' + str(msg.qos) + ' que contêm ' + msg_payload)
  parsed_msg = json.loads(msg_payload)
  parsed_msg['received_time'] = str(datetime.now())
  dynamodb.put_message(parsed_msg)


def on_connect(mqttc, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  if rc == 0 :
    mqtt.connected = True
    mqtt.subscribe()


def subscriber():
  mqtt.connect()
  mqtt.client.on_message = on_message
  mqtt.loop_forever()


def main():
  parametros = validacao_parametros()
  global dynamodb ; global mqtt

  mqtt = Mqtt(parametros['broker'],parametros['user'],parametros['password'],parametros['topic'],on_connect)
  dynamodb = Dynamo(parametros['dynamodb'])

  subscriber()


if __name__ == '__main__':
    main()
