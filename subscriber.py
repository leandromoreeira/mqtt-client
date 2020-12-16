import time, datetime, json, os, sys, argparse, sched
from random import randint
from uuid import uuid1
from dynamo import Dynamo
from mqtt import Mqtt

dynamodb = None


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
  global dynamodb
  msg_payload = str(msg.payload.decode('utf-8'))
  print('Nova mensagen no tópico ' + msg.topic + ' usando qos ' + str(msg.qos) + ' que contêm ' + msg_payload)
  parsed_msg = json.loads(msg_payload)
  parsed_msg['s_timestamp'] = str(datetime.datetime.now())
  dynamodb.put_message(parsed_msg)


def subscriber(mqtt, topic):
  mqtt.client.on_message = on_message
  mqtt.subscribe(topic)
  mqtt.loop_forever()


def main():
  # topic, scenario, host, user, password, dynamo_table_name, messages, timer = validacao_parametros()
  parametros = validacao_parametros()
  mqtt = Mqtt(parametros['broker'],parametros['user'],parametros['password'])
  global dynamodb
  dynamodb = Dynamo(parametros['dynamodb'])

  subscriber(mqtt, parametros['topic'])


if __name__ == '__main__':
    main()
