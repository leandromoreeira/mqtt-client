import time, json, os, sys, argparse, sched
from datetime import datetime
from random import randint
from uuid import uuid1
from dynamo import Dynamo
from mqtt import Mqtt

dynamodb = None
mqtt_chaos = None
mqtt_control = None

def validacao_parametros():
  parser = argparse.ArgumentParser()
  parser.add_argument("-t", "--topic", help = "Tópico utilizado para subscrever ou publicar")
  parser.add_argument("-b1", "--broker1", help = "Endereço do Broker MQTT de Controle")
  parser.add_argument("-b2", "--broker2", help = "Endereço do Broker MQTT do Caos")
  parser.add_argument("-u", "--user", help = "Usuário do Broker MQTT")
  parser.add_argument("-p", "--password", help = "Senha do usuário do Broker MQTT")
  parser.add_argument("-d", "--dynamodb", help = "Nome da tablela do DynamoDB")
  parser.add_argument("-m", "--messages", help = "Quantidade de mensagens que deve ser eviada por conexão")
  parser.add_argument("-s", "--seconds", help = "Tempo em segundos que o envio da mensagens deve ser feito")

  response = {
    'topic': parser.parse_args().topic,
    'broker1': parser.parse_args().broker1,
    'broker2': parser.parse_args().broker2,
    'user': parser.parse_args().user,
    'password': parser.parse_args().password,
    'dynamodb': parser.parse_args().dynamodb,
    'messages': parser.parse_args().messages,
    'seconds': parser.parse_args().seconds
  }
  for _,v in response.items():
    if v is None:
      sys.exit('Todos os parametros deve ser especificado, chame --help para saber todos os parametros')

  return response


def publisher():

  message = { }
  message['id'] = str(uuid1())
  message['sent_time'] = str(datetime.now())
  message['temperature'] = randint(25, 40)
  message['humidity'] = randint(15, 95)
  mqtt_chaos.publish(json.dumps(message))
  mqtt_control.publish(json.dumps(message))
  dynamodb.put_message(message)

  print('Enviando nova mensagem ' + str(json.dumps(message)) + ' no tópico ' + mqtt_chaos.topic)


def schedule_it(scheduler,messages, duration, callable, *args):
    for i in range(messages):
        delay = i * duration/messages
        scheduler.enter( delay, 1, callable, args)


def main():

  parametros = validacao_parametros()

  global dynamodb; global mqtt_chaos; global mqtt_control

  mqtt_chaos = Mqtt(parametros['broker2'],parametros['user'],parametros['password'],parametros['topic'])
  mqtt_control = Mqtt(parametros['broker1'],parametros['user'],parametros['password'],parametros['topic'])
  dynamodb = Dynamo(parametros['dynamodb'])

  scheduler = sched.scheduler(time.time, time.sleep)
  schedule_it(scheduler,int(parametros['messages']),int(parametros['seconds']),publisher)

  mqtt_chaos.connect()
  mqtt_control.connect()
  mqtt_chaos.client.loop_start()
  mqtt_control.client.loop_start()

  scheduler.run()

  mqtt_chaos.client.loop_stop()
  mqtt_control.client.loop_stop()
  mqtt_control.client.disconnect()
  mqtt_chaos.client.disconnect()

if __name__ == '__main__':
    main()
