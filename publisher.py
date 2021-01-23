import time, json, os, sys, argparse, sched
from datetime import datetime
from random import randint
from uuid import uuid1
from logs import Logs
from mqtt import Mqtt

logs = None
mqtt = None

def validacao_parametros():
  parser = argparse.ArgumentParser()
  parser.add_argument("-t", "--topic", help = "Tópico utilizado para subscrever ou publicar")
  parser.add_argument("-b", "--broker", help = "Endereço do Broker MQTT")
  parser.add_argument("-u", "--user", help = "Usuário do Broker MQTT")
  parser.add_argument("-p", "--password", help = "Senha do usuário do Broker MQTT")
  parser.add_argument("-g", "--group", help = "group")
  parser.add_argument("-ss", "--stream", help = "stream")
  parser.add_argument("-m", "--messages", help = "Quantidade de mensagens que deve ser eviada por conexão")
  parser.add_argument("-s", "--seconds", help = "Tempo em segundos que o envio da mensagens deve ser feito")

  response = {
    'topic': parser.parse_args().topic,
    'broker': parser.parse_args().broker,
    'user': parser.parse_args().user,
    'password': parser.parse_args().password,
    'group': parser.parse_args().group,
    'stream': parser.parse_args().stream,
    'messages': parser.parse_args().messages,
    'seconds': parser.parse_args().seconds
  }
  for _,v in response.items():
    if v is None:
      sys.exit('Todos os parametros deve ser especificado, chame --help para saber todos os parametros')

  return response


def publisher():

  date = datetime.now()

  message = { }
  message['id'] = str(uuid1())
  message['sent_time'] = str(date)
  message['temperature'] = randint(25, 40)
  message['humidity'] = randint(15, 95)
  mqtt.publish(json.dumps(message))
  logs.put_log_event(int(date.timestamp()*1000),str(json.dumps(message)))

  print('Enviando nova mensagem ' + str(json.dumps(message)) + ' no tópico ' + mqtt.topic)


def schedule_it(scheduler,messages, duration, callable, *args):
    for i in range(messages):
        delay = i * duration/messages
        scheduler.enter( delay, 1, callable, args)


def main():

  parametros = validacao_parametros()

  global mqtt; global logs

  mqtt = Mqtt(parametros['broker'],parametros['user'],parametros['password'],parametros['topic'])
  logs = Logs(parametros['group'],parametros['stream'])

  scheduler = sched.scheduler(time.time, time.sleep)
  schedule_it(scheduler,int(parametros['messages']),int(parametros['seconds']),publisher)


  mqtt.connect()
  mqtt.client.loop_start()

  scheduler.run()

  mqtt.client.loop_stop()
  mqtt.client.disconnect()

if __name__ == '__main__':
    main()
