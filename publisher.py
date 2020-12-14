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
  parser.add_argument("-m", "--messages", help = "Quantidade de mensagens que deve ser eviada por conexão")
  parser.add_argument("-s", "--seconds", help = "Tempo em segundos que o envio da mensagens deve ser feito")

  response = {
    'topic': parser.parse_args().topic,
    'broker': parser.parse_args().broker,
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


def publisher(mqtt, topic):
  global dynamodb

  message = { }
  message['id'] = str(uuid1())
  message['timestamp'] = str(datetime.datetime.now())
  message['temperature'] = randint(25, 40)
  message['humidity'] = randint(15, 95)
  mqtt.publish(topic,json.dumps(message))
  dynamodb.put_message(message)

  print('Enviando nova mensagem ' + str(json.dumps(message)) + ' no tópico ' + topic)


def schedule_it(scheduler,messages, duration, callable, *args):
    for i in range(messages):
        delay = i * duration/messages
        scheduler.enter( delay, 1, callable, args)


def main():
  # topic, scenario, host, user, password, dynamo_table_name, messages, timer = validacao_parametros()
  parametros = validacao_parametros()
  mqtt = Mqtt(parametros['broker'],parametros['user'],parametros['password'])
  global dynamodb
  dynamodb = Dynamo(parametros['dynamodb'])

  scheduler = sched.scheduler(time.time, time.sleep)
  schedule_it(scheduler,int(parametros['messages']),int(parametros['seconds']),publisher,mqtt,parametros['topic'])
  scheduler.run()


if __name__ == '__main__':
    main()
