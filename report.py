from dynamo import Dynamo
import datetime, statistics, math, argparse, sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import os.path


import pickle


# validação dos parametros de entrada
def validacao_parametros():
  parser = argparse.ArgumentParser()
  parser.add_argument("-p", "--publisher", help = "Nome da tabela do publisher")
  parser.add_argument("-s", "--subscriber", help = "Nome da tabela do subscriber")

  response = {
    'publisher': parser.parse_args().publisher,
    'subscriber': parser.parse_args().subscriber
  }
  for _,v in response.items():
    if v is None:
      sys.exit('Todos os parametros deve ser especificado, chame --help para saber todos os parametros')

  return response


# validação das mensagens buscando as metricas
def validacao_mensagens(pub_items, sub_items):

  # validação da falta de algum elementos
  count = 0
  for a in pub_items:
    for b in sub_items:
      if a['id'] == b['id']:
        count += 1
  porcentagem = count * 100 / len(pub_items)

  # geração da diferença do horário enviado e recebido
  diferenca = []
  mensagens = []
  for item in sub_items:
    received_time = datetime.datetime.strptime(item['received_time'], '%Y-%m-%d %H:%M:%S.%f')
    sent_time = datetime.datetime.strptime(item['sent_time'], '%Y-%m-%d %H:%M:%S.%f')
    difference = (received_time - sent_time).total_seconds()
    new = {
      'received_time': received_time,
      'sent_time': sent_time,
      'difference': difference,
      'id': item['id']
    }
    diferenca.append(difference)
    mensagens.append(new)

  return diferenca, mensagens, porcentagem

def inter_arrival_times(mesagens):
  timestamp = []
  arrival = []
  mesagens.sort(key=lambda x:x['received_time'])

  for i in range(len(mesagens)):
    if i > 0:
      timestamp.append(mesagens[i]['received_time'])
      arrival.append((mesagens[i]['received_time']-mesagens[i-1]['received_time']).total_seconds())

  xticks = []
  xticks.append(timestamp[0])
  for i in range(len(timestamp)):
    if arrival[i] > 2:
      xticks.append(timestamp[i-1])
      xticks.append(timestamp[i])
  xticks.append(timestamp[-1])

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.plot_date(timestamp,arrival, ls='-', marker='.')
  ax.set_xticks(xticks)
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S.%f'))
  ax.set_title('Intervalo de chegada das mensagens')
  ax.set_ylabel('Diferença tempo entre mensanges (segundos)')
  ax.set_xlabel('Horário de recebimento das mensangens')
  fig.autofmt_xdate(rotation=45)
  fig.tight_layout()
  plt.show()
def inter_arrival_rate(mesagens):
  mesagens.sort(key=lambda x:x['received_time'])
  count = []
  timestamp = []

  timestamp.append(mesagens[0]['received_time'])
  j = 1
  for i in range(1,len(mesagens)):
    if mesagens[i]['received_time'] - timestamp[-1] > datetime.timedelta(seconds=10):
      timestamp.append(mesagens[i]['received_time'])
      count.append(j)
      j = 1
    else:
      j += 1
  count.append(j)

  xticks = []
  xticks.append(timestamp[0])
  for i in range(1,len(timestamp)-1):
    if count[i] < 20:
      xticks.append(timestamp[i])
      # xticks.append(timestamp[i+1])
    if i == int(len(timestamp)/2):
      xticks.append(timestamp[i])
  xticks.append(timestamp[-1])

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.plot_date(timestamp,count, ls='-', marker='.')
  ax.set_xticks(xticks)
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S.%f'))
  #ax.set_title('Taxa de chegada das mensagens')
  ax.set_ylabel('Taxa das mensagens no período de 10 segundos ')
  ax.set_xlabel('Horário de recebimento das mensangens')
  fig.autofmt_xdate(rotation=45)
  fig.tight_layout()
  plt.show()
def inter_sending_rate(mesagens):
  sent_time = []
  for message in mesagens:
    sent_time.append(datetime.datetime.strptime(message['sent_time'], '%Y-%m-%d %H:%M:%S.%f'))
  sent_time.sort()

  count = []
  timestamp = []

  timestamp.append(sent_time[0])
  j = 1
  for i in range(1,len(mesagens)):
    if sent_time[i] - timestamp[-1] > datetime.timedelta(seconds=10):
      timestamp.append(sent_time[i])
      count.append(j)
      j = 1
    else:
      j += 1
  count.append(j)

  xticks = []
  xticks.append(timestamp[0])
  for i in range(1,len(timestamp)-1):
    if count[i] < 20:
      xticks.append(timestamp[i])
      xticks.append(timestamp[i+1])
  xticks.append(timestamp[-1])

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.plot_date(timestamp,count, ls='-', marker='.')
  ax.set_xticks(xticks)
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S.%f'))
  #ax.set_title('Taxa de envio das mensagens')
  ax.set_ylabel('Taxa das mensagens no período de 10 segundos ')
  ax.set_xlabel('Horário de envio das mensangens')
  fig.autofmt_xdate(rotation=45)
  fig.tight_layout()
  plt.show()
def grafico_atraso(mensagens):
  mensagens.sort(key=lambda x:x['received_time'])
  difference  = []
  received_time = []
  for mensagen in mensagens:
    difference.append(mensagen['difference'])
    received_time.append(mensagen['received_time'])

  xticks = []
  xticks.append(received_time[0])
  for i in range(len(difference)):
    if difference[i] > 5:
      if received_time[i] - xticks[-1] > datetime.timedelta(seconds=60):
        xticks.append(received_time[i])
  xticks.append(received_time[-1])

  fig = plt.figure()
  ax = fig.add_subplot(111)
  ax.plot_date(received_time,difference, marker='.')
  ax.set_xticks(xticks)
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S.%f'))
  #ax.yaxis.set_major_formatter(mdates.DateFormatter('%S.%f'))

  #ax.set_title('Taxa de envio das mensagens')
  ax.set_ylabel('Atraso das mensagens em segundos')
  ax.set_xlabel('Horário de recebimento das mensangens')
  fig.autofmt_xdate(rotation=45)
  fig.tight_layout()
  plt.show()
def main():

  parametros = validacao_parametros()
  if os.path.exists('dump/' + parametros['publisher']):
    pub_items=pickle.load(open('dump/' + parametros['publisher'], 'rb'))
  else:
    pub_table = Dynamo(parametros['publisher'])
    pub_items =  pub_table.scan_messages()
    with open('dump/' + parametros['publisher'], 'wb') as fp:
      pickle.dump(pub_items, fp)

  if os.path.exists('dump/' + parametros['subscriber']):
    sub_items=pickle.load(open('dump/' + parametros['subscriber'], 'rb'))
  else:
    sub_table = Dynamo(parametros['subscriber'])
    sub_items =  sub_table.scan_messages()
    with open('dump/' + parametros['subscriber'], 'wb') as fp:
      pickle.dump(sub_items, fp)

  diferenca, mensagens, porcentagem = validacao_mensagens(pub_items, sub_items)

  #inter_arrival_times(mensagens)
  #inter_arrival_rate(mensagens)
  #inter_sending_rate(pub_items)
  #grafico_atraso( mensagens)

  #impressão dos resultados optidos
  print("Foi enviado "+ str(len(pub_items)) + " mensagens")
  print("Foram recebidos " + str(porcentagem) + "% das mensagens!")
  print('A média aritmética dos atrasos foram ' + str(statistics.mean(diferenca)) +' segundos')
  print('A mediana dos atrasos foram ' + str(statistics.median(diferenca)) +' segundos')
  print('A variância populacional dos atrasos foram ' + str(statistics.pvariance(diferenca)) +' segundos')
  print('O desvio padrão populacional dos atrasos foram ' + str(statistics.pstdev(diferenca)) +' segundos')
  min_index = diferenca.index(min(diferenca))
  max_index = diferenca.index(max(diferenca))
  print('O maior atraso foi da mensagem id: ' + str(mensagens[max_index]['id']) + ' que teve uma diferença de ' + str(diferenca[max_index]) + ' segundos')
  print('O menor atraso foi da mensagem id: ' + str(mensagens[min_index]['id']) + ' que teve uma diferença de ' + str(diferenca[min_index]) + ' segundos')


if __name__ == '__main__':
    main()
