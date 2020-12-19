from dynamo import Dynamo
import datetime, statistics, math, argparse, sys


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
    diferenca.append((received_time - sent_time).total_seconds())
    mensagens.append(item)

  return diferenca, mensagens, porcentagem


def main():
  parametros = validacao_parametros()
  pub_table = Dynamo(parametros['publisher'])
  sub_table = Dynamo(parametros['subscriber'])
  pub_items =  pub_table.scan_messages()
  sub_items =  sub_table.scan_messages()

  diferenca, mensagens, porcentagem = validacao_mensagens(pub_items, sub_items)

  # impressão dos resultados optidos
  print("Foram recebidos " + str(porcentagem) + "% das mensagens!")
  print('A média aritmética dos atrasos foram ' + str(statistics.mean(diferenca)) +' segundos')
  print('A mediana dos atrasos foram ' + str(statistics.median(diferenca)) +' segundos')
  print('A variância populacional dos atrasos foram ' + str(statistics.pvariance(diferenca)) +' segundos')
  print('O desvio padrão populacional dos atrasos foram ' + str(statistics.pstdev(diferenca)) +' segundos')
  min_index = diferenca.index(min(diferenca))
  max_index = diferenca.index(max(diferenca))
  print('O maior atraso foi da mensagem id: ' + str(mensagens[max_index]['id']) + ' temperatura ' + str(int(mensagens[max_index]['temperature'])) + ' e humidade ' + str(int(mensagens[max_index]['humidity'])) + ' que teve uma diferença de ' + str(diferenca[max_index]) + ' segundos')
  print('O menor atraso foi da mensagem id: ' + str(mensagens[min_index]['id']) + ' temperatura ' + str(int(mensagens[min_index]['temperature'])) + ' e humidade ' + str(int(mensagens[min_index]['humidity'])) + ' que teve uma diferença de ' + str(diferenca[min_index]) + ' segundos')


if __name__ == '__main__':
    main()
