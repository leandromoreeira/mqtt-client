from dynamo import Dynamo


pub_table = Dynamo('iot-PublisherAutoScaling-1BWKDK9FTEXSR-DynamoDBTable-ARTMMEFZ2HG7')
sub_table = Dynamo('iot-SubscriberAutoScaling-W2WTEXV0C7KF-DynamoDBTable-10D64PNSPKUD')


pub_table_items =  pub_table.scan_messages()
sub_table_items =  sub_table.scan_messages()



if len(pub_table_items) == len(sub_table_items):
  print("100\'%' das mensagens foram recebidas")
  for item in sub_table_items:

else:
  print("ouve perda de x\'%' das mensagens")

result = []

for pub_item in pub_table_items:
  falha = True
  for sub_item in sub_table_items:
    if pub_item['id'] == sub_item['id']:
      m = {
        'id' : pub_item['id'],
        'sent_time': pub_item['sent_time'],
        'received_time': sub_item['received_time']
      }
      falha = False
      sub_table_items.remove(sub_item)
      break
  if falha:
    m = {
      'id' : pub_item['id'],
      'fail': True,
    }