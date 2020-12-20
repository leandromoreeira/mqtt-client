import boto3
import argparse

# módulo utilizado para criação de instancias publisher na AWS

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--launch_template_name", help = "Nome do Launch Template com os parâmetros da instância")
parser.add_argument("-m", "--messages", help = "Quantidade de mensagens que deve ser eviada por conexão")
parser.add_argument("-s", "--seconds", help = "Tempo em segundos que o envio da mensagens deve ser feito")
parser.add_argument("-si", "--subnetid", help = "Tempo em segundos que o envio da mensagens deve ser feito")

launch_template_name = parser.parse_args().launch_template_name
messages = parser.parse_args().messages
seconds = parser.parse_args().seconds
subnetid = parser.parse_args().subnetid

messages_per_intance =  int(int(messages)/3)
print('Será enviado '+ str(messages_per_intance*3) + ' ao total')

client =  boto3.client('ec2')

response = client.run_instances(
  LaunchTemplate={
    'LaunchTemplateName': launch_template_name
  },
  MaxCount = 3,
  MinCount = 3,
  SubnetId = subnetid,
  TagSpecifications=[
    {
    'ResourceType' : 'instance',
    'Tags': [
      {
        'Key': 'messages',
        'Value': str(messages_per_intance)
      },
      {
        'Key': 'seconds',
        'Value': str(seconds)
      },
      {
        'Key': 'Name',
        'Value': 'publisher-instance'
      },
      {
        'Key': 'chaos',
        'Value': 'no'
      }
    ]
    }
  ]
)