import boto3
from boto3.dynamodb.conditions import Key

class Dynamo(object):

  def __init__(self, table_name):
    self.dynamodb = boto3.resource('dynamodb')
    self.table = self.dynamodb.Table(table_name)

  def put_message(self,message):
    response = self.table.put_item(
        Item=message
    )
    return response

  def scan_messages(self):
    response = self.table.scan()
    return response['Items']
