import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class Dynamo(object):

  def __init__(self, table_name):
    self.dynamodb = boto3.resource('dynamodb')
    self.table = self.dynamodb.Table(table_name)

  def put_message(self,message):
    try:
      response = self.table.put_item(
          Item=message,
          ConditionExpression='attribute_not_exists(id)'
      )
      return response
    except:
      return None

  def scan_messages(self):
    response = self.table.scan()
    return response['Items']
