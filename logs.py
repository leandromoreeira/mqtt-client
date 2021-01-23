import boto3
from botocore.exceptions import ClientError

class Logs(object):


  def __init__(self, log_group, log_stream):
    self.client = boto3.client('logs')
    self.log_group = log_group
    self.log_stream = log_stream
    self.create_log_group()
    self.create_log_stream()
    self.sequence_token = self.describe_log_streams()


  def create_log_group(self):
    try:
      self.client.create_log_group(
          logGroupName=self.log_group,
      )
    except ClientError as e:
      if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
        print("Log Group já existente")
      else:
        print("Unexpected error: %s" % e)


  def create_log_stream(self):
    try:
      self.client.create_log_stream(
          logGroupName=self.log_group,
          logStreamName=self.log_stream
      )
    except ClientError as e:
      if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
        print("Log Stream já existente")
      else:
        print("Unexpected error: %s" % e)


  def put_log_event(self,timestamp,message):
    for i in range(10):
      try:
        if self.sequence_token == None:
          response = self.client.put_log_events(
              logGroupName=self.log_group,
              logStreamName=self.log_stream,
              logEvents=[
                  {
                      'timestamp': timestamp,
                      'message': message
                  },
              ],
          )
        else:
          response = self.client.put_log_events(
              logGroupName=self.log_group,
              logStreamName=self.log_stream,
              logEvents=[
                  {
                      'timestamp': timestamp,
                      'message': message
                  },
              ],
              sequenceToken=self.sequence_token
          )
        self.sequence_token = response['nextSequenceToken']
      except ClientError as e:
        print("Falha ao gravar o log, tentando mais " + str(10-i) + " vezes")
        if e.response['Error']['Code'] == 'InvalidSequenceTokenException':
          self.sequence_token = self.describe_log_streams()
        else:
          print("Unexpected error: %s" % e)
      else:
        print("Log inserido com sucesso!")
        break


  def describe_log_streams(self):
    try:
      response = self.client.describe_log_streams(
          logGroupName=self.log_group,
          logStreamNamePrefix=self.log_stream,
          orderBy='LogStreamName',
          limit=1
      )
      if 'uploadSequenceToken' in response['logStreams'][0]:
        return response['logStreams'][0]['uploadSequenceToken']
      else:
        return None
    except ClientError as e:
      print("Unexpected error: %s" % e)
      return None