from dynamo import Dynamo


pub_table = Dynamo('iot-PublisherAutoScaling-1BWKDK9FTEXSR-DynamoDBTable-ARTMMEFZ2HG7')
sub_table = Dynamo('iot-SubscriberAutoScaling-W2WTEXV0C7KF-DynamoDBTable-10D64PNSPKUD')


pub_table_items =  pub_table.scan_messages()
sub_table_items =  sub_table.scan_messages()