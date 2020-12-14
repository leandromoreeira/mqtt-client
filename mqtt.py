import paho.mqtt.client as mqtt
import json

class Mqtt(object):

  def __init__(self, broker_host, broker_user, broker_passwd):
    self.client = mqtt.Client()
    self.client.username_pw_set(username=broker_user,password=broker_passwd)
    self.client.connect(broker_host)


  def publish(self,topic,message):
    self.client.publish(topic,message)


  def subscribe(self, topic):
    self.client.subscribe(topic)


  def loop_forever(self):
    self.client.loop_forever()