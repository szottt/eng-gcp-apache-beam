import csv
import time
from google.cloud import pubsub_v1
import keys as keys
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keys.serviceAccount

subscription = 'projects/curso-dataflow-beam-399318/subscriptions/MeusVoos-sub'
subscriber = pubsub_v1.SubscriberClient()

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack()

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)