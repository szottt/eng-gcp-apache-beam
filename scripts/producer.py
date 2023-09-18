#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import keys as keys
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keys.serviceAccount

topico = 'projects/curso-dataflow-beam-399318/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = r"/home/szot/projetos/eng/eng-gcp-apache-beam/files/voos_sample.csv"

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico,row)
        time.sleep(2)