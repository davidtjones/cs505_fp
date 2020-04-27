#!/usr/bin/env python
import pika
from config import mq_cred

def pub(message):

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"
    username = mq_cred['username']
    password = mq_cred['password']
    hostname = mq_cred['hostname']
    virtualhost = mq_cred['virtualhost']

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(hostname,
                                           5672,
                                           virtualhost,
                                           credentials)

    connection = pika.BlockingConnection(parameters)


    channel = connection.channel()

    exchange_name = 'patient_data'

    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    routing_key = 'patient.info'

    channel.basic_publish(
         exchange=exchange_name, routing_key=routing_key, body=message)
    print(" [x] Sent %r:%r" % (routing_key, message))

    connection.close()
