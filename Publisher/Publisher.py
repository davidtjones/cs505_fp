#!/usr/bin/env python
import pika


def pub(message):

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

    channel = connection.channel()

    exchange_name = 'patient_data2'

    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    routing_key='patient.info'

    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=message)
    print(" [x] Sent %r:%r" % (routing_key, message))

    connection.close()
