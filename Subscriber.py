#!/usr/bin/env python
import pika
import sys
from config import mq_cred
import json
import requests

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

def callback(ch, method, properties, body):
    URL = "http://localhost:5000/api/of1"
    json_body = json.loads(body)
    payload = requests.post(URL, json=json.dumps(json_body))
    # print(" [x] %r:%r" % (method.routing_key, body))


while(True):
    try:
        print("Connecting...")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        exchange_name = 'patient_data'
        channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

        result = channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        
        binding_keys = "#"

        if not binding_keys:
            sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
            sys.exit(1)
            

        for binding_key in binding_keys:
            channel.queue_bind(
                exchange=exchange_name, queue=queue_name, routing_key=binding_key)

        print(' [*] Waiting for logs. To exit press CTRL+C')

        


        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            connection.close()
            break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment the break to not recovery from server-initiated closure
            # break
            continue

    # Do not recover on channel errors
    except pika.exceptions.AMQPChannelError as err:
        print(f"Caught channel error {err}, stopping...")
        break

    # Recover on all other connection issues
    except pika.exceptions.AMQPConnectionError:
        print("No connection, attempting to reconnect...")
        continue

