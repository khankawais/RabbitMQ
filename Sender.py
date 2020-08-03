import sys
import pika


credentials = pika.PlainCredentials('admin', 'root')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='awais', durable=True)

message = ''.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='',routing_key="awais",body=message,properties=pika.BasicProperties(delivery_mode = 2,))
print(" [x] Sent %r" % message)