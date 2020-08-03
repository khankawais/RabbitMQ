import time
import pika
import _thread
import sys
from logger import genlog

def Help():
   print("   --------------------------------------------------------------------------------------------------")
   print("   |                                                                                                |")
   print("   |    This Python Script is used to consume messages from RabbitMQ queue with multi threading     |")   
   print("   |                                                                                                |")
   print("   |    Syntax :  python3 Reciever.py [ # of Threads ]                                              |")
   print("   |                                                                                                |")
   print("   |    Example : python3 Reciever.py 3                                                             |")
   print("   |                                                                                                |")
   print("   |    Written By : Awais Khan                                                                     |")
   print("   |                                                                                                |")
   print("   --------------------------------------------------------------------------------------------------")

def callback(ch, method, properties, body):
    # print(" [x] Received %r" % body)
    genlog.info("[x] Received "+body.decode("utf-8"))
    # time.sleep(1)
    f=open("log.txt","a")
    f.write(body.decode("utf-8")+"\n")
    ch.basic_ack(delivery_tag = method.delivery_tag)

def consumer(t_no):
    credentials = pika.PlainCredentials('admin', 'root')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',credentials))
    genlog.info("Connected to RabbitMQ Server")
    channel = connection.channel()
    channel.queue_declare(queue='awais', durable=True)
    genlog.debug("Created a durable queue")
    channel.basic_qos(prefetch_count=1)
    genlog.debug("Setting the prefetch (1)")

    genlog.info("Started Consuming from thread # "+ t_no)
    channel.basic_consume(queue='awais', on_message_callback=callback)
    channel.start_consuming()


if len(sys.argv) < 2:
    Help()
else:
    f=open("log.txt","w") # For Local logs of messages consumed
    f.close()             #
    for i in range(1,(int(sys.argv[1])+1),1):
        _thread.start_new_thread(consumer, (str(i),))
        genlog.info("Initialted Thread # "+ str(i) +" for consuming ")
    loop=True
    while loop:
        a=input("Write q and press Enter to (quit) > ")
        if a=="q":
            loop=False