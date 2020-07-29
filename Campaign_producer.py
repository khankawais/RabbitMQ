import mysql.connector
from mysql.connector import Error
import sys
import pika
import pickle

credentials = pika.PlainCredentials('admin', 'root')

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',credentials))
    channel = connection.channel()
    channel.queue_declare(queue='hello')

    try:

        connection = mysql.connector.connect(host='localhost',
                                            database='campaign',
                                            user='root',
                                            password='root')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            # cursor.execute("select * from customers where name='awais'")
            cursor.execute(sys.argv[1])
            Result=cursor.fetchall()
            
            if len(Result) == 0:
                print("Query Returned Nothin ! --- Nothing to Publish ------")
            else:
                for result in Result:
                    id_num=result[0]
                    name=result[1]
                    phone_number=result[2]
                    email=result[3]
                    process_id=result[4]
                    process_time=result[5]

                    string_to_send=f'"id":"{id_num}","name":"{name}","phone_number":"{phone_number}","email":"{email}","process_id":"{process_id}","process_time":"{process_time}"'
                    dictionary={"id":id_num,"name":name,"phone_number":phone_number,"email":email,"process_id":process_id,"process_time":process_time}
                    bytes_to_send=pickle.dumps(dictionary)
                    channel.basic_publish(exchange='', routing_key='hello', body=bytes_to_send)
    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

except:
    print("Error while connecting to RabbitMQ Server !!")