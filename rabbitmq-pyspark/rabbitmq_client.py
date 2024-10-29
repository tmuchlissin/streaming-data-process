import json
import pika
import mysql.connector
import socket


def subs_rabbitmq():
    credentials = pika.PlainCredentials('admin', 'admin')
    parameters = pika.ConnectionParameters(
        host='localhost', port=5672, virtual_host='smartsiklon_broker', credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='dummy_data')

    # Create a socket server 
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(1)
    print("Socket server listening on port 9999...")

    conn, addr = server_socket.accept()
    print('Connection address:', addr)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        conn.send(body)

    channel.basic_consume(queue='dummy_data',
                          on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    subs_rabbitmq()
 