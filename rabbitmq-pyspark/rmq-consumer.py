import pika
import json
import socket

# Fungsi untuk membuat koneksi RabbitMQ
def create_rabbitmq_connection():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
    channel = connection.channel()
    return channel

# Fungsi callback untuk memproses pesan dari RabbitMQ
def callback(ch, method, properties, body):
    try:
        # Parse pesan JSON
        data = json.loads(body)
        
        # Kirim data ke socket server
        send_to_socket_server(data)

    except Exception as e:
        print(f"Error processing message: {e}")

# Fungsi untuk mengirim data ke socket server
def send_to_socket_server(data):
    # Konversi data ke format yang sesuai
    message = json.dumps(data)
    
    # Kirim data ke socket server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(('localhost', 9999))
            s.sendall(message.encode())
        except Exception as e:
            print(f"Error sending data to socket server: {e}")

# Membuat koneksi RabbitMQ
channel = create_rabbitmq_connection()

# Mendeklarasikan antrian RabbitMQ dan mengikat fungsi callback
queue_name = 'sensor_data_queue'
channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange='sensor_data', queue=queue_name, routing_key='sensor_data.*')
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

# Memulai konsumsi pesan dari RabbitMQ
print(' [*] Waiting for sensor data. To exit press CTRL+C')
channel.start_consuming()
