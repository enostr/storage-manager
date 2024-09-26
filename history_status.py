
import sqlite3
import time
import pika
import sys
import json
import base64

# RabbitMQ setup
def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            '103.73.188.228', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR')))
        channel = connection.channel()
        channel.queue_declare(queue='bahrain.detection.queue', durable=True)
        return channel
    except Exception as e:
        print(f"RabbitMQ connection failed: {e}")
        return None

# Function to send message to RabbitMQ
def send_to_rabbitmq(channel, message):
    try:
        channel.basic_publish(exchange='vms.main.exchange', routing_key='bahrain.detection.queue.key', body=message)
        print(f"Sent: < ========= ")
        return True
    except Exception as e:
        print(f"Failed to send message to RabbitMQ: {e}")
        return False

# Function to update status in the SQLite database
def update_status(conn, record_id, new_status):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE videos SET status = ? WHERE id = ?", (new_status, record_id))
        conn.commit()
        print(f"Updated record {record_id} to status {new_status}")
    except Exception as e:
        print(f"Failed to update status in SQLite: {e}")

# Function to encode video in base64
def encode_video_to_bytes(video_path):
    with open(video_path, 'rb') as video_file:
        return base64.b64encode(video_file.read()).decode('utf-8')

# Function to check the SQLite database for records with status 'HISTORY' in the payloads table
def check_and_send_payloads(conn, rabbitmq_channel):
    cursor = conn.cursor()
    cursor.execute("SELECT id, status FROM payloads WHERE status = 'HISTORY'")
    records = cursor.fetchall()

    for record in records:
        record_id = record[0]
        payload = record[1]

        if rabbitmq_channel is None:
            rabbitmq_channel = connect_rabbitmq()

        if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, payload):
            update_status(conn, record_id, 'STORAGE')
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Function to check the SQLite database for records with status 'HISTORY' in the videos table
def check_and_send_videos(conn, rabbitmq_channel):
    cursor = conn.cursor()
    cursor.execute("SELECT id, videopath FROM videos WHERE status = 'HISTORY'")
    records = cursor.fetchall()

    for record in records:
        record_id = record[0]
        video_path = record[1]

        # Create video payload
        image_id = video_path.split('__')[1].split('_')[0]  # Extracting image_id
        video_payload = {
            "device_id": 1,
            "frame_no": image_id,
            "stream_id": 1,
            "event_video": encode_video_to_bytes(video_path),
            "event_id": 30
        }

        # Send the payloads as a JSON string
        payload_str = json.dumps(video_payload)  # Convert dict to JSON string

        if rabbitmq_channel is None:
            rabbitmq_channel = connect_rabbitmq()

        if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, payload_str):
            update_status(conn, record_id, 'STORAGE')
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Main function to monitor the database at regular intervals
def monitor_db():
    try:
        conn = sqlite3.connect('videologs.db')
    except sqlite3.Error as e:
        print(f"SQLite connection failed: {e}")
        sys.exit(1)

    rabbitmq_channel = connect_rabbitmq()

    try:
        while True:
            check_and_send_payloads(conn, rabbitmq_channel)
            check_and_send_videos(conn, rabbitmq_channel)  # Check videos table as well
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        print("Program terminated.")
    finally:
        if conn:
            conn.close()
        if rabbitmq_channel and rabbitmq_channel.connection:
            rabbitmq_channel.connection.close()

if __name__ == "__main__":
    monitor_db()
