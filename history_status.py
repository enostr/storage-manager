import sqlite3
import time
import pika
import sys
import json
import base64
import logging

# # Function to close and restart RabbitMQ connection
# def restart_rabbitmq_connection(rabbitmq_channel):
#     try:
#         # Close existing RabbitMQ connection if it exists
#         if rabbitmq_channel and rabbitmq_channel.connection:
#             rabbitmq_channel.connection.close()
#             logger.info("RabbitMQ connection closed.")
#     except Exception as e:
#         logger.error(f"Error closing RabbitMQ connection: {e}")
    
#     # Reconnect to RabbitMQ in a loop until successful
#     logger.info("Attempting to reconnect to RabbitMQ...")
#     rabbitmq_channel = connect_rabbitmq()  # Reconnect
#     return rabbitmq_channel

# RabbitMQ setup
def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            '192.168.134.117', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR'))) #'103.73.188.228'
        channel = connection.channel()
        channel.queue_declare(queue='bahrain.detection.ai.testing', durable=True)
        return channel
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {e}")
        return None

# Function to connect to RabbitMQ with retry mechanism
def connect_rabbitmq():
    logger.info("Connecting to RabbitMQ...")
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                '192.168.134.117', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR')))
            channel = connection.channel()
            channel.queue_declare(queue='bahrain.detection.ai.testing', durable=True)
            logger.info("Successfully connected to RabbitMQ.")
        except Exception as e:
            logger.error(f"RabbitMQ connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)  # Wait for 5 seconds before trying again
    return channel

# Function to send message to RabbitMQ
def send_to_rabbitmq(channel, message):
    try:
        channel.basic_publish(exchange='vms.main.exchange', routing_key='bahrain.detection.queue.key', body=message)
        return True
    except Exception as e:
        logger.error(f"Failed to send message to RabbitMQ: {e}")
        restart_rabbitmq_connection()
        return False

# Function to update status in the SQLite database for payloads
def update_payload_status(conn, record_id, new_status):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE payloads SET status = ? WHERE id = ?", (new_status, record_id))
        conn.commit()
        logger.info(f"Updated payload record {record_id} to status {new_status}")
    except Exception as e:
        logger.error(f"Failed to update payload status in SQLite: {e}")

# Function to update status in the SQLite database
def update_status(conn, record_id, new_status):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE videos SET status = ? WHERE id = ?", (new_status, record_id))
        conn.commit()
        logger.info(f"Updated record {record_id} to status {new_status}")
    except Exception as e:
        logger.error(f"Failed to update status in SQLite: {e}")

# # Function to encode video in base64
# def encode_video_to_bytes(video_path):
#     with open(video_path, 'rb') as video_file:
#         return base64.b64encode(video_file.read()).decode('utf-8')

# Function to encode video in base64
def encode_video_to_bytes(video_path):
    try:
        with open(video_path, 'rb') as video_file:
            logging.info(f"Successfully opened {video_path}")
            return base64.b64encode(video_file.read()).decode('utf-8')
    except FileNotFoundError:
        logging.error(f"File not found: {video_path}")
        return None  # Or raise an exception, or handle as needed
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None  # Or handle the exception as needed

# Function to check the SQLite database for records with status 'HISTORY' in the payloads table
def check_and_send_payloads(conn, rabbitmq_channel):

    logger.info("===== METADATA PAYLOADS =====")

    cursor = conn.cursor()
    cursor.execute("SELECT id, status FROM payloads WHERE status = 'HISTORY'")
    records = cursor.fetchall()

    for record in records:
        record_id = record[0]
        payload = record[1]

        if rabbitmq_channel is None:
            rabbitmq_channel = connect_rabbitmq()

        if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, str(payload)):
            update_payload_status(conn, record_id, 'STORAGE')
            logger.info(f"Status Updates = {record_id} to STORAGE")
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Function to check the SQLite database for records with status 'HISTORY' in the videos table
def check_and_send_videos(conn, rabbitmq_channel):

    logger.info("===== VIDEO PAYLOADS =====")

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, videopath FROM videos WHERE status = 'HISTORY'")
        records = cursor.fetchall()
    except sqlite3.OperationalError as e:
        logger.error("Database error: %s", e)
        return  # Exit the function if the table does not exist or an error occurs

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
        logger.info(payload_str)

        if rabbitmq_channel is None:
            rabbitmq_channel = connect_rabbitmq()

        if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, payload_str):
            update_status(conn, record_id, 'STORAGE')
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Main function to monitor the database at regular intervals
def monitor_db():
    try:
        logger.info("Trying to connect to db")
        conn = sqlite3.connect('/home/mtx003/data/videologs.db')
    except sqlite3.Error as e:
        logger.error(f"SQLite connection failed: {e}")
        sys.exit(1)

    rabbitmq_channel = connect_rabbitmq()

    try:
        while True:
            check_and_send_payloads(conn, rabbitmq_channel)
            check_and_send_videos(conn, rabbitmq_channel)
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        logger.error("Program terminated.")
    finally:
        if conn:
            conn.close()
        if rabbitmq_channel and rabbitmq_channel.connection:
            rabbitmq_channel.connection.close()

if __name__ == "__main__":

    # Configure the logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
        handlers=[
            logging.FileHandler('./logs/history_status.log'),  # Log messages to a file
            logging.StreamHandler()
        ]
    )

    # Create a logger object
    logger = logging.getLogger(__name__)
    # logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)
    logging.getLogger("watchdog").setLevel(logging.WARNING)

    monitor_db()
