import sqlite3
import time
import sys
import json
import base64
import logging
import os
import cv2
from kombu import Connection, Queue, Producer, Exchange
from datetime import datetime
import numpy as np
import traceback

#KOMBU connection here

# Define the exchange and queue
exchange = Exchange('vms.main.exchange', type='direct')
queue = Queue('bahrain.detection.ai.testing', exchange, routing_key='bahrain.detection.queue.key', durable=True)

# Connection parameters
connection_params = {
    'hostname': '192.168.134.117',
    'port': 45701,
    'userid': 'user',
    'password': 'zSfC5GT2NWZdLxeR',
    'virtual_host': '/',
    'heartbeat': 60,  # Set heartbeat to 60 seconds
}

def publish_message(payload):
    try:
        # Convert payload to JSON string
        payload_str = json.dumps(payload)
        logger.debug(f"Payload string to send: {payload_str}")

        # Create the connection URL with heartbeat
        connection_url = f"amqp://{connection_params['userid']}:{connection_params['password']}@{connection_params['hostname']}:{connection_params['port']}/{connection_params['virtual_host']}?heartbeat={connection_params['heartbeat']}"
        
        # Establish a connection and publish the message
        with Connection(connection_url) as conn:
            logger.info("Connection established successfully.")
            # Create a producer
            producer = Producer(conn)

            # Publish the message with properties
            producer.publish(
                payload_str,
                exchange=exchange,
                routing_key='bahrain.detection.queue.key',
                headers={"__TypeId__": "in.trois.bahrain.poc.request.payload.BahrainDetectionsRequestPayload"},
                content_type='application/json',
                delivery_mode=2  # Make the message persistent
            )
            logger.info("Message published successfully.")
            time.sleep(3)
        return True

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
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
def check_and_send_payloads(conn):

    logger.info("===== METADATA PAYLOADS =====")

    cursor = conn.cursor()
    cursor.execute("SELECT id, status, data FROM payloads WHERE status = 'HISTORY'")
    records = cursor.fetchall()
    # logger.info(f'{records}')
    
    for record in records:
        record_id = record[0]
        payload = record
        data = record[2]

        # if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, str(payload)):
        if publish_message(payload):
            update_payload_status(conn, record_id, 'STORAGE')
            logger.info(f"Status Updates = {record_id} to STORAGE")
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Function to check the SQLite database for records with status 'HISTORY' in the videos table
def check_and_send_videos(conn):

    logger.info("===== VIDEO PAYLOADS =====")

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, videopath FROM videos WHERE status = 'STORAGE'")
        records = cursor.fetchall()
    except sqlite3.OperationalError as e:
        logger.error("Database error: %s", e)
        return  # Exit the function if the table does not exist or an error occurs
    
    print(records)

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

        # if rabbitmq_channel and send_to_rabbitmq(rabbitmq_channel, payload_str):
        if publish_message(video_payload):
            update_status(conn, record_id, 'STORAGE')
            logger.info(f"Video Payload Sent {record_id} from {video_path}")
        else:
            time.sleep(5)  # Wait for 5 seconds before trying again

# Main function to monitor the database at regular intervals
def monitor_db():

    today_date = datetime.now().strftime("%d-%m-%y")
    db_name = f'./database_records/videologs_{today_date}.db'


    try:
        logger.info("Trying to connect to db")
        conn = sqlite3.connect(db_name)
    except sqlite3.Error as e:
        logger.error(f"SQLite connection failed: {e}")
        sys.exit(1)

    try:
        while True:
            check_and_send_payloads(conn)
            check_and_send_videos(conn)
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        logger.error("Program terminated.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":

    # Configure the logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
        handlers=[
            logging.FileHandler('./logs/history_status_kombu.log'),  # Log messages to a file
            logging.StreamHandler()
        ]
    )

    # Create a logger object
    logger = logging.getLogger(__name__)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)

    monitor_db()
