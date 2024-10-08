import time
import os
import sqlite3
import json
import base64
from datetime import datetime
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kombu import Connection, Queue, Producer, Exchange
import pika
import logging
from logging.handlers import RotatingFileHandler

class DatabaseManager:
    """Handles database creation, saving, and updating."""

    def __init__(self, db_path):
        """Initialize the database connection and create tables."""
        self.db_path = db_path
        self.create_tables()

    def create_connection(self):
        """Create a new database connection."""
        return sqlite3.connect(self.db_path)

    def create_tables(self):
        """Create the necessary tables if they do not already exist."""
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id TEXT,
                    object_id TEXT,
                    date TIMESTAMP,
                    status TEXT,
                    videopath TEXT
                )
            ''')
    
    def insert_videorecords(self, records):
        """Insert videorecords records into the database."""
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.executemany('''
                INSERT INTO videos (image_id, object_id, date, status, videopath) 
                VALUES (?, ?, ?, ?, ?)
            ''', records)
            conn.commit()
            logger.info('Updated Video DB')

    def update_video_status(self, v_id, status):
        """Update the status of a video in the database."""
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute('''
                UPDATE videos
                SET status = ?
                WHERE image_id = ? AND status = 'LIVE'
            ''', (status, v_id))
            conn.commit()

    def close(self):
        """No persistent connection to close."""
        pass

# def connect_rabbitmq():
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters(
#             '103.73.188.228', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR')))
#         channel = connection.channel()
#         channel.queue_declare(queue='bahrain.detection.queue', durable=True)
#         return channel
#     except Exception as e:
#         logger.info(f"RabbitMQ connection failed: {e}")
#         return None

def encode_video_to_bytes(video_path):
    with open(video_path, 'rb') as video_file:
        return base64.b64encode(video_file.read()).decode('utf-8')

# def send_payload_to_rabbitmq(channel, payload):
#     try:
#         message_properties = pika.BasicProperties(
#             headers={"__TypeId__": "in.trois.bahrain.poc.request.payload.BahrainDetectionsRequestPayload"},
#             content_type='application/octet-stream',
#             delivery_mode=2
#         )
        
#         payload_str = json.dumps(payload)

#         # Send payload with images to RabbitMQ
#         channel.basic_publish(exchange='vms.main.exchange',
#                               routing_key='bahrain.detection.queue.key',
#                               body=payload_str,
#                               properties=message_properties)
#         return True
#     except json.JSONDecodeError as e:
#         logger.info(f"Error decoding JSON payload: {e}")
#         return False
#     except Exception as e:
#         logger.info(f"Failed to send payload to RabbitMQ: {e}")
#         return False

def publish_message(payload):
    try:
        # Convert payload to JSON string
        payload_str = json.dumps(payload)

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
        return True

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        return False

class MP4Handler(FileSystemEventHandler):
    def __init__(self, db_manager):
        self.db_manager = db_manager
        # self.channel = channel

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.mp4'):
            file_name = os.path.basename(event.src_path)
            logger.info(f"New MP4 file detected: {file_name}")

            time.sleep(1)  # Adjust this delay if necessary

            self.vid_payload(event.src_path, file_name)

    def vid_payload(self, video_path, file_name):
        try:
            
            # Split the filename into parts
            parts = file_name.split('__')

            # Check if the filename has the expected number of parts
            if len(parts) < 2:
                logger.error(f"Filename format not as expected: {file_name}")
                return

            # Split the second part to extract image_id and source_id
            image_id = parts[1].split('_')[0]  # frame no
            source_id = parts[1].split('_')[1]  # mtx or ptz
            object_id = parts[1].split('_')[2]  # object id

            # Video Payload
            record2 = {
                "device_id": 1,
                "frame_no": image_id,
                "object_id": object_id,
                "stream_id": source_id,
                "event_video": encode_video_to_bytes(video_path),
                "event_id": 23
            }

            video_payload_for_db = [(record2['frame_no'], object_id, datetime.now(), 'LIVE', video_path)]
            self.db_manager.insert_videorecords(video_payload_for_db)

            if publish_message(record2):
                status = 'STORAGE'
                logger.info(f"Payload sent for video: {file_name}")
            else:
                status = 'HISTORY'
                logger.error(f"Failed to send payload for video: {file_name}")
                
            db_manager.update_video_status(image_id, status)
            logger.info('DB STATUS UPDATED')

        except Exception as e:
            logger.error(f"Error processing video {file_name}: {e}")
        
        db_manager.close()


if __name__ == "__main__":
    
    # Create a rotating file handler
    handler = RotatingFileHandler(
        './logs/video_payloads_kombu.log', 
        mode='a',  # Append mode
        maxBytes=3 * 1024 * 1024,  # 3 MB size limit
        backupCount=10  # Optional: number of backup logs to keep
    )

    # Configure the logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
        handlers=[handler]
        
        # handlers=[
        #     logging.FileHandler('./logs/history_status_kombu.log'),  # Log messages to a file
        #     logging.StreamHandler()
        # ]
    )

    # Create a logger object
    logger = logging.getLogger(__name__)
    logging.getLogger("watchdog").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)
    logging.getLogger("subprocess").setLevel(logging.WARNING)


    today_date = datetime.now().strftime("%d-%m-%y")
    db_name = f'/home/mtx003/data/database_records/videologs_{today_date}.db'

    db_manager = DatabaseManager(db_name)  # Set the actual path to your database
    # channel = connect_rabbitmq()

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
    
    current_date = datetime.now().strftime("%d-%m-%Y")
    path = f"/home/mtx003/data/{current_date}/videos"
    
    while not os.path.exists(path):
        logger.info(f"Waiting for directory {path} to be created...")
        time.sleep(5)
    
    logger.info(f"Directory {path} exists. Starting observer.")
    event_handler = MP4Handler(db_manager)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        db_manager.close()
    
    observer.join()
