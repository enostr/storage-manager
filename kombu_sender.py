# from kombu import Connection, Queue, Producer, Exchange
# import json
# import logging
# import time
# import base64

# # Setup logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# # Create a logger for the application
# logger = logging.getLogger(__name__)

# # Set up Kombu logging
# kombu_logger = logging.getLogger('kombu')
# kombu_logger.setLevel(logging.DEBUG)  # Set Kombu log level to DEBUG

# # Add a console handler to Kombu logger
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# kombu_logger.addHandler(console_handler)

# # Define the exchange and queue
# exchange = Exchange('vms.main.exchange', type='direct')
# queue = Queue('bahrain.detection.ai.testing', exchange, routing_key='bahrain.detection.queue.key', durable=True)

# # Function to encode image in base64
# def encode_image_to_bytes(image_path):
#     try:
#         logger.debug(f"Encoding image at path: {image_path}")
#         with open(image_path, 'rb') as image_file:
#             encoded_bytes = base64.b64encode(image_file.read()).decode('utf-8')
#             logger.info(f"Image encoded successfully: {image_path}")
#             return encoded_bytes
#     except Exception as e:
#         logger.error(f"Failed to encode image: {image_path}, error: {e}")
#         return None

# def publish_message(payload):
#     try:
#         # Convert payload to JSON string
#         payload_str = json.dumps(payload)
#         logger.debug(f"Payload string to send: {payload_str}")

#         # Establish a connection and publish the message
#         with Connection('amqp://user:zSfC5GT2NWZdLxeR@192.168.134.117:45701//') as conn:
#             logger.info("Connection established successfully.")
#             # Create a producer
#             producer = Producer(conn)

#             # Publish the message with properties
#             producer.publish(
#                 payload_str,
#                 exchange=exchange,
#                 routing_key='bahrain.detection.queue.key',
#                 headers={"__TypeId__": "in.trois.bahrain.poc.request.payload.BahrainDetectionsRequestPayload"},
#                 content_type='application/json',
#                 delivery_mode=2  # Make the message persistent
#             )
#             logger.info("Message published successfully.")
#         return True

#     except Exception as e:
#         logger.error(f"Failed to publish message: {e}")
#         return False

# if __name__ == "__main__":
#     while True:
#         # Create a sample payload
#         payload = {
#             "device_id": 1,
#             "stream_id": 1,
#             "frame_no": int(2132),
#             "object_id": int(234),
#             "speed": 0,
#             "vehicle_class": "CAR",
#             "box_coord": [123, 23, 35, 45],
#             "detected_at": int(time.time()),
#             "detection_confidence": 0.65478515625,
#             "event_id": 0,
#             "tracker_confidence": 1.0,
#             "fov_bright": encode_image_to_bytes('/home/mtx003/data/2024-09-27/HDHE/hd_he_27-09-2024_15_44_52_045.jpg'),
#             "fov_dark": encode_image_to_bytes('/home/mtx003/data/2024-09-27/HDHE/hd_he_27-09-2024_15_44_52_045.jpg'),
#         }

#         publish_message(payload)
        
#         # Wait for a specified amount of time before sending the next message
#         time.sleep(5)  # Adjust the sleep duration as needed


# Function to encode image in base64
def encode_image_to_bytes(image_path):
    try:
        logger.debug(f"Encoding image at path: {image_path}")
        with open(image_path, 'rb') as image_file:
            encoded_bytes = base64.b64encode(image_file.read()).decode('utf-8')
            logger.info(f"Image encoded successfully: {image_path}")
            return encoded_bytes
    except Exception as e:
        logger.error(f"Failed to encode image: {image_path}, error: {e}")
        return None


from kombu import Connection, Queue, Producer, Exchange
import json
import logging
import time
import base64

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger for the application
logger = logging.getLogger(__name__)

# Set up Kombu logging
kombu_logger = logging.getLogger('kombu')
kombu_logger.setLevel(logging.DEBUG)  # Set Kombu log level to DEBUG

# Add a console handler to Kombu logger
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
kombu_logger.addHandler(console_handler)

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
        return True

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        return False

if __name__ == "__main__":
    while True:
        # Create a sample payload
        payload = {
            "device_id": 1,
            "stream_id": 1,
            "frame_no": int(2132),
            "object_id": int(234),
            "speed": 0,
            "vehicle_class": "CAR",
            "box_coord": [123, 23, 35, 45],
            "detected_at": int(time.time()),
            "detection_confidence": 0.65478515625,
            "event_id": 0,
            "tracker_confidence": 1.0,
            "fov_bright": encode_image_to_bytes('/home/mtx003/data/2024-09-27/HDHE/hd_he_27-09-2024_15_44_52_045.jpg'),  # Replace with actual encoding
            "fov_dark": encode_image_to_bytes('/home/mtx003/data/2024-09-27/HDHE/hd_he_27-09-2024_15_44_52_045.jpg'),    # Replace with actual encoding
        }

        publish_message(payload)
        
        # Wait for a specified amount of time before sending the next message
        time.sleep(5)  # Adjust the sleep duration as needed
