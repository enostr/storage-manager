import os
#from io import ByetesIO
import json
from datetime import datetime
import cv2
import numpy as np
import zmq
import sqlite3
import pika
import time
import base64
import traceback

class DatabaseManager:
    """Handles database creation, saving, and updating."""

    def __init__(self, db_path):
        """Initialize the database connection and cursor."""
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        """Create the necessary tables if they do not already exist."""
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                v_id TEXT,
                event_id TEXT,
                violation TEXT,
                date TIMESTAMP,
                objectid INTEGER,
                camid TEXT,
                status TEXT,
                label TEXT,
                lane INTEGER,
                bbox TEXT,
                direction TEXT,
                speed TEXT,
                stopped_duration TEXT,
                hdhe TEXT,
                hdle TEXT,
                sdhe TEXT,
                sdle TEXT,
                videopath TEXT
            )
        ''')

        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS payloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                v_id TEXT,
                status TEXT,
                data TEXT
            )
        ''')

    def insert_violations(self, records):
        """Insert violations records into the database."""
        self.cur.executemany('''
            INSERT INTO violations (v_id, event_id, violation, date, objectid, camid, status, label, lane, bbox, direction, speed, stopped_duration, hdhe, hdle, sdhe, sdle, videopath) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        self.conn.commit()
        return self.cur.lastrowid

    def insert_payloads(self, payloads):
        """Insert payloads records into the database."""
        self.cur.executemany('''
            INSERT INTO payloads (v_id, status, data) 
            VALUES (?, ?, ?)
        ''', payloads)
        self.conn.commit()
        logger.info('Added to payload db ---')

    def update_payload_status(self, v_id, status):
        """Update the status of a payload in the database."""
        self.cur.execute('''
            UPDATE payloads
            SET status = ?
            WHERE v_id = ? AND status = 'LIVE'
        ''', (status, v_id))
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cur.close()
        self.conn.close()

class ReceiveData:
    """Class to receive data from a ZeroMQ socket.

    Attributes:
        context (zmq.Context): ZeroMQ context for socket communication.
        subscriber (zmq.Socket): A subscriber socket that receives data from the specified endpoint.
    """

    def __init__(self, endpoint, topic=""):
        """Initializes the ZeroMQ subscriber and connects to the given endpoint.

        Args:
            endpoint (str): The ZeroMQ endpoint to connect to.
            topic (str, optional): The topic to subscribe to. Defaults to an empty string for all topics.
        """
        self.context = zmq.Context()
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect(endpoint)
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
        logger.info(f"Connected to {endpoint} and waiting for messages on topic '{topic}'...")

    def get_data(self):
        """Attempts to receive data from the ZeroMQ subscriber.

        Returns:
            dict: The JSON object containing image data, or None if no data is available or an error occurs.
        """
        try:
            self.message = self.subscriber.recv(flags=zmq.NOBLOCK)  # Non-blocking call
            self.serialized_combined_data = self.message.decode('utf-8')
            images_json = json.loads(self.message)
            logger.info("#[storage manager] Got Data")

            return images_json

        except zmq.Again:  # No message available (non-blocking)
            # print("No message available (non-blocking)")
            time.sleep(0.1)
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error in receiving data: {e}")
            return None


class ImageStorage:
    """Class to manage saving HD and SD images to directories based on the current date.

    Attributes:
        base_directory (str): The base directory where images will be saved.
    """

    def __init__(self, base_directory="image_storage"):
        """Initializes the storage directory.

        Args:
            base_directory (str, optional): The base directory for storing images. Defaults to 'image_storage'.
        """
        self.base_directory = base_directory
        if not os.path.exists(self.base_directory):
            os.makedirs(self.base_directory)

    def save_images(self, hdhe_image_base64, hdle_image_base64):
        """Saves HD and SD images as .jpg files with timestamps in their respective directories.

        Args:
            hd_image_bytes (bytes): Byte data for the HD image.
            sd_image_bytes (bytes): Byte data for the SD image.

        Returns:
            bool: True if images were successfully saved, False otherwise.
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            day_directory = os.path.join(self.base_directory, today)

            # Create directories for HD and SD if they don't exist
            hdhe_directory = os.path.join(day_directory, "HDHE")
            hdle_directory = os.path.join(day_directory, "HDLE")
            os.makedirs(hdhe_directory, exist_ok=True)
            os.makedirs(hdle_directory, exist_ok=True)

            current_time = datetime.now()
            microsecond = current_time.microsecond
            millisecond = microsecond // 1000

            # Generate timestamp for image filenames
            timestamp = current_time.strftime("%d-%m-%Y_%H_%M_%S")
            timestamp = f"{timestamp}_{millisecond:03}"

            # Create filenames for HD and SD images with timestamp
            hdhe_filename = f"hd_he_{timestamp}.jpg"
            hdle_filename = f"hd_le_{timestamp}.jpg"

             # Decode Base64 strings to bytes
            hdhe_image_bytes = base64.b64decode(hdhe_image_base64)
            hdle_image_bytes = base64.b64decode(hdle_image_base64)

            # Convert byte arrays to images using OpenCV
            hdhe_image = cv2.imdecode(np.frombuffer(hdhe_image_bytes, np.uint8), cv2.IMREAD_COLOR)
            hdle_image = cv2.imdecode(np.frombuffer(hdle_image_bytes, np.uint8), cv2.IMREAD_COLOR)
            # print("hd_image.shape ==",hd_image.shape)
            # print("sd_image.shape ==",sd_image.shape)

            if hdhe_image is None or hdhe_image is None:
                logger.error("Failed to decode images.")
                return None, None

            # Save the images
            hdhe_image_path = os.path.join(hdhe_directory, hdhe_filename)
            hdle_image_path = os.path.join(hdle_directory, hdle_filename)
            cv2.imwrite(hdhe_image_path, hdhe_image,[cv2.IMWRITE_JPEG_QUALITY, 70])
            cv2.imwrite(hdle_image_path, hdle_image,[cv2.IMWRITE_JPEG_QUALITY, 70])
           # with open(os.path.join(hdhe_image_path), 'wb') as hd_file:
            #    hd_file.write(hdhe_image_base64)

           # with open(os.path.join(hdle_image_path), 'wb') as sd_file:
            #    sd_file.write(hdle_image_base64)

            # print(f"Images saved as {hd_image_path} and {sd_image_path}.")
            return hdhe_image_path, hdle_image_path
        except Exception as e:
            logger.error(f"Error saving images: {e}")
            return None, None

# RabbitMQ setup
def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            '202.88.232.230', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR')))
        channel = connection.channel()
        channel.queue_declare(queue='bahrain.detection.queue', durable = True)
        return channel
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {e}")
        return None

# Function to encode video in base64
def encode_video_to_bytes(video_path):
    with open(video_path, 'rb') as video_file:
        return base64.b64encode(video_file.read()).decode('utf-8') #video_file.read()

# Function to encode video in base64
def encode_image_to_bytes(image_path):
    with open(image_path, 'rb') as image_file:

        #data = image_file.read()
        #im = Image.open(BytesIO(data))

        return base64.b64encode(image_file.read()).decode('utf-8')
        
def scale_bounding_box(bbox, original_size=(1920, 1080), target_size=(3840, 2160)):
    """Scale bbox"""
    original_width, original_height = original_size
    target_width, target_height = target_size

    scale_x = target_width / original_width
    scale_y = target_height / original_height

    x_min, y_min, x_max, y_max = bbox
    scaled_bbox = (
        int(x_min * scale_x),
        int(y_min * scale_y),
        int(x_max * scale_x),
        int(y_max * scale_y)
    )

    return scaled_bbox

def send_payload_to_rabbitmq(channel, payload): #, hdhe_image_path, hdle_image_path):
    """
    Send payload to RabbitMQ with images from their file paths included in the message.

    Args:
        channel: RabbitMQ channel.
        payload: Payload data to send (in JSON format or dictionary).
        hd_image_path: Path to the saved HD image.
        sd_image_path: Path to the saved SD image.
    
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Debug print to check payload content
        # print(f"Payload before processing: {payload}")


        # # Read the HD and SD images from their respective paths
        # with open(hdhe_image_path, 'rb') as hdhe_file:
        #     hdhe_image_bytes = hdhe_file.read()

        # with open(hdle_image_path, 'rb') as hdle_file:
        #     hdle_image_bytes = hdle_file.read()

        # # print(payload)
        # # print(type(payload))

        # # # Base64 encode the images to include in the payload
        # payload + ", "
        # payload = payload + 'hdhe_image' + "'" + base64.b64encode(hdhe_image_bytes).decode('utf-8') + "' , " 
        # payload = payload + 'hdle_image' + "'" + base64.b64encode(hdle_image_bytes).decode('utf-8') + "' )" 

        message_properties = pika.BasicProperties(
                    headers={"__TypeId__":"in.trois.bahrain.poc.request.payload.BahrainDetectionsRequestPayload"},
                    content_type ='application/octet-stream',
                    delivery_mode = 2
                    )
        # # Convert payload back to JSON string for sending
        payload_str = json.dumps(payload)
        # payload_str = payload

        # Debug print to check payload string before sending
        # print(f"Payload string to send: {payload_str}")

        # Send payload with images to RabbitMQ
        channel.basic_publish(exchange='vms.main.exchange',
                              routing_key='bahrain.detection.queue.key',
                              body=payload_str,
                              properties = message_properties
                              )
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON payload: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send payload to RabbitMQ: {e}")
        return False

def main():

    # Configure the logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
        handlers=[
            logging.FileHandler('./logs/video_payloads.log'),  # Log messages to a file
            logging.StreamHandler()            # Also log to console
        ]
    )

    # Create a logger object
    logger = logging.getLogger(__name__)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)

    first_run = 0

    vehicle_list = ["CAR", "TRUCK", "BUS", "AUTO", "MINI_TRUCK", "MINI_BUS", "TWO_WHEELER" ]
        # 0: "PERSON_SB",
        # 0: "HELMET",
        # 0: "NO_PHONE",
        # 0: "NOTR",
        # 0: "BACK_HELMET",
        # 0: "BACK_NO_HELMET"

    violation_list = {
        2: "NO_HELMET",
        3: "PHONE",
        4: "TR",
        5: "PERSON_NSB",
        6: "PHONE_LEFT",
        7: "PHONE_RIGHT",
        8: "LANE_VIOLATION",
        9: "LANE_CHANGE",
        10: "WRONG_LANE",
        11: "DIRECTION_VIOLATION",
        12: "STOPPED_VEHICLE",
        14: "OVER_SPEED_VIOLATION",
        15: "SLOW_SPEED_VIOLATION",
        18: "RED_LIGHT_VIOLATION",
        19: "PEDESTRIAN",
        20: "FIRE",
        23: "BICYCLE",
        24: "ANIMAL",
        25: "ZIGZAG"
    }

    """Main function to continuously receive image data and save them to disk."""
    try:
        data_receiver = ReceiveData("ipc:///tmp/MTX_out")
        image_storage = ImageStorage("/home/mtx003/data")
        # SQLite database manager
        db_manager = DatabaseManager('/home/mtx003/data/videologs.db')

        channel =  connect_rabbitmq()

        while True:
            images_json = data_receiver.get_data()
          
            # print(images_json)
            # print("check out point1")
            if images_json is not None:
                logger.info("#[storage manager] got image json")

                try:
                    index_pos = images_json["index"]
                    # print(index_pos)

                    if 0 not in index_pos or 1 not in index_pos or len(images_json["index"]) < 2:
                        logger.warning("#[storage manager]   lost frames ")
                        continue
                    hdhe_image_base64 = images_json["images"][0]
                    hdle_image_base64 = images_json["images"][2]

                    payload = images_json["metadata"]

                    logger.info("#[storage manager] payload => ",payload, len(payload), images_json.keys())
                    

                    if payload != 'null':

                        #speed violation
                        # {"output":[{"image_id":"image361",
                        # "result":{"coords":"null","direction":"null","label":"null","lane":"null","objectid":"28_0","speed":"27","stoped_duration":"null"},"source_id":"mtx"}]}                        
                            
                        # Save images
                        hdhe_image_path, hdle_image_path = image_storage.save_images(hdhe_image_base64,hdle_image_base64)
                        if not hdhe_image_path or not hdle_image_path:
                            logger.error("#[storage manager] Image saving failed.")
                            continue
                        record_list = []

                        # for i in payload:
                        coords = payload['coords']
                        coords_scaled = scale_bounding_box(coords)
                        label = payload['label']

                        if label in vehicle_list:
                            match_id = [1] # [id for id, vehicle in vehicle_list.items() if vehicle == label]
                        elif label in violation_list.values():
                            match_id = [id for id, violation in violation_list.items() if violation == label]
                        else:
                            match_id = [1]
                        record = (
                            f'{images_json["ImageId"]}',
                            f'{match_id[0]}',
                            f'{label}_{payload["objectid"]}', 
                            datetime.now(), 
                            payload['objectid'], 
                            "MTXcam1", 
                            "HISTORY", 
                            label, 
                            payload['lane'], 
                            f"{coords_scaled}",
                            payload['direction'], 
                            payload['speed'], 
                            payload['stoped_duration'], 
                            hdhe_image_path, 
                            hdle_image_path, 
                            "./HDHE/", 
                            "./HDLE/",
                            "./videopath/"
                        )

                        if payload['speed'] != 'null':
                            logger.info('#[storage manager] - speed payload')
                            #SPEED
                            record2 = {
                                "device_id": 2,
                                #"fov_bright" : [],
                                #"fov_dark": [],                                
                                #"event_video" : [],
                                "frame_no"  : f'{payload[output]["image_id"]}',
                                "stream_id" : 1,
                                "object_id" : f'{payload["objectid"].split("_")[0]}',
                                "speed" : f'{payload[result]["speed"]}',
                                #"vehicle_class": '',
                                #"box_coord": [],
                                "detected_at": int(time.time()), #f'{datetime.now()}',
                                #"detection_confidence": 0,
                                "event_id": 22,
                                #"tracker_confidence": 0,           
                            }

                        elif label in violation_list.values():
                            logger.info('#[storage manager] - event payload')
                            #event
                            record2 = {
                                "device_id": 2,
                                "frame_no"  : int(images_json["ImageId"]),
                                "stream_id" : 1,
                                "object_id" : int(f'{payload["objectid"].split("_")[0]}'),
                                "speed" : 0,
                                "vehicle_class": '',
                                "box_coord": coords_scaled,
                                "detected_at": int(time.time()), #f'{datetime.now().strftime("%A, %B %d, %Y %I:%M %p")}',
                                "detection_confidence": 0.65478515625,
                                "event_id": match_id[0],  
                                "tracker_confidence": 1.0,           
                                "fov_bright" : encode_image_to_bytes(hdhe_image_path), #"fov image in bytes",
                                #"fov_dark": [0], #f'{encode_image_to_bytes(hdle_image_path)}',                                
                                "event_video" : encode_video_to_bytes("./sample_video/mtx.mp4")
                            }

                        elif label in vehicle_list:
                            logger.info('#[storage manager] - ANPR payload')
                            #ALL ANPR
                            record2 = {
                                "device_id": 2,
                                #"event_video" : [],
                                "stream_id" : 1,
                                "frame_no"  : int(images_json["ImageId"]),
                                "object_id" : int(f'{payload["objectid"].split("_")[0]}'),
                                "speed" : 0,
                                "vehicle_class": label,
                                "box_coord": coords_scaled,
                                "detected_at": int(time.time()), #f'{datetime.now()}',
                                "detection_confidence": 0.65478515625,
                                "event_id": match_id[0],
                                "tracker_confidence": 1.0,           
                                "fov_bright" : encode_image_to_bytes(hdhe_image_path), #[f'{encode_image_to_bytes(hdhe_image_path)}'], #"fov image in bytes",
                                "fov_dark": encode_image_to_bytes(hdle_image_path),                                
                            
                                }

                        record_list.append(record)
                        # Insert violation records into the database
                        last_violation_id = db_manager.insert_violations(record_list)

                        # Insert payloads
                        payload_record_list = [(last_violation_id, 'LIVE', json.loads(json.dumps(record2)))]#str(record)) for record in record2]

                        payload_record_for_db = [(last_violation_id, 'LIVE', json.dumps(record2))]
                        db_manager.insert_payloads(payload_record_for_db)
                        #print("#[storage manager]== jsondumps ", json.dumps(record2))
                        #print("#[storage manager]== record ", record2)

#                        with open(f'{images_json["ImageId"]}_file.json', 'w') as json_file:
#                           json.dump(record2, json_file)

                        # Specify the keys to print
                        keys_to_print = ["device_id", "frame_no", "stream_id", "object_id", "speed", "vehicle_class", "box_coord", "detected_at", "detection_confidence", "event_id", "tracker_confidence" ]

                        # Print only the specified keys
                        #print("----------"*80)
                        #for key in keys_to_print:
                        #    if key in record2:
                        #        print(f"{key}: {record2[key]}")

                        #print("----------"*80)

                        # Attempt to send to RabbitMQ and update status
                        for record in payload_record_list:
                            payload_str = record[2]  # Extracting the 'data' field
                            if send_payload_to_rabbitmq(channel, payload_str): #, hdhe_image_path, hdle_image_path):
                                status = 'STORAGE'
                            else:
                                status = 'HISTORY'
                            
                            db_manager.update_payload_status(last_violation_id, status)
                            logger.info("#[storage manager]",f"Record updated with status {status} ---------------------------------------------------------------------------")
                    else:
                        continue
                    
                except KeyError as e:
                    logger.error("#[storage manager]",f"Key error: {e}. Ensure correct data format.")
                    logger.error("#[storage manager]",traceback.format_exc())
                except Exception as e:
                    logger.error("#[storage manager]",f"Unexpected error processing images: {e}")
                    logger.error("#[storage manager]",traceback.format_exc())

            else:
                # No data received, continue loop
                pass
        db_manager.close()

    except KeyboardInterrupt:
        logger.error("#[storage manager]","Process interrupted by user.")
    except zmq.ZMQError as e:
        logger.error("#[storage manager]",f"ZMQ error: {e}")
    except Exception as e:
        logger.error("#[storage manager]",f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    main()