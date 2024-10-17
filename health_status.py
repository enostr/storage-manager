import os
import subprocess
import pika
import json
import logging
import time
from datetime import datetime
import sqlite3
# from datetime import datetime
from kombu import Connection, Queue, Producer, Exchange


# Configure the logger
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[
        logging.FileHandler('./logs/health_status_kombu.log'),  # Log messages to a file
        logging.StreamHandler() # Also log to console
    ]
)

# Create a logger object
logger = logging.getLogger(__name__)
# logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("sqlite3").setLevel(logging.WARNING)

#KOMBU connection here

# Define the exchange and queue
exchange = Exchange('vms.main.exchange', type='direct')
queue = Queue('bahrain.device.monitoring.queue', exchange, routing_key='bahrain.device.monitoring.queue.key', durable=True)

# Connection parameters
connection_params = {
    'hostname': '202.88.232.230', #'192.168.134.117',
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
                routing_key='bahrain.device.monitoring.queue.key',
                headers={"__TypeId__": "in.trois.bahrain.poc.dto.fr.BahrainDeviceMonitoringDto"},
                content_type='application/json',
                delivery_mode=2  # Make the message persistent
            )
            logger.info("Message published successfully.")
        return True

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        return False

# # RabbitMQ setup
# def connect_rabbitmq():
#     try:
#         connection = pika.BlockingConnection(pika.ConnectionParameters(
#             '103.73.188.228', 45701, '/', pika.PlainCredentials('user', 'zSfC5GT2NWZdLxeR')))
#         channel = connection.channel()
#         channel.queue_declare(queue='bahrain.detection.queue', durable = True)
#         return channel
#     except Exception as e:
#         print(f"RabbitMQ connection failed: {e}")
#         return None

# def send_payload_to_rabbitmq(channel, payload):
#     try:
#         payload_str = payload

#         # Send payload with images to RabbitMQ
#         channel.basic_publish(exchange='vms.main.exchange',
#                               routing_key='bahrain.detection.queue.key',
#                               body=payload_str)
#         return True

#     except json.JSONDecodeError as e:
#         print(f"Error decoding JSON payload: {e}")
#         return False
#     except Exception as e:
#         print(f"Failed to send payload to RabbitMQ: {e}")
#         return False

def get_device_id(device):
    if device == 'FR':
        device_id = 1
        return device_id
    elif device == 'RLVDS':
        device_id = 2
        return device_id
    elif device == 'SVDS':
        device_id = 3
        return device_id

def get_folder_size(mount_point):
    
    try:
        # Run the 'du' command to get total size of the directory
        result = subprocess.run(['du', '-sb', mount_point], capture_output=True, text=True)
        
        # Output format: "size_in_bytes directory_path"
        size_str = result.stdout.split()[0]
        
        # Convert size from string to integer (bytes)
        folder_size = int(size_str)
        
        return folder_size
    except Exception as e:
        print(f"Error getting folder size: {e}")
        return None

# Function to get the total available space on the file system
def get_total_space(mount_point):
    
    try:
        # Use os.statvfs() to get file system statistics
        stat = os.statvfs(mount_point)
        
        # Calculate total size in bytes (block size * total blocks)
        total_space = stat.f_blocks * stat.f_frsize
        
        return total_space
    except Exception as e:
        print(f"Error getting total space: {e}")
        return None

def get_process_uptime(process_name):
    """Retrieve the uptime of a process by its name."""
    # Command to get PID, uptime, and command of the process
    command = f"ps -eo pid,etime,comm | grep {process_name} | grep -v grep"
    
    # Execute the command and capture the output
    with os.popen(command) as stream:
        output = stream.read().strip()

    # Check if there is output
    if output:
        # Split the output into columns (PID, Uptime, Command)
        columns = output.split()
        
        # Ensure we have enough columns to parse
        if len(columns) >= 3:
            # The second column (etime) contains the uptime in the format HH:MM or D-HH:MM:SS
            uptime = columns[1]
            return uptime
        
    # If no process is found or an error occurs
    return "Process not found."

def get_offline_data_percentage(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # Get the total count of records in the 'payloads' table
        cur.execute("SELECT COUNT(*) FROM payloads")
        total_count = cur.fetchone()[0]

        # Get the count of records with status 'HISTORY'
        cur.execute("SELECT COUNT(*) FROM payloads WHERE status = 'HISTORY'")
        history_count = cur.fetchone()[0]

        # Calculate the percentage of 'HISTORY' records
        if total_count == 0:
            return 0.0  # To avoid division by zero

        percentage = (history_count / total_count) * 100
        return round(percentage, 2)  # Round to two decimal places

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None

    finally:
        cur.close()
        conn.close()

def device_uptime():

    # Read the uptime in seconds from /proc/uptime
    with open('/proc/uptime', 'r') as f:
        uptime_seconds = float(f.readline().split()[0])

    # Convert seconds to hours, minutes, and seconds
    hours = int(uptime_seconds // 3600)
    minutes = int((uptime_seconds % 3600) // 60)
    seconds = int(uptime_seconds % 60)

    # Return formatted uptime as "HH:MM:SS"
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def check_camera_status():
    return 'online'

def check_ir_status():
    return 'on'

def permanent_storage_used(mount_point):

    # Get folder size and total space
    folder_size = get_folder_size(mount_point)
    total_space = get_total_space(mount_point)

    # Calculate and print the percentage of used space in the directory
    if folder_size is not None and total_space is not None:
        percentage_used = (folder_size / total_space) * 100
        # print(f"Data usage in {mount_point}: {round(percentage_used, 2)}%")
        return round(percentage_used, 2)
    else:
        print(f"Failed to calculate data usage for {mount_point}")

def main():

    # channel =  connect_rabbitmq()

    today_date = datetime.now().strftime("%d-%m-%y")
    db_name = f'/home/mtx003/data/database_records/videologs_{today_date}.db'

    mount_point = "/home/mtx003/data"

    # db_path = '/home/mtx003/data/videologs.db'

    while True:

        ####payload structure for health packet
        health_payload = {
            "device_id": get_device_id('FR'),
            "camera_1": check_camera_status(),
            "camera_2": check_camera_status(),
            "ai_program_uptime": get_process_uptime('P2_client'),  # Format: "HH:MM:SS"
            "total_device_uptime": device_uptime(),  # Format: "HH:MM:SS"
            "input_voltage": 12.5,  # Voltage in Volts (e.g., 12.5)
            "battery_voltage": 10.1,  # Optional, if applicable
            "IR_status": check_ir_status(),
            "permanent_storage_used": permanent_storage_used(mount_point),  # Percentage (0-100)
            "offline_data_to_sync": get_offline_data_percentage(db_name),  # Percentage (0-100)
            "health_time" : int(time.time()), #1701676735,
            "latitude" : 13.726045,
            "longitude" : 75.166692
        }
        
        publish_message(health_payload)

        time.sleep(10)

if __name__ == "__main__":
    main()