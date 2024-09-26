#!/usr/bin/env python3

import subprocess
import os

# Define variables
device = "/dev/sda1"
mapper_name = "cryptdisk"
mount_point = "/home/mtx003/data"
crypt_password = "123"  # Change this if you want a more secure password input method

# Function to check if the partition is already mounted
def is_mounted(mount_point):
    try:
        # Check if the mount point is already in use
        result = subprocess.run(['mount'], capture_output=True, text=True)
        return mount_point in result.stdout
    except Exception as e:
        print(f"Error checking if mounted: {e}")
        return False

# Function to create the mount point directory if it doesn't exist
def create_mount_point(mount_point):
    if not os.path.exists(mount_point):
        print(f"Mount point {mount_point} does not exist. Creating it...")
        os.makedirs(mount_point)
        print(f"Mount point {mount_point} created successfully.")
    else:
        print(f"Mount point {mount_point} already exists.")

# Function to run shell commands with subprocess
def run_command(command):
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error: {e}")

# Check if already mounted
if is_mounted(mount_point):
    print(f"The partition is already mounted at {mount_point}")
    exit(0)

# Create the mount point if it doesn't exist
create_mount_point(mount_point)

# Unlock the encrypted partition
print("Unlocking the encrypted partition...")
run_command(['echo', crypt_password, '|', 'sudo', 'cryptsetup', 'open', device, mapper_name])

# Mount the partition
print(f"Mounting the partition at {mount_point}...")
run_command(['sudo', 'mount', f'/dev/mapper/{mapper_name}', mount_point])

print(f"Partition mounted successfully at {mount_point}")

