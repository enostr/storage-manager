#!/usr/bin/env python3

import subprocess
import os

# Define variables
mapper_name = "cryptdisk"
mount_point = "/home/mtx003/data"

# Function to check if the partition is mounted
def is_mounted(mount_point):
    try:
        # Check if the mount point is in use
        result = subprocess.run(['mount'], capture_output=True, text=True)
        return mount_point in result.stdout
    except Exception as e:
        print(f"Error checking if mounted: {e}")
        return False

# Function to run shell commands with subprocess
def run_command(command):
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error: {e}")

# Check if the partition is mounted
if is_mounted(mount_point):
    print(f"Unmounting the partition at {mount_point}...")
    run_command(['sudo', 'umount', mount_point])
else:
    print(f"The partition is not mounted at {mount_point}.")

# Close the encrypted partition
print(f"Closing the encrypted partition {mapper_name}...")
run_command(['sudo', 'cryptsetup', 'close', mapper_name])

print(f"Encrypted partition {mapper_name} closed successfully.")

