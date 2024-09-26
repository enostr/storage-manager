#!/usr/bin/env python3

import subprocess
import os
# Define the mount point directory
mount_point = "/home/mtx003/data"

# Function to get the total size of files inside the directory using 'du' command
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

# Get folder size and total space
folder_size = get_folder_size(mount_point)
total_space = get_total_space(mount_point)

# Calculate and print the percentage of used space in the directory
if folder_size is not None and total_space is not None:
    percentage_used = (folder_size / total_space) * 100
    print(f"Data usage in {mount_point}: {round(percentage_used, 2)}%")
else:
    print(f"Failed to calculate data usage for {mount_point}")

