import subprocess
import os

def remove_autossh_service_file():
    service_file_path = '/etc/systemd/system/autossh-tunnel.service'
    if os.path.exists(service_file_path):
        os.remove(service_file_path)
        print(f"Removed service file: {service_file_path}")
    else:
        print(f"Service file does not exist: {service_file_path}")

def uninstall_autossh():
    try:
        # Stop and disable the autossh service
        subprocess.run(['sudo', 'systemctl', 'stop', 'autossh-tunnel.service'], check=True)
        subprocess.run(['sudo', 'systemctl', 'disable', 'autossh-tunnel.service'], check=True)
        print("Autossh Tunnel Service stopped and disabled.")
        
        # Remove the systemd service file
        remove_autossh_service_file()

        # Optionally, remove autossh package
        # Uncomment the following line if you want to remove autossh package
        # subprocess.run(['sudo', 'apt', 'remove', '--purge', '-y', 'autossh'], check=True)
        # print("Autossh package removed.")

        # Reload systemd to apply changes
        subprocess.run(['sudo', 'systemctl', 'daemon-reload'], check=True)
        print("Systemd daemon reloaded.")
        
    except subprocess.CalledProcessError as e:
        print(f"Error during uninstallation: {e}")

def main():
    print("Uninstalling Autossh Tunnel Service...")
    uninstall_autossh()

if __name__ == "__main__":
    main()

