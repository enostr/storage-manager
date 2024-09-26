import os
import subprocess

def create_autossh_service_file(port_remote):
    username = 'mtx003'
    private_key_path = '/home/mtx003/.ssh/id_rsa'
    
    service_content = f"""
[Unit]
Description=Autossh Tunnel Service
After=network.target

[Service]
ExecStartPre=/usr/bin/ssh-agent -a /tmp/ssh-agent.sock
ExecStartPre=/usr/bin/ssh-add {private_key_path}
ExecStart=/usr/bin/autossh -M 0 -q -N -o "ServerAliveInterval 60" -o "ServerAliveCountMax 3" -i {private_key_path} trois@parkpro.live -R {port_remote}:localhost:22 -R 22:localhost:22
Environment="SSH_AUTH_SOCK=/tmp/ssh-agent.sock"
User={username}
Restart=always
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=autossh

[Install]
WantedBy=multi-user.target
    """
    
    service_file_path = '/etc/systemd/system/autossh-tunnel.service'
    with open(service_file_path, 'w') as file:
        file.write(service_content)
    
    print(f"Service file created at {service_file_path}")

def setup_autossh_service():
    print("Setting up Autossh Tunnel Service...")

    port_remote = input("Enter the remote port number for forwarding (e.g., 2244): ")

    # Creating the systemd service file
    create_autossh_service_file(port_remote)

    # Reload systemd and restart autossh service
    try:
        subprocess.run(['sudo', 'systemctl', 'daemon-reload'], check=True)
        subprocess.run(['sudo', 'systemctl', 'enable', 'autossh-tunnel.service'], check=True)
        subprocess.run(['sudo', 'systemctl', 'start', 'autossh-tunnel.service'], check=True)
        print("Autossh Tunnel Service started and enabled.")
    except subprocess.CalledProcessError as e:
        print(f"Error during service setup: {e}")

def main():
    print("Automated SSH Tunnel Setup Script")
    setup_autossh_service()

if __name__ == "__main__":
    main()

