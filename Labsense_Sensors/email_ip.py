import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import socket
import subprocess
import time
import sys

# Configuration
email_user = "labsenseip@gmail.com"
email_password = "xxxx"  # application-specific password created through gmail
email_send = "labsense.project@gmail.com"
subject = "IP address update"

# WiFi Configuration - UPDATE THESE WITH YOUR WIFI DETAILS
WIFI_SSID = "your_wifi_ssid"
WIFI_PASSWORD = "your_wifi_password"


def is_connected_to_wifi():
    """Check if the Raspberry Pi is connected to WiFi"""
    try:
        result = subprocess.run(
            ["nmcli", "connection", "show", "--active"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return "wifi" in result.stdout or "wlan0" in result.stdout
        return False
    except Exception as e:
        print(f"Error checking WiFi status: {e}")
        return False


def connect_to_wifi(ssid, password, timeout=30):
    """Attempt to connect to WiFi with timeout"""
    print(f"Attempting to connect to WiFi: {ssid}")

    try:
        # Add WiFi connection
        subprocess.run(
            ["nmcli", "device", "wifi", "connect", ssid, "password", password],
            capture_output=True,
            timeout=timeout,
        )
        print("WiFi connection command sent")

        # Wait for connection to establish
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_connected_to_wifi():
                print("Successfully connected to WiFi")
                time.sleep(2)  # Give IP assignment time to complete
                return True
            time.sleep(1)

        print(f"Failed to connect to WiFi within {timeout} seconds")
        return False

    except subprocess.TimeoutExpired:
        print("WiFi connection attempt timed out")
        return False
    except Exception as e:
        print(f"Error connecting to WiFi: {e}")
        return False


def get_ifconfig():
    """Get network interface configuration"""
    try:
        result = subprocess.run(
            ["ifconfig"], capture_output=True, text=True, check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing ifconfig: {e}")
        return "Network info unavailable"


def send_ip_email(hostname, ip_address, ifconfig_output):
    """Send email with IP address information"""
    try:
        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["Subject"] = subject

        message = f"""Raspberry Pi Connected!

Hostname: {hostname}
IP Address: {ip_address}

Full Network Configuration:
{ifconfig_output}"""

        msg.attach(MIMEText(message, "plain"))
        text = msg.as_string()

        print("Sending email...")
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, email_password)
        server.sendmail(email_user, email_send, text)
        server.quit()
        print("Email sent successfully!")
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False


def main():
    """Main function"""
    print("Starting WiFi connection check...")

    # Check if already connected
    if not is_connected_to_wifi():
        print("Not connected to WiFi. Attempting connection...")
        if not connect_to_wifi(WIFI_SSID, WIFI_PASSWORD):
            print("Failed to connect to WiFi after multiple attempts")
            sys.exit(1)
    else:
        print("Already connected to WiFi")

    # Get network information
    print("Retrieving network information...")
    time.sleep(2)  # Ensure IP is assigned

    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        ifconfig_output = get_ifconfig()

        print(f"Hostname: {hostname}")
        print(f"IP Address: {ip_address}")

        # Send email
        send_ip_email(hostname, ip_address, ifconfig_output)

    except Exception as e:
        print(f"Error retrieving network information: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
