import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import socket
import subprocess
import time
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# Email Configuration
email_user = os.getenv("EMAIL_USER", "").strip()
email_password = os.getenv("EMAIL_PASSWORD", "").strip()
email_send = os.getenv("EMAIL_SEND", "").strip()
subject = "IP address update"

# WiFi Configuration
WIFI_SSID = os.getenv("WIFI_SSID", "").strip()
WIFI_PASSWORD = os.getenv("WIFI_PASSWORD", "").strip()

# Validate credentials loaded
if not all([email_user, email_password, email_send]):
    print("Error: Email credentials not properly loaded from .env file")
    print(f"EMAIL_USER: {'loaded' if email_user else 'missing'}")
    print(f"EMAIL_PASSWORD: {'loaded' if email_password else 'missing'}")
    print(f"EMAIL_SEND: {'loaded' if email_send else 'missing'}")


def is_connected_to_wifi():
    """Check if the Raspberry Pi is connected to WiFi"""
    try:
        # Method 1: Check if wlan0 has an IP address
        result = subprocess.run(
            ["ip", "addr", "show", "wlan0"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and "inet " in result.stdout:
            print("WiFi connection detected via IP address on wlan0")
            return True

        # Method 2: Try with iwgetid to check connected SSID
        result = subprocess.run(
            ["iwgetid", "-r"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            print(f"WiFi connection detected: {result.stdout.strip()}")
            return True

        # Method 3: Check network connectivity
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "2", "8.8.8.8"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            print("Network connectivity confirmed")
            return True

        return False
    except Exception as e:
        print(f"Error checking WiFi status: {e}")
        # If all checks fail, assume connected and let the script try
        return True


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


def get_wlan0_ip():
    """Get the actual IP address of wlan0 interface"""
    try:
        result = subprocess.run(
            ["ip", "addr", "show", "wlan0"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            # Parse the IP address from the output
            for line in result.stdout.split("\n"):
                if "inet " in line and "inet6" not in line:
                    # Extract IP address (format: inet 192.168.1.100/24)
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        ip = parts[1].split("/")[0]
                        return ip
        return None
    except Exception as e:
        print(f"Error getting wlan0 IP: {e}")
        return None


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
        print(f"Connecting to SMTP server...")
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.set_debuglevel(1)  # Enable debug output
        server.starttls()
        print(f"Logging in with user: {email_user}")
        server.login(email_user, email_password)
        print(f"Sending message to: {email_send}")
        server.sendmail(email_user, email_send, text)
        server.quit()
        print("Email sent successfully!")
        return True

    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP Authentication Error: {e}")
        print(f"Check that EMAIL_USER and EMAIL_PASSWORD are correct in .env file")
        print(
            f"For Gmail, ensure you're using an App Password, not your regular password"
        )
        return False
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

        # Get actual wlan0 IP address
        ip_address = get_wlan0_ip()
        if not ip_address:
            # Fallback to hostname lookup
            ip_address = socket.gethostbyname(hostname)
            print(f"Warning: Using hostname-based IP (may be localhost): {ip_address}")

        ifconfig_output = get_ifconfig()

        print(f"Hostname: {hostname}")
        print(f"IP Address: {ip_address}")
        print(
            f"Email credentials check: user={email_user}, password={'*' * len(email_password) if email_password else 'MISSING'}"
        )

        # Send email
        send_ip_email(hostname, ip_address, ifconfig_output)

    except Exception as e:
        print(f"Error retrieving network information: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
