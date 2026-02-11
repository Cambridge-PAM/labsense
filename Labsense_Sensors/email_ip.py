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
import logging
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/var/log/raspberry_pi_ip.log"),
    ],
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    logger.error(f".env file not found at {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)

# Email Configuration
email_user = os.getenv("EMAIL_USER", "").strip()
email_password = os.getenv("EMAIL_PASSWORD", "").strip()
email_send = os.getenv("EMAIL_SEND", "").strip()
smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
smtp_port = int(os.getenv("SMTP_PORT", "587"))
subject = "IP address update"

# WiFi Configuration
WIFI_SSID = os.getenv("WIFI_SSID", "").strip()
WIFI_PASSWORD = os.getenv("WIFI_PASSWORD", "").strip()

# Timeouts (configurable via .env)
WIFI_TIMEOUT = int(os.getenv("WIFI_TIMEOUT", "60"))
EMAIL_RETRY_COUNT = int(os.getenv("EMAIL_RETRY_COUNT", "3"))
EMAIL_RETRY_DELAY = int(os.getenv("EMAIL_RETRY_DELAY", "5"))

# Success marker to prevent spam on repeated runs
SUCCESS_MARKER = Path(__file__).parent / ".last_ip_sent"

# Validate required credentials
missing_creds = []
if not email_user:
    missing_creds.append("EMAIL_USER")
if not email_password:
    missing_creds.append("EMAIL_PASSWORD")
if not email_send:
    missing_creds.append("EMAIL_SEND")

if missing_creds:
    logger.error(f"Missing required credentials in .env: {', '.join(missing_creds)}")
    sys.exit(1)

logger.info("All required credentials loaded successfully")


def check_command_exists(command: str) -> bool:
    """Check if a command exists on the system"""
    try:
        result = subprocess.run(
            ["which", command],
            capture_output=True,
            timeout=2,
        )
        return result.returncode == 0
    except Exception:
        return False


def is_connected_to_wifi() -> bool:
    """Check if the Raspberry Pi is connected to WiFi"""
    try:
        # Method 1: Check if wlan0 has an IP address
        if check_command_exists("ip"):
            result = subprocess.run(
                ["ip", "addr", "show", "wlan0"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and "inet " in result.stdout:
                logger.info("WiFi connection detected via IP address on wlan0")
                return True

        # Method 2: Try with iwgetid to check connected SSID
        if check_command_exists("iwgetid"):
            result = subprocess.run(
                ["iwgetid", "-r"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                logger.info(f"WiFi connection detected: {result.stdout.strip()}")
                return True

        # Method 3: Check network connectivity
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "2", "8.8.8.8"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            logger.info("Network connectivity confirmed via ping")
            return True

        return False
    except subprocess.TimeoutExpired:
        logger.warning("WiFi check timed out")
        return False
    except Exception as e:
        logger.warning(f"Error checking WiFi status: {e}")
        # Conservative approach: assume not connected if checks fail
        return False


def connect_to_wifi(ssid: str, password: str, timeout: int = None) -> bool:
    """Attempt to connect to WiFi with timeout and validation"""
    if timeout is None:
        timeout = WIFI_TIMEOUT

    # Validate WiFi credentials
    if not ssid or not password:
        logger.error("WiFi SSID or password not configured in .env file")
        return False

    # Check if nmcli is available
    if not check_command_exists("nmcli"):
        logger.error(
            "nmcli command not found. Install NetworkManager: sudo apt install network-manager"
        )
        return False

    logger.info(f"Attempting to connect to WiFi: {ssid}")

    try:
        # Add WiFi connection
        result = subprocess.run(
            ["nmcli", "device", "wifi", "connect", ssid, "password", password],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            logger.error(f"nmcli connection failed: {result.stderr}")
        else:
            logger.info("WiFi connection command sent successfully")

        # Wait for connection to establish
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_connected_to_wifi():
                logger.info("Successfully connected to WiFi")
                time.sleep(3)  # Give IP assignment time to complete
                return True
            time.sleep(2)

        logger.error(f"Failed to connect to WiFi within {timeout} seconds")
        return False

    except subprocess.TimeoutExpired:
        logger.error("WiFi connection attempt timed out")
        return False
    except FileNotFoundError:
        logger.error("nmcli command not found")
        return False
    except Exception as e:
        logger.error(f"Error connecting to WiFi: {e}")
        return False


def get_wlan0_ip() -> Optional[str]:
    """Get the actual IP address of wlan0 interface"""
    try:
        if not check_command_exists("ip"):
            logger.warning("'ip' command not found, cannot get wlan0 IP")
            return None

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
                        logger.info(f"Found wlan0 IP: {ip}")
                        return ip
        else:
            logger.warning(f"Failed to get wlan0 info: {result.stderr}")
        return None
    except subprocess.TimeoutExpired:
        logger.error("Timeout getting wlan0 IP")
        return None
    except Exception as e:
        logger.error(f"Error getting wlan0 IP: {e}")
        return None


def get_network_info() -> str:
    """Get network interface configuration using ip addr"""
    try:
        if check_command_exists("ip"):
            result = subprocess.run(
                ["ip", "addr"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout

        # Fallback to ifconfig if ip command not available
        if check_command_exists("ifconfig"):
            result = subprocess.run(
                ["ifconfig"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout

        return "Network info unavailable (no ip or ifconfig command found)"
    except subprocess.TimeoutExpired:
        logger.error("Timeout getting network info")
        return "Network info retrieval timed out"
    except Exception as e:
        logger.error(f"Error getting network info: {e}")
        return f"Error: {str(e)}"


def send_ip_email(
    hostname: str, ip_address: str, network_info: str, retry_count: int = None
) -> bool:
    """Send email with IP address information with retry logic"""
    if retry_count is None:
        retry_count = EMAIL_RETRY_COUNT

    for attempt in range(1, retry_count + 1):
        try:
            msg = MIMEMultipart()
            msg["From"] = email_user
            msg["To"] = email_send
            msg["Subject"] = subject

            message = f"""Raspberry Pi Connected!

Hostname: {hostname}
IP Address: {ip_address}

Full Network Configuration:
{network_info}"""

            msg.attach(MIMEText(message, "plain"))
            text = msg.as_string()

            logger.info(f"Sending email (attempt {attempt}/{retry_count})...")
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
            server.starttls()
            server.login(email_user, email_password)
            server.sendmail(email_user, email_send, text)
            server.quit()
            logger.info("Email sent successfully!")
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication Error: {e}")
            logger.error("Check EMAIL_USER and EMAIL_PASSWORD in .env file")
            logger.error("For Gmail, ensure you're using an App Password")
            return False  # Don't retry on auth errors
        except smtplib.SMTPException as e:
            logger.error(f"SMTP Error (attempt {attempt}/{retry_count}): {e}")
            if attempt < retry_count:
                logger.info(f"Retrying in {EMAIL_RETRY_DELAY} seconds...")
                time.sleep(EMAIL_RETRY_DELAY)
            else:
                return False
        except socket.timeout:
            logger.error(f"Email send timeout (attempt {attempt}/{retry_count})")
            if attempt < retry_count:
                logger.info(f"Retrying in {EMAIL_RETRY_DELAY} seconds...")
                time.sleep(EMAIL_RETRY_DELAY)
            else:
                return False
        except Exception as e:
            logger.error(
                f"Unexpected error sending email (attempt {attempt}/{retry_count}): {e}"
            )
            if attempt < retry_count:
                logger.info(f"Retrying in {EMAIL_RETRY_DELAY} seconds...")
                time.sleep(EMAIL_RETRY_DELAY)
            else:
                return False

    return False


def check_recently_sent(ip_address: str) -> bool:
    """Check if we recently sent an email for this IP to prevent spam"""
    try:
        if SUCCESS_MARKER.exists():
            last_ip = SUCCESS_MARKER.read_text().strip()
            if last_ip == ip_address:
                logger.info(f"Email already sent for IP {ip_address}. Skipping.")
                return True
    except Exception as e:
        logger.warning(f"Error checking success marker: {e}")
    return False


def mark_success(ip_address: str):
    """Mark that we successfully sent email for this IP"""
    try:
        SUCCESS_MARKER.write_text(ip_address)
        logger.info("Success marker updated")
    except Exception as e:
        logger.warning(f"Error writing success marker: {e}")


def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("Raspberry Pi IP Email Notification Script Starting")
    logger.info("=" * 60)

    try:
        # Check if already connected
        if not is_connected_to_wifi():
            logger.info("Not connected to WiFi. Attempting connection...")

            if not WIFI_SSID or not WIFI_PASSWORD:
                logger.error("WiFi credentials not configured. Cannot connect.")
                logger.error("Set WIFI_SSID and WIFI_PASSWORD in .env file")
                sys.exit(1)

            if not connect_to_wifi(WIFI_SSID, WIFI_PASSWORD):
                logger.error("Failed to connect to WiFi")
                sys.exit(1)
        else:
            logger.info("Already connected to WiFi")

        # Get network information
        logger.info("Retrieving network information...")
        time.sleep(3)  # Ensure IP is fully assigned

        hostname = socket.gethostname()

        # Get actual wlan0 IP address
        ip_address = get_wlan0_ip()
        if not ip_address:
            # Fallback to hostname lookup
            try:
                ip_address = socket.gethostbyname(hostname)
                logger.warning(
                    f"Using hostname-based IP (may be localhost): {ip_address}"
                )
            except socket.gaierror as e:
                logger.error(f"Failed to get IP address: {e}")
                sys.exit(1)

        network_info = get_network_info()

        logger.info(f"Hostname: {hostname}")
        logger.info(f"IP Address: {ip_address}")

        # Check if we already sent email for this IP
        if check_recently_sent(ip_address):
            logger.info("Exiting to prevent duplicate emails")
            sys.exit(0)

        # Send email
        if send_ip_email(hostname, ip_address, network_info):
            mark_success(ip_address)
            logger.info("Script completed successfully")
            sys.exit(0)
        else:
            logger.error("Failed to send email after all retry attempts")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
