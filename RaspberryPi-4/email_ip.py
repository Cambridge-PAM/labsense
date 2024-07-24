import smtplib 
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart 
from email.mime.base import MIMEBase 
from email import encoders 
import socket
import subprocess 

email_user = "labsenseip@gmail.com" 
email_password = "cqxktvjfzhejnvzj"  
email_send = "labsense.project@gmail.com" 
subject = "IP address update" 

msg = MIMEMultipart() 
msg["From"] = email_user 
msg["To"] = email_send 
msg["Subject"] = subject 


def get_ifconfig(): 
    try: 
        # Execute the `ifconfig` command 
        result = subprocess.run(['ifconfig'], capture_output=True, text=True, check=True) 
        # Print the output 
        return result.stdout
    except subprocess.CalledProcessError as e: 
        print(f"An error occurred while executing ifconfig: {e}") 

  

# Call the function 
ip= get_ifconfig() 


hostname = socket.gethostname() 
ip_address = socket.gethostbyname(hostname) 

print(f"Hostname: {hostname}")
print(f"IP Address Details: {ip}") 

message = f"""This message is sent from Python.
Hostname: {hostname} 
IP Address: {ip} """ 

msg.attach(MIMEText(message,"plain")) 

text = msg.as_string() 
server = smtplib.SMTP("smtp.gmail.com",587) 
server.starttls() 
server.login(email_user,email_password) 
 

server.sendmail(email_user,email_send,text) 
server.quit() 