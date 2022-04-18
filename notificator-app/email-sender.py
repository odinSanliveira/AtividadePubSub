import smtplib, ssl
from email.mime.text import MIMEText
from config import email, password


# def EmailSender(notification):
try:
    smtp_server = "smtp.gmail.com"
    port = 465  # For SSL
    # message = subject + "\n email de teste."
    to_email = 'odinmiguel97@gmail.com' #can change destination
    message = MIMEText('This is test mail')
    message['Subject']= 'Kafka Notification'
    message['From'] = email
    message['To'] = to_email
    
    context = ssl.create_default_context()
    
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email, password)
        server.sendmail(email, to_email, message.as_string())
        server.quit()
        print("Email Sended!")
except:
        print("failed to send mail")

