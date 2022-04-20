from confluent_kafka import Consumer, KafkaError
import json
import logging
import telegram
from telegram.ext import Updater
from telegram.ext import CommandHandler
from config import api_key

import smtplib, ssl
from email.mime.text import MIMEText
from config import email_sender, password


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificator-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notification'])

#telegram bot configuration
bot = telegram.Bot(token=api_key)

def sendTelegramMessage(notification, user_id):
    print('telegram message')
    bot.send_message(chat_id=user_id, text=notification)


def start(update, context):
    chat_id = update.message.chat_id
    update.message.reply_text(f"your id is: {chat_id}")

def updaterHandler(api_key_value):
    updater = Updater(api_key_value)
    start_command = CommandHandler('start',start)
    updater.dispatcher.add_handler(start_command)
    updater.start_polling()

def EmailSender(notification, to_email):
    try:
        smtp_server = "smtp.gmail.com"
        port = 465  # For SSL
        message = MIMEText(notification)
        message['Subject']= 'Kafka Notification'
        message['From'] = email_sender
        message['To'] = to_email
        logging.warning("Email Prepared!")
        context = ssl.create_default_context()
        
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(email_sender, password)
            server.sendmail(email_sender, to_email, message.as_string())
            server.quit()
            print("Email Sended!")
    except Exception as e: 
        logging.warning(e)
        logging.warning("failed to send mail")





try:
    # implementar código de envio de pelo kafka pelo bot
    updaterHandler(api_key)
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            method = data['method']
            email = data['email']
            username = data['username']

            logging.warning(f"READING {filename}")
            if method == 'grayscale':
                #o metodo de telegram e email é aqui (preto e branco)
                grayscale_log = f'O arquivo {filename} foi transformado em preto e branco.'
                logging.warning(f'USER_ID: {username}')
                sendTelegramMessage(grayscale_log, username)
                EmailSender(grayscale_log, email)
                logging.warning(grayscale_log)   
                             
            else:
                #o metodo de telegram e email é aqui (rotacionado)
                rotate_log = f'O arquivo {filename} foi rotacionado.'
                sendTelegramMessage(rotate_log, username)
                EmailSender(rotate_log, email)
                logging.warning(rotate_log)

            logging.warning (f"ENDING {filename}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()