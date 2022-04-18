from confluent_kafka import Consumer, KafkaError
import json
import logging
import telegram

#telegram bot configuration
api_key = '5359525154:AAE3LkUbNZF2VhWw7Ua_0Wrce9gdOSp7Pek'
user_id = '1456647874'

bot = telegram.Bot(token=api_key)
# bot.send_message(chat_id=user_id, text='kafka-test-bot is working!')

c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'grayscale-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notification'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            method = data['method']
            logging.warning(f"READING {filename}")
            if method == 'grayscale':
                #o metodo de telegram e email é aqui (preto e branco)
                grayscale_log = f'O arquivo {filename} foi transformado em preto e branco.'
                logging.warning(grayscale_log)                
            else:
                #o metodo de telegram e email é aqui (rotacionado)
                rotate_log = f'O arquivo {filename} foi rotacionado.'
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