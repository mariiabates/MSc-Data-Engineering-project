### This module is a classifier consumer of producer modules and a publisher to poster.py.

from io import BytesIO
from dotenv import dotenv_values
import json
import pandas as pd

from helpers.helper_funcs import setup_logger, RabbitMQ
from helpers.classifier_training import clean_text, train_model

config = dotenv_values(".env")

# Rabbit
RABBITMQ_HOST = config.get("RABBITMQ_HOST")
EXCHANGE_NAME_CONSUME = config.get("NLP_EXCHANGE_NAME")
QUEUE_NAME_CONSUME = config.get("QUEUE_NAME_CLASSIFIER")
EXCHANGE_NAME_PUBLISH = config.get("POST_EXCHANGE_NAME")
# PG
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
# Classifier
CLASS_PIPE, CLASS_ENC = train_model()


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # -- 1: decode message
    body_dict = json.loads(body) 
    source_lst = body_dict.pop('source', None)
    if source_lst is None:
        return
    source = source_lst[0]
    
    df = pd.DataFrame.from_dict(body_dict)
    df = df.set_index('id')

    # -- 2: pre-process
    df['text_clean'] = clean_text(df['text'])

    # -- 3: predict label
    predicted_class_label = CLASS_PIPE.predict(df['text_clean'])
    predicted_class_name = CLASS_ENC.inverse_transform(predicted_class_label)[0]  # single element list

    # -- 4: post ID and label
    json_dict = {"id": str(df.index[0]), "label_1": str(predicted_class_name)}
    json_obj = json.dumps(json_dict)

    rabbit_client.send_to_exchange(EXCHANGE_NAME_PUBLISH, json_obj, source)

    logger.info(f" [x-1] Sent {json_obj} | {source} to {EXCHANGE_NAME_PUBLISH}")


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)

    rabbit_client.setup_exchange(EXCHANGE_NAME_PUBLISH, "direct")

    rabbit_client.setup_exchange(EXCHANGE_NAME_CONSUME, "fanout")
    rabbit_client.setup_queue(queue=QUEUE_NAME_CONSUME, exchange=EXCHANGE_NAME_CONSUME)

    logger.info(' [*] NLP_1 | Class Label | Waiting for articles | To exit press CTRL+C')
    rabbit_client.read_from_queue(QUEUE_NAME_CONSUME, callback)

