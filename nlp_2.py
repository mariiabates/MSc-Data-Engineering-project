### This module is a sentiment consumer of producer modules and a publisher to poster.py.

from helpers.helper_funcs import setup_logger, RabbitMQ
from textblob import TextBlob
from dotenv import dotenv_values
import pandas as pd
from io import BytesIO
import json


config = dotenv_values(".env")
RABBITMQ_HOST = config.get("RABBITMQ_HOST")
EXCHANGE_NAME_CONSUME = config.get("NLP_EXCHANGE_NAME")
QUEUE_NAME_CONSUME = config.get("QUEUE_NAME_SENTIMENT")
EXCHANGE_NAME_PUBLISH = config.get("POST_EXCHANGE_NAME")


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

     # -- 2: predict label
    trained_sentiment = TextBlob(df["text"].iloc[0]).sentiment
    predicted_subj_score = round(trained_sentiment.subjectivity, 2)

    # -- 3: post ID and label
    json_dict = {"id": str(df.index[0]), "label_2": str(predicted_subj_score)}
    json_obj = json.dumps(json_dict)

    rabbit_client.send_to_exchange(EXCHANGE_NAME_PUBLISH, json_obj, source)

    logger.info(f" [x-2] Sent {json_obj} | {source} to {EXCHANGE_NAME_PUBLISH}")


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)

    rabbit_client.setup_exchange(EXCHANGE_NAME_PUBLISH, "direct")

    rabbit_client.setup_exchange(EXCHANGE_NAME_CONSUME, "fanout")
    rabbit_client.setup_queue(queue=QUEUE_NAME_CONSUME, exchange=EXCHANGE_NAME_CONSUME)

    logger.info(' [*] NLP_2 | Subjectivity Score | Waiting for articles | To exit press CTRL+C')
    rabbit_client.read_from_queue(QUEUE_NAME_CONSUME, callback)
