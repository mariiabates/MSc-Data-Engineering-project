### This module posts labels to the destination table.
# Receive: id & clean text

import time
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from helpers.helper_funcs import RabbitMQ, setup_logger
from dotenv import dotenv_values


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
EXCHANGE_NAME = config.get('POST_EXCHANGE_NAME', 'Post')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
QUEUE_NAME = config.get("QUEUE_NAME_POST")
ROUTING_KEY_BBC = config.get('BBC_SOURCE_TABLE')
ROUTING_KEY_NEWSAPI = config.get('NEWSAPI_TARGET_TABLE')


def callback(ch, method, properties, body):
    # start = time.time()
    # logger.info(f"START TIME: {start}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    data_target = method.routing_key

    data = json.loads(body)
    id = str(data.get('id', None))

    label_1 = str(data.get('label_1', ""))
    label_2 = str(data.get('label_2', ""))

    ### XXX: Add labels for new NLP models accordingly:
    # label_3 = str(data.get('label_3', ""))

    with pg_client.connect() as connection:
        col_name = 'label_1' if label_1 else 'label_2'
        label = label_1 if label_1 else label_2

        if label_1:
            col_name = 'label_1'
            label = label_1

        if label_2:
            col_name = 'label_2'
            label = label_2

        ### XXX: Adjust accordingly for new NLP models:
        # if label_3:
        #     col_name = 'label_3'
        #     label = label_3

        sql = text(
            f"""INSERT INTO {data_target}(id,{col_name}) 
                VALUES (:id, :label) 
                ON CONFLICT (id) 
                DO UPDATE 
                SET {col_name} = :label 
                WHERE {data_target}.id = :id
            """)
        connection.execute(sql, id=id, label=label)
        logger.info(f" [*] ({id}, {label}) inserted into {data_target}")
        
        # end = time.time()
        # per_article = end - start
        # logger.info(f"END TIME: {end}")
        # logger.info(f"PER ARTICLE: {per_article}")
  


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)

    rabbit_client.setup_exchange(EXCHANGE_NAME, "direct")
    rabbit_client.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY_NEWSAPI)
    rabbit_client.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY_BBC)

    logger.info(' [*] POSTER | Waiting for messages | To exit press CTRL+C')

    pg_client = create_engine(CONNECTION_STRING)
    rabbit_client.read_from_queue(QUEUE_NAME, callback)
