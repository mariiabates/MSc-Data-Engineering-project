### This module gives an outline to add another NLP model to the pipeline.
# Follow TODO tags.

from helper_funcs import setup_logger, RabbitMQ
from dotenv import dotenv_values
import pandas as pd
from io import BytesIO
import json

### TODO: Add a QUEUE_NAME in .env to bind the model to the queue
#
config = dotenv_values(".env")
RABBITMQ_HOST = config.get("RABBITMQ_HOST")
EXCHANGE_NAME_CONSUME = config.get("NLP_EXCHANGE_NAME")
QUEUE_NAME_CONSUME = config.get("QUEUE_NAME")  
EXCHANGE_NAME_PUBLISH = config.get("POST_EXCHANGE_NAME")

### TODO: Import a trained model HERE
#

def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # -- 1: decode message
    body_dict = json.loads(body) 
    source_lst = body_dict.pop('source', None)
    if source_lst is None:
        return
    source = source_lst[0]

    df_to_process = pd.DataFrame.from_dict(body_dict)
    df_to_process = df_to_process.set_index('id')

    # -- 2: preprocess and predict on the df_to_process
    ### TODO: label_predicted = 
    #

    # -- 3: post ID and label

    ### TODO: Give a proper LABEL_NAME
    #
    json_dict = {
        "index": str(df_to_process.index[0]), 
        "LABEL_NAME": str(label_predicted)
    }
    
    json_obj = json.dumps(json_dict)
    rabbit_client.send_to_exchange(EXCHANGE_NAME_PUBLISH, json_obj, source)

    logger.info(f" [x-2] {df_to_process.index[0], label_predicted}")


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)

    rabbit_client.setup_exchange(EXCHANGE_NAME_PUBLISH, "direct")

    rabbit_client.setup_exchange(EXCHANGE_NAME_CONSUME, "fanout")  # creates if does not exist
    rabbit_client.setup_queue(queue=QUEUE_NAME_CONSUME, exchange=EXCHANGE_NAME_CONSUME)

    logger.info(' [*] Waiting for logs. To exit press CTRL+C')
    rabbit_client.read_from_queue(QUEUE_NAME_CONSUME, callback)
