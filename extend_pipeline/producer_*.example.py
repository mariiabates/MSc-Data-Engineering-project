### This module is a producer of data from Database.

import json
from sqlalchemy import create_engine
from helpers.helper_funcs import setup_logger, RabbitMQ
from dotenv import dotenv_values

### TODO: 
config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
NLP_EXCHANGE_NAME = config.get('NLP_EXCHANGE_NAME', 'NLP')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
DATA_SOURCE = config.get('ROUTING_KEY')


if __name__ == '__main__':
    logger = setup_logger()
    rabbit_client = RabbitMQ(RABBITMQ_HOST)
    rabbit_client.setup_exchange(NLP_EXCHANGE_NAME, "fanout")
    logger.info("Setup finished")

    ### TODO: 
    # result = <fetch an article from a data source>

    for row in result:
        json_dict = dict()
        json_dict["id"] = [row[0]] 
        json_dict["text"] = [row[1]]
        json_dict["source"] = [DATA_SOURCE]
        
        json_obj = json.dumps(json_dict)

        status = rabbit_client.send_to_exchange(NLP_EXCHANGE_NAME, json_obj)

        logger.info(status)

    rabbit_client.close()
