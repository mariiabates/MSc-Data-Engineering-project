### This module is a producer of data from Database.

import json
from sqlalchemy import create_engine
from helpers.helper_funcs import setup_logger, RabbitMQ
from dotenv import dotenv_values
import time


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
NLP_EXCHANGE_NAME = config.get('NLP_EXCHANGE_NAME', 'NLP')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
BBC_SOURCE_TABLE = config.get('BBC_SOURCE_TABLE')

NUMBER_OF_RUNS = 1

if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)
    rabbit_client.setup_exchange(NLP_EXCHANGE_NAME, "fanout")
    logger.info("BBC Data | Setup finished")

    pg_client = create_engine(CONNECTION_STRING)
    # start = time.time()
    with pg_client.connect() as connection:
        for _ in range(NUMBER_OF_RUNS):
            result = connection.execute(f"SELECT * FROM {BBC_SOURCE_TABLE};")
            for row in result:
                json_dict = dict()
                json_dict["id"] = [row[0]]
                json_dict["text"] = [row[1]]
                json_dict["source"] = [BBC_SOURCE_TABLE]
                
                json_obj = json.dumps(json_dict)

                rabbit_client.send_to_exchange(NLP_EXCHANGE_NAME, json_obj)

                logger.info(f" [x] Sent {json_obj} to {NLP_EXCHANGE_NAME}")
    
    # end = time.time()
    # total = end - start
    # logger.info(f"TOTAL: {total}")
    # logger.info(f"PER ARTICLE: {total/(445*NUMBER_OF_RUNS)}")

    rabbit_client.close()
