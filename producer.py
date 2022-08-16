import json
from sqlalchemy import create_engine
from helper_funcs import RabbitMQ, setup_logger
from dotenv import dotenv_values


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost') #'localhost'
EXCHANGE_NAME = config.get('EXCHANGE_NAME', 'NLP')  #'NLP'
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')  #'postgresql://postgres:postgres@localhost:5433/postgres'


if __name__ == '__main__':
    logger = setup_logger()
    rabbit = RabbitMQ(RABBITMQ_HOST, EXCHANGE_NAME)
    engine = create_engine(CONNECTION_STRING)

    with engine.connect() as connection:
        result = connection.execute("SELECT * FROM testing_data;")
        for row in result:
            json_dict = dict()
            json_dict["id"] = [row[0]]
            json_dict["text"] = [row[1]]
            # json_dict["label_1"] = [row[2]]
            # json_dict["label_2"] = row[3]
            
            json_obj = json.dumps(json_dict)
            status = rabbit.send_to_exchange(EXCHANGE_NAME, json_obj)

            logger.info(status)

    rabbit.close()
