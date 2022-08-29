### This module is a producer of data from NewsAPI.

import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from helper_funcs import setup_logger, RabbitMQ
from dotenv import dotenv_values
from newsdataapi import NewsDataApiClient


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
NLP_EXCHANGE_NAME = config.get('NLP_EXCHANGE_NAME', 'NLP')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
NEWS_API_KEY = config.get('NEWS_API_KEY')
NEWSAPI = config.get('NEWSAPI_ROUTING_KEY')


def get_data_api():
    api = NewsDataApiClient(apikey=NEWS_API_KEY)
    # response = api.news_api(country="gb", language="en")
    response = api.news_api(language="en")
    articles = response.get("results")  # list of dicts

    pg_client = create_engine(CONNECTION_STRING)

    for article in articles:
        # -- 1: retrieve data
        content = article.get("content")

        if content is None:  # no empty articles
            logger.info(f" [*] SKIP: No article")
            continue

        num_words_in_content = len(content.split())
        if num_words_in_content < 100:  # no short articles
            print(content)
            logger.info(f" [*] SKIP: article too short ({num_words_in_content} words)")
            continue

        id = article.get("link")
        category = article.get("category") if article.get("category") else []  # for SQL
        keywords = article.get("keywords") if article.get("keywords") else []

        # -- 2: post retrieved data to DB
        with pg_client.connect() as connection:

            sql = text(
                f"""INSERT INTO {NEWSAPI}(id,text,category,keywords) 
                    VALUES(:id, :content, :category, :keywords) 
                    ON CONFLICT (id) 
                    DO NOTHING
                """)
            connection.execute(sql, id=id, content=content, category=category, keywords=keywords)
            logger.info(f" [*] ({id}) inserted into {NEWSAPI}")

            # connection.execute(f"INSERT INTO news_db(id,category,keywords) VALUES('{id}', ARRAY {category}::text[], ARRAY {keywords}::text[]) ON CONFLICT (id) DO NOTHING;")
        
        # -- 3: return columns for training
        yield id, content


if __name__ == '__main__':
    logger = setup_logger()
    rabbit_client = RabbitMQ(RABBITMQ_HOST)
    rabbit_client.setup_exchange(NLP_EXCHANGE_NAME, "fanout")
    logger.info("Setup finished")

    for id, content in get_data_api():
        json_dict = dict()
        json_dict["id"] = [id]
        json_dict["text"] = [content]
        json_dict["source"] = [NEWSAPI]
        
        json_obj = json.dumps(json_dict)

        # if rabbit_client.connection.is_closed:
        status = rabbit_client.send_to_exchange(NLP_EXCHANGE_NAME, json_obj)

        logger.info(status)

    # rabbit_client.close()
