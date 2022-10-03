### This module populates tables with data.

import os
import pandas as pd
from dotenv import dotenv_values
from helpers.helper_funcs import setup_logger, Postgres
from sklearn.model_selection import train_test_split

config = dotenv_values(".env")
DATA_DIR = config.get('DATA_DIR', "./data")
TRAIN_TABLE = config.get('TRAIN_TABLE')
BBC_SOURCE_TABLE = config.get('BBC_SOURCE_TABLE')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')


def get_label_and_text(path):
    '''
    Path should have folders indicating labels 
    with sorted .txt files containing text data.
    
    '''
    for subdir, dirs, files in os.walk(path):
        for file in files:
            parent_dir = os.path.basename(subdir)
            file_path = os.path.join(subdir, file)
            if not file_path.endswith('txt'):
                continue
            with open(file_path) as f:
                text = f.read()
                yield (parent_dir, text)


if __name__ == '__main__':

    logger = setup_logger()

    # -- 1. Read and prepare data.
    labeled_data_generator = get_label_and_text(DATA_DIR)
    raw_data_labeled = pd.DataFrame(
        labeled_data_generator, 
        columns=["label", "text"],
    )
    logger.info(f"Read {len(raw_data_labeled)} rows of data")  # 2225

    # -- 2. Split data.
    train_data, test_data = train_test_split(
        raw_data_labeled, 
        train_size=0.8, 
        stratify=raw_data_labeled["label"], 
        random_state=777,
    )

    # -- 3. Load data.
    pg = Postgres(CONNECTION_STRING)

    rows_inserted = pg.load_data(
        df=train_data, table_name=f'{TRAIN_TABLE}', table_index="id"
    )
    logger.info(f"Inserted {rows_inserted} rows of data into {TRAIN_TABLE}")  # 1780

    rows_inserted = pg.load_data(
        df=test_data, table_name=f'{BBC_SOURCE_TABLE}', table_index="id"
    )
    logger.info(f"Inserted {rows_inserted} rows of data into {BBC_SOURCE_TABLE}")  # 445

    pg.close()
