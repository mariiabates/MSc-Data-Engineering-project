-- Create tables for data

DROP TABLE IF EXISTS classifier_training_data;
DROP TABLE IF EXISTS bbc_source_target;
DROP TABLE IF EXISTS news_api_target;

CREATE TYPE class AS ENUM ('politics', 'entertainment', 'business', 'sport', 'tech');

CREATE TABLE IF NOT EXISTS classifier_training_data (
    id	TEXT,	
    text	TEXT,	
    label	TEXT,	
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bbc_source_target (
    id	TEXT,	
    text	TEXT,	
    label	TEXT,	
    label_1	class,	
    label_2	NUMERIC(3,2),
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS news_api_target (
    id	TEXT,
    text TEXT,
    category TEXT[],
    keywords TEXT[],	
    label_1	class,	
    label_2	NUMERIC(3,2),
PRIMARY KEY(id)
);