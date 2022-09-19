-- Create tables for data

DROP TABLE IF EXISTS training_data;
DROP TABLE IF EXISTS bbc_source_target;
DROP TABLE IF EXISTS news_api_target;

CREATE TABLE IF NOT EXISTS training_data (
    id	TEXT,	
    text	TEXT,	
    label	TEXT,	
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bbc_source_target (
    id	TEXT,	
    text	TEXT,	
    label	TEXT,	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS news_api_target (
    id	TEXT,
    text TEXT,
    category TEXT[],
    keywords TEXT[],	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);