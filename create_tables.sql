-- Create 2 tables for data
-- NOTE: name contentful_target etc.

DROP TABLE IF EXISTS training_data;
DROP TABLE IF EXISTS testing_data;
DROP TABLE IF EXISTS contentful_db;
DROP TABLE IF EXISTS news_db;

CREATE TABLE IF NOT EXISTS training_data (
    id	INT,	
    text	TEXT,	
    label_1	TEXT,	
    -- label_2	TEXT,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS testing_data (
    id	INT,	
    text	TEXT,	
    label_1	TEXT,	
    -- label_2	TEXT,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS contentful_db (
    id	INT,	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS news_db (
    id	TEXT,
    text TEXT,
    category TEXT[],
    keywords TEXT[],	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);