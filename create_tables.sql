-- Create 2 tables for data

DROP TABLE IF EXISTS training_data;
DROP TABLE IF EXISTS testing_data;

CREATE TABLE IF NOT EXISTS training_data (
    id	INT,	
    text	TEXT,	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS testing_data (
    id	INT,	
    text	TEXT,	
    label_1	TEXT,	
    label_2	TEXT,
PRIMARY KEY(id)
);