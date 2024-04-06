CREATE TABLE IF NOT EXISTS data (
    id SERIAL PRIMARY KEY,
    row_index INTEGER,
    date DATE,
    name VARCHAR(255),
    scenario VARCHAR(255),
    figure VARCHAR(255),
    value FLOAT,
    business_unit VARCHAR(255),
    factor INTEGER
);