CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER
);