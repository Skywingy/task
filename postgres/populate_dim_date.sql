-- Stored procedure to populate "dim date" table
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    loop_date DATE := start_date;
BEGIN
    -- Loop through dates from start_date to end_date
    WHILE loop_date <= end_date LOOP
        -- Insert into "dim date" table
        INSERT INTO dim_date (full_date, day, month, quarter, year)
        VALUES (
            loop_date,
            EXTRACT(DAY FROM loop_date),
            EXTRACT(MONTH FROM loop_date),
            EXTRACT(QUARTER FROM loop_date),
            EXTRACT(YEAR FROM loop_date)
            -- Add more column values as needed
        );
        -- Move to the next day
        loop_date := loop_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
